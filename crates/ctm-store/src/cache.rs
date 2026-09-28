//! Local caches under `~/.cache/continuum/<repo-id>/`, shared by every mount process:
//!
//! - `meta/<id>`: metadata objects, never evicted;
//! - `chunks/<ab>/<id>.<generation>`: one sparse file per chunk, filled whole or 64 KiB block by
//!   block, LRU-evicted by the bytes present, tracked in `cache.db`.
//!
//! `cache.db` records, per chunk, which blocks are present and whether the whole chunk has been
//! hash-verified. Whole chunks are verified before they're inserted; blocks fetched on their own
//! are served unverified (TLS and the provider's checksums cover them) until the chunk is
//! complete, when the caller verifies it. A chunk's file name carries a random generation, so a
//! process filling blocks never writes into a file another process has already evicted and
//! recreated. `last_access` updates and hit/miss counters are batched in memory and written by
//! `flush`, which the owner calls periodically, so reads never write SQLite: a write can wait
//! seconds behind a checkpoint's fsync when the disk is busy.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ctm_core::Id;
use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::Result;

/// The unit of partial chunk reads and of the presence bitmap. Chunks are at most 4 MiB, so a
/// chunk has at most 64 blocks and its bitmap fits a `u64`.
pub const BLOCK: u32 = 64 << 10;
/// `cache.db`'s schema; a cache written by an older version is set aside and deleted.
const SCHEMA: i64 = 3;
/// Chunks whose state this process remembers without asking `cache.db`.
const MEMO_MAX: usize = 100_000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub bytes: u64,
    pub objects: u64,
    pub hits: u64,
    pub misses: u64,
    /// Bytes downloaded to fill misses.
    pub fetched_bytes: u64,
}

/// The bitmap of every block of a chunk of `len` bytes.
pub fn all_blocks(len: u32) -> u64 {
    let n = len.div_ceil(BLOCK);
    if n >= 64 { u64::MAX } else { (1 << n) - 1 }
}

/// The bitmap of the blocks that bytes `range` touch.
pub fn blocks_of(range: &Range<u32>) -> u64 {
    if range.is_empty() {
        return 0;
    }
    let (first, last) = (range.start / BLOCK, (range.end - 1) / BLOCK);
    all_blocks((last + 1) * BLOCK) & !all_blocks(first * BLOCK)
}

/// Bytes present in a chunk of `len` bytes with `blocks` cached.
fn present(blocks: u64, len: u32) -> i64 {
    let mut n = i64::from(blocks.count_ones()) * i64::from(BLOCK);
    let last = len.div_ceil(BLOCK) - 1;
    if blocks & (1 << last) != 0 {
        n -= i64::from((last + 1) * BLOCK - len);
    }
    n
}

#[derive(Clone, Copy)]
struct Known {
    generation: i64,
    blocks: u64,
    verified: bool,
}

pub struct ChunkCache {
    root: PathBuf,
    limit: u64,
    /// For writes. Lookups use `reader`, so a read never waits behind a writer (in WAL mode,
    /// readers don't block on a write or a checkpoint's fsync).
    db: Mutex<Connection>,
    reader: Mutex<Connection>,
    pending: Mutex<Pending>,
    memo: Mutex<HashMap<Id, Known>>,
}

#[derive(Default)]
struct Pending {
    access: HashMap<Id, i64>,
    hits: u64,
    misses: u64,
    fetched_bytes: u64,
    last_stamp: i64,
}

impl Pending {
    /// A strictly increasing access stamp (nanoseconds), so LRU order has no ties here.
    fn stamp(&mut self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i64);
        self.last_stamp = now.max(self.last_stamp + 1);
        self.last_stamp
    }
}

fn write_atomic(tmp_dir: &Path, dest: &Path, bytes: &[u8]) -> io::Result<()> {
    fs::create_dir_all(tmp_dir)?;
    fs::create_dir_all(dest.parent().expect("cache paths have a parent"))?;
    let tmp = tmp_dir.join(uuid::Uuid::new_v4().simple().to_string());
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, dest)
}

fn new_generation() -> i64 {
    (uuid::Uuid::new_v4().as_u128() as i64) & i64::MAX
}

fn remove(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

impl ChunkCache {
    pub fn open(root: impl Into<PathBuf>, limit: u64) -> Result<ChunkCache> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let mut db = Connection::open(root.join("cache.db")).map_err(sqlite)?;
        db.busy_timeout(Duration::from_secs(10)).map_err(sqlite)?;
        db.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .map_err(sqlite)?;
        let old = {
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sqlite)?;
            let version: i64 = tx
                .query_row("PRAGMA user_version", [], |r| r.get(0))
                .map_err(sqlite)?;
            let mut old = None;
            if version != SCHEMA {
                tx.execute_batch(&format!(
                    "DROP TABLE IF EXISTS chunks;
                     DROP TABLE IF EXISTS totals;
                     CREATE TABLE chunks (
                         id BLOB PRIMARY KEY, generation INTEGER NOT NULL, len INTEGER NOT NULL,
                         blocks INTEGER NOT NULL, verified INTEGER NOT NULL,
                         size INTEGER NOT NULL, last_access INTEGER NOT NULL);
                     CREATE INDEX chunks_lru ON chunks (last_access);
                     -- The bytes present, kept by triggers: summing the table on every insert
                     -- made caching a commit's chunks quadratic.
                     CREATE TABLE totals (bytes INTEGER NOT NULL);
                     INSERT INTO totals VALUES (0);
                     CREATE TRIGGER chunks_added AFTER INSERT ON chunks
                         BEGIN UPDATE totals SET bytes = bytes + NEW.size; END;
                     CREATE TRIGGER chunks_removed AFTER DELETE ON chunks
                         BEGIN UPDATE totals SET bytes = bytes - OLD.size; END;
                     CREATE TRIGGER chunks_resized AFTER UPDATE OF size ON chunks
                         BEGIN UPDATE totals SET bytes = bytes - OLD.size + NEW.size; END;
                     CREATE TABLE IF NOT EXISTS counters (
                         name TEXT PRIMARY KEY, value INTEGER NOT NULL);
                     PRAGMA user_version = {SCHEMA};"
                ))
                .map_err(sqlite)?;
                // Move the old files aside while no other process can use the cache.
                let chunks = root.join("chunks");
                if chunks.exists() {
                    fs::create_dir_all(root.join("tmp"))?;
                    let aside = root
                        .join("tmp")
                        .join(format!("old-{}", uuid::Uuid::new_v4().simple()));
                    fs::rename(&chunks, &aside)?;
                    old = Some(aside);
                }
            }
            tx.commit().map_err(sqlite)?;
            old
        };
        if let Some(old) = old {
            fs::remove_dir_all(old)?;
        }
        let reader = Connection::open(root.join("cache.db")).map_err(sqlite)?;
        reader
            .busy_timeout(Duration::from_secs(10))
            .map_err(sqlite)?;
        Ok(ChunkCache {
            root,
            limit,
            db: Mutex::new(db),
            reader: Mutex::new(reader),
            pending: Mutex::new(Pending::default()),
            memo: Mutex::new(HashMap::new()),
        })
    }

    fn path(&self, id: &Id, generation: i64) -> PathBuf {
        let hex = id.to_hex();
        self.root
            .join("chunks")
            .join(&hex[..2])
            .join(format!("{hex}.{generation:016x}"))
    }

    fn remember(&self, id: Id, k: Option<Known>) {
        let mut memo = self.memo.lock().unwrap();
        match k {
            Some(k) => {
                if memo.len() >= MEMO_MAX {
                    memo.clear();
                }
                memo.insert(id, k);
            }
            None => {
                memo.remove(&id);
            }
        }
    }

    /// What's cached of a chunk: from memory if that already covers `need`, else `cache.db`
    /// (another process may have added blocks).
    fn known(&self, id: &Id, need: u64) -> Result<Option<Known>> {
        if let Some(k) = self.memo.lock().unwrap().get(id)
            && k.blocks & need == need
        {
            return Ok(Some(*k));
        }
        let row = {
            let db = self.reader.lock().unwrap();
            db.prepare_cached("SELECT generation, blocks, verified FROM chunks WHERE id = ?1")
                .map_err(sqlite)?
                .query_row([&id.0[..]], |r| {
                    Ok(Known {
                        generation: r.get(0)?,
                        blocks: r.get::<_, i64>(1)? as u64,
                        verified: r.get(2)?,
                    })
                })
                .optional()
                .map_err(sqlite)?
        };
        self.remember(*id, row);
        Ok(row)
    }

    /// Counts a hit or miss, in memory: reads never write `cache.db` (the owner calls `flush`
    /// periodically; `stats` and dropping the cache flush too).
    fn count(&self, hit: Option<&Id>) -> Result<()> {
        {
            let mut p = self.pending.lock().unwrap();
            match hit {
                Some(id) => {
                    p.hits += 1;
                    let stamp = p.stamp();
                    p.access.insert(*id, stamp);
                }
                None => p.misses += 1,
            }
        }
        Ok(())
    }

    /// Bytes `range` of a chunk of `len` bytes, if every block they touch is cached.
    pub fn read(&self, id: &Id, len: u32, range: Range<u32>) -> Result<Option<Vec<u8>>> {
        let need = blocks_of(&range) & all_blocks(len);
        let found = match self.known(id, need)? {
            Some(k) if k.blocks & need == need => self.read_file(id, k.generation, &range)?,
            _ => None,
        };
        self.count(found.as_ref().map(|_| id))?;
        Ok(found)
    }

    fn read_file(&self, id: &Id, generation: i64, range: &Range<u32>) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0; range.len()];
        let read = File::open(self.path(id, generation))
            .and_then(|f| f.read_exact_at(&mut buf, u64::from(range.start)));
        match read {
            Ok(()) => Ok(Some(buf)),
            // Evicted by another process, or cut short by a crash.
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::UnexpectedEof
                ) =>
            {
                self.remember(*id, None);
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }

    /// The blocks of a chunk that are cached (without counting a hit or miss).
    pub fn blocks(&self, id: &Id) -> Result<u64> {
        Ok(self.known(id, u64::MAX)?.map_or(0, |k| k.blocks))
    }

    /// Whether every block of the chunk is cached (without counting a hit or miss).
    pub fn has_all(&self, id: &Id, len: u32) -> Result<bool> {
        let all = all_blocks(len);
        Ok(self.known(id, all)?.is_some_and(|k| k.blocks & all == all))
    }

    /// Counts bytes downloaded to fill a miss.
    pub fn record_fetch(&self, bytes: u64) {
        self.pending.lock().unwrap().fetched_bytes += bytes;
    }

    /// Stores a whole, already-verified chunk, then evicts least-recently-used chunks (never
    /// this one) until the cache is within its limit.
    pub fn insert(&self, id: &Id, bytes: &[u8]) -> Result<()> {
        let len = u32::try_from(bytes.len()).expect("chunks are at most 4 MiB");
        let generation = new_generation();
        write_atomic(&self.root.join("tmp"), &self.path(id, generation), bytes)?;
        let stamp = self.pending.lock().unwrap().stamp();
        let (replaced, victims) = {
            let mut db = self.db.lock().unwrap();
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sqlite)?;
            let replaced: Option<i64> = tx
                .query_row(
                    "SELECT generation FROM chunks WHERE id = ?1",
                    [&id.0[..]],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sqlite)?;
            // A delete then an insert (not `OR REPLACE`), so the triggers see both.
            tx.execute("DELETE FROM chunks WHERE id = ?1", [&id.0[..]])
                .map_err(sqlite)?;
            tx.execute(
                "INSERT INTO chunks
                     (id, generation, len, blocks, verified, size, last_access)
                 VALUES (?1, ?2, ?3, ?4, 1, ?3, ?5)",
                params![
                    &id.0[..],
                    generation,
                    i64::from(len),
                    all_blocks(len) as i64,
                    stamp
                ],
            )
            .map_err(sqlite)?;
            let victims = self.over_limit(&tx, id)?;
            tx.commit().map_err(sqlite)?;
            (replaced, victims)
        };
        self.remember(
            *id,
            Some(Known {
                generation,
                blocks: all_blocks(len),
                verified: true,
            }),
        );
        if let Some(old) = replaced.filter(|g| *g != generation) {
            remove(&self.path(id, old))?;
        }
        self.unlink(victims)?;
        // A download just happened: make it visible to `ctm cache stats` now.
        self.flush()
    }

    /// Stores unverified blocks of a chunk of `len` bytes, starting at block `first`. Returns
    /// whether every block of the chunk is now present and not yet verified, so the caller
    /// should verify it ([`ChunkCache::verify`]).
    pub fn insert_blocks(&self, id: &Id, len: u32, first: u32, bytes: &[u8]) -> Result<bool> {
        let stamp = self.pending.lock().unwrap().stamp();
        let generation = {
            let mut db = self.db.lock().unwrap();
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sqlite)?;
            let existing: Option<i64> = tx
                .query_row(
                    "SELECT generation FROM chunks WHERE id = ?1",
                    [&id.0[..]],
                    |r| r.get(0),
                )
                .optional()
                .map_err(sqlite)?;
            let generation = match existing {
                Some(g) => g,
                None => {
                    let g = new_generation();
                    tx.execute(
                        "INSERT INTO chunks
                             (id, generation, len, blocks, verified, size, last_access)
                         VALUES (?1, ?2, ?3, 0, 0, 0, ?4)",
                        params![&id.0[..], g, i64::from(len), stamp],
                    )
                    .map_err(sqlite)?;
                    g
                }
            };
            tx.commit().map_err(sqlite)?;
            generation
        };
        let path = self.path(id, generation);
        fs::create_dir_all(path.parent().expect("cache paths have a parent"))?;
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        if file.metadata()?.len() != u64::from(len) {
            file.set_len(u64::from(len))?;
        }
        file.write_all_at(bytes, u64::from(first * BLOCK))?;
        let added = blocks_of(&(first * BLOCK..first * BLOCK + bytes.len() as u32));
        let (known, victims) = {
            let mut db = self.db.lock().unwrap();
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sqlite)?;
            let row: Option<(i64, bool)> = tx
                .query_row(
                    "SELECT blocks, verified FROM chunks WHERE id = ?1 AND generation = ?2",
                    params![&id.0[..], generation],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(sqlite)?;
            // Evicted meanwhile: the blocks went into a file that's gone.
            let Some((blocks, verified)) = row else {
                return Ok(false);
            };
            let blocks = blocks as u64 | added;
            tx.execute(
                "UPDATE chunks SET blocks = ?2, size = ?3, last_access = ?4 WHERE id = ?1",
                params![&id.0[..], blocks as i64, present(blocks, len), stamp],
            )
            .map_err(sqlite)?;
            let victims = self.over_limit(&tx, id)?;
            tx.commit().map_err(sqlite)?;
            (
                Known {
                    generation,
                    blocks,
                    verified,
                },
                victims,
            )
        };
        self.remember(*id, Some(known));
        self.unlink(victims)?;
        self.flush()?;
        Ok(known.blocks == all_blocks(len) && !known.verified)
    }

    /// Checks a complete chunk with `check`: marks it verified if it passes, evicts it if not.
    /// Returns whether it passed (`true` too if it's gone or already verified).
    pub fn verify(&self, id: &Id, len: u32, check: impl FnOnce(&[u8]) -> bool) -> Result<bool> {
        let Some(k) = self.known(id, all_blocks(len))? else {
            return Ok(true);
        };
        if k.verified || k.blocks != all_blocks(len) {
            return Ok(true);
        }
        let Some(bytes) = self.read_file(id, k.generation, &(0..len))? else {
            return Ok(true);
        };
        if check(&bytes) {
            let db = self.db.lock().unwrap();
            db.execute(
                "UPDATE chunks SET verified = 1 WHERE id = ?1 AND generation = ?2",
                params![&id.0[..], k.generation],
            )
            .map_err(sqlite)?;
            drop(db);
            self.remember(
                *id,
                Some(Known {
                    verified: true,
                    ..k
                }),
            );
            Ok(true)
        } else {
            self.evict(id)?;
            Ok(false)
        }
    }

    /// Removes a chunk from the cache.
    pub fn evict(&self, id: &Id) -> Result<()> {
        let generation: Option<i64> = {
            let db = self.db.lock().unwrap();
            db.query_row(
                "DELETE FROM chunks WHERE id = ?1 RETURNING generation",
                [&id.0[..]],
                |r| r.get(0),
            )
            .optional()
            .map_err(sqlite)?
        };
        self.remember(*id, None);
        if let Some(g) = generation {
            remove(&self.path(id, g))?;
        }
        Ok(())
    }

    /// Deletes least-recently-used rows (never `keep`) until the cache fits its limit, and
    /// returns them for `unlink` after the transaction commits.
    fn over_limit(&self, tx: &Transaction, keep: &Id) -> Result<Vec<(Id, i64)>> {
        let mut total: i64 = tx
            .query_row("SELECT bytes FROM totals", [], |r| r.get(0))
            .map_err(sqlite)?;
        let mut victims = Vec::new();
        if total <= self.limit as i64 {
            return Ok(victims);
        }
        {
            let mut oldest = tx
                .prepare(
                    "SELECT id, generation, size FROM chunks WHERE id != ?1 ORDER BY last_access",
                )
                .map_err(sqlite)?;
            let mut rows = oldest.query(params![&keep.0[..]]).map_err(sqlite)?;
            while total > self.limit as i64 {
                let Some(row) = rows.next().map_err(sqlite)? else {
                    break;
                };
                let victim: Vec<u8> = row.get(0).map_err(sqlite)?;
                let size: i64 = row.get(2).map_err(sqlite)?;
                total -= size;
                victims.push((
                    Id(victim.try_into().expect("ids are 32 bytes")),
                    row.get(1).map_err(sqlite)?,
                ));
            }
        }
        for (v, _) in &victims {
            tx.execute("DELETE FROM chunks WHERE id = ?1", params![&v.0[..]])
                .map_err(sqlite)?;
        }
        Ok(victims)
    }

    fn unlink(&self, victims: Vec<(Id, i64)>) -> Result<()> {
        for (v, generation) in victims {
            self.pending.lock().unwrap().access.remove(&v);
            self.remember(v, None);
            remove(&self.path(&v, generation))?;
        }
        Ok(())
    }

    /// Writes batched access times and counters to `cache.db`.
    pub fn flush(&self) -> Result<()> {
        self.flush_with(self.db.lock().unwrap())
    }

    fn flush_with(&self, mut db: std::sync::MutexGuard<'_, Connection>) -> Result<()> {
        let pending = {
            let mut p = self.pending.lock().unwrap();
            Pending {
                access: std::mem::take(&mut p.access),
                hits: std::mem::take(&mut p.hits),
                misses: std::mem::take(&mut p.misses),
                fetched_bytes: std::mem::take(&mut p.fetched_bytes),
                ..Pending::default()
            }
        };
        let tx = db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sqlite)?;
        for (id, stamp) in &pending.access {
            tx.execute(
                "UPDATE chunks SET last_access = MAX(last_access, ?2) WHERE id = ?1",
                params![&id.0[..], stamp],
            )
            .map_err(sqlite)?;
        }
        for (name, value) in [
            ("hits", pending.hits),
            ("misses", pending.misses),
            ("fetched_bytes", pending.fetched_bytes),
        ] {
            if value > 0 {
                tx.execute(
                    "INSERT INTO counters (name, value) VALUES (?1, ?2)
                     ON CONFLICT (name) DO UPDATE SET value = value + ?2",
                    params![name, value as i64],
                )
                .map_err(sqlite)?;
            }
        }
        tx.commit().map_err(sqlite)
    }

    /// Totals across every process that uses this cache.
    pub fn stats(&self) -> Result<CacheStats> {
        self.flush()?;
        let db = self.db.lock().unwrap();
        let (bytes, objects): (i64, i64) = db
            .query_row(
                "SELECT (SELECT bytes FROM totals), COUNT(*) FROM chunks",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sqlite)?;
        let counter = |name: &str| -> Result<u64> {
            let v: Option<i64> = db
                .query_row("SELECT value FROM counters WHERE name = ?1", [name], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(sqlite)?;
            Ok(v.unwrap_or(0) as u64)
        };
        Ok(CacheStats {
            bytes: bytes as u64,
            objects: objects as u64,
            hits: counter("hits")?,
            misses: counter("misses")?,
            fetched_bytes: counter("fetched_bytes")?,
        })
    }
}

impl Drop for ChunkCache {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            tracing::warn!("flushing the chunk cache: {e}");
        }
    }
}

fn sqlite(e: rusqlite::Error) -> crate::Error {
    crate::Error::Backend(format!("cache.db: {e}"))
}

/// Metadata objects (stored form), never evicted in v0.
pub struct MetaCache {
    root: PathBuf,
}

impl MetaCache {
    pub fn open(root: impl Into<PathBuf>) -> Result<MetaCache> {
        let root = root.into();
        fs::create_dir_all(root.join("meta"))?;
        Ok(MetaCache { root })
    }

    fn path(&self, id: &Id) -> PathBuf {
        let hex = id.to_hex();
        self.root.join("meta").join(&hex[..2]).join(hex)
    }

    pub fn get(&self, id: &Id) -> Result<Option<Vec<u8>>> {
        match fs::read(self.path(id)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn insert(&self, id: &Id, stored: &[u8]) -> Result<()> {
        Ok(write_atomic(
            &self.root.join("tmp"),
            &self.path(id),
            stored,
        )?)
    }
}
