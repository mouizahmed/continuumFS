//! Local caches under `~/.cache/continuum/<repo-id>/`, shared by every mount process:
//!
//! - `meta/<id>`: metadata objects, never evicted in v0;
//! - `chunks/<ab>/<id>`: whole chunks, LRU-evicted by size, tracked in `cache.db`.
//!
//! Objects are written to `tmp/`, then renamed into place, so readers never see a partial
//! file. Callers verify objects before inserting them. `last_access` updates and hit/miss
//! counters are batched in memory and flushed every 5 s, so reads don't write SQLite.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ctm_core::Id;
use rusqlite::{Connection, OptionalExtension, params};

use crate::Result;

const FLUSH_EVERY: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub bytes: u64,
    pub objects: u64,
    pub hits: u64,
    pub misses: u64,
    /// Bytes downloaded to fill misses.
    pub fetched_bytes: u64,
}

pub struct ChunkCache {
    root: PathBuf,
    limit: u64,
    db: Mutex<Connection>,
    pending: Mutex<Pending>,
}

#[derive(Default)]
struct Pending {
    access: HashMap<Id, i64>,
    hits: u64,
    misses: u64,
    fetched_bytes: u64,
    last_stamp: i64,
    last_flush: Option<Instant>,
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

impl ChunkCache {
    pub fn open(root: impl Into<PathBuf>, limit: u64) -> Result<ChunkCache> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let db = Connection::open(root.join("cache.db")).map_err(sqlite)?;
        db.busy_timeout(Duration::from_secs(10)).map_err(sqlite)?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS chunks (
                 id BLOB PRIMARY KEY, size INTEGER NOT NULL, last_access INTEGER NOT NULL);
             CREATE INDEX IF NOT EXISTS chunks_lru ON chunks (last_access);
             CREATE TABLE IF NOT EXISTS counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);",
        )
        .map_err(sqlite)?;
        Ok(ChunkCache {
            root,
            limit,
            db: Mutex::new(db),
            pending: Mutex::new(Pending::default()),
        })
    }

    fn path(&self, id: &Id) -> PathBuf {
        let hex = id.to_hex();
        self.root.join("chunks").join(&hex[..2]).join(hex)
    }

    /// An open handle to a cached chunk of `len` bytes, or `None` on a miss.
    pub fn get(&self, id: &Id, len: u32) -> Result<Option<File>> {
        let found = match File::open(self.path(id)) {
            Ok(f) if f.metadata()?.len() == u64::from(len) => Some(f),
            Ok(_) => None,
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let flush = {
            let mut p = self.pending.lock().unwrap();
            if found.is_some() {
                p.hits += 1;
                let stamp = p.stamp();
                p.access.insert(*id, stamp);
            } else {
                p.misses += 1;
            }
            p.last_flush.is_none_or(|t| t.elapsed() >= FLUSH_EVERY)
        };
        if flush {
            self.flush()?;
        }
        Ok(found)
    }

    /// Counts bytes downloaded to fill a miss.
    pub fn record_fetch(&self, bytes: u64) {
        self.pending.lock().unwrap().fetched_bytes += bytes;
    }

    /// Stores an already-verified chunk, then evicts least-recently-used chunks (never this
    /// one) until the cache is within its limit.
    pub fn insert(&self, id: &Id, bytes: &[u8]) -> Result<()> {
        write_atomic(&self.root.join("tmp"), &self.path(id), bytes)?;
        let stamp = self.pending.lock().unwrap().stamp();
        let victims = {
            let mut db = self.db.lock().unwrap();
            let tx = db
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sqlite)?;
            tx.execute(
                "INSERT OR REPLACE INTO chunks (id, size, last_access) VALUES (?1, ?2, ?3)",
                params![&id.0[..], bytes.len() as i64, stamp],
            )
            .map_err(sqlite)?;
            let mut total: i64 = tx
                .query_row("SELECT COALESCE(SUM(size), 0) FROM chunks", [], |r| {
                    r.get(0)
                })
                .map_err(sqlite)?;
            let mut victims = Vec::new();
            if total > self.limit as i64 {
                let mut oldest = tx
                    .prepare("SELECT id, size FROM chunks WHERE id != ?1 ORDER BY last_access")
                    .map_err(sqlite)?;
                let mut rows = oldest.query(params![&id.0[..]]).map_err(sqlite)?;
                while total > self.limit as i64 {
                    let Some(row) = rows.next().map_err(sqlite)? else {
                        break;
                    };
                    let victim: Vec<u8> = row.get(0).map_err(sqlite)?;
                    let size: i64 = row.get(1).map_err(sqlite)?;
                    total -= size;
                    victims.push(Id(victim.try_into().expect("ids are 32 bytes")));
                }
                drop(rows);
                drop(oldest);
                for v in &victims {
                    tx.execute("DELETE FROM chunks WHERE id = ?1", params![&v.0[..]])
                        .map_err(sqlite)?;
                }
            }
            tx.commit().map_err(sqlite)?;
            victims
        };
        for v in victims {
            self.pending.lock().unwrap().access.remove(&v);
            match fs::remove_file(self.path(&v)) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        // A download just happened: make it visible to `ctm cache stats` now.
        self.flush()
    }

    /// Writes batched access times and counters to `cache.db`.
    pub fn flush(&self) -> Result<()> {
        let pending = {
            let mut p = self.pending.lock().unwrap();
            p.last_flush = Some(Instant::now());
            Pending {
                access: std::mem::take(&mut p.access),
                hits: std::mem::take(&mut p.hits),
                misses: std::mem::take(&mut p.misses),
                fetched_bytes: std::mem::take(&mut p.fetched_bytes),
                ..Pending::default()
            }
        };
        let mut db = self.db.lock().unwrap();
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
                "SELECT COALESCE(SUM(size), 0), COUNT(*) FROM chunks",
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
