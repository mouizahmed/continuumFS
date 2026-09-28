//! Where objects live: written into packs (R1), found by location hints (R3) or the index.
//!
//! Writes go into two open packs (data and metadata). A pack is uploaded when it reaches 32 MiB,
//! and every open pack is closed and uploaded, followed by one index segment for everything
//! uploaded since, by `flush`, which runs before every ref write. Each object is written with a
//! hint per reference saying where the referenced object is, and refs carry hints for their
//! roots, so readers go straight from a ref to any object without the index.
//!
//! A reader looks, in order:
//! 1. in this handle's own packs (not yet indexed);
//! 2. at a hint learned from a ref or a parent object, checking the object's hash;
//! 3. in the index mirror (a local SQLite copy of the bucket's index segments);
//! 4. under the loose key, where objects written before R1 live;
//! 5. in the mirror again, after syncing it.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use rusqlite::{Connection, OptionalExtension, params};

use ctm_core::encoding::VerifyError;
use ctm_core::pack::{
    Entry, Listed, Location, PACK_TARGET, PackBuilder, PackId, decode_index, encode_index,
    read_trailer,
};
use ctm_core::{Encoded, Id, ObjectType, RepoKey};
use ctm_store::{Backend, PutMode};

use crate::{Error, Result};

/// Requests in flight, shared by every operation on a repo.
const CONCURRENCY: usize = 64;
/// Packs uploading at once (each up to 32 MiB in memory).
const PACK_UPLOADS: usize = 4;
/// `index.db`'s schema; a database with another version is a cache from an older build and is
/// rebuilt.
const SCHEMA: i64 = 3;

/// Where an object is stored before R1: `chunks/<id>` for chunks, `meta/<id>` for the rest.
pub fn object_key(ty: ObjectType, id: &Id) -> String {
    let dir = if ty.is_data() { "chunks" } else { "meta" };
    format!("{dir}/{id}")
}

/// Objects and bytes PUT so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Uploaded {
    pub objects: u64,
    pub bytes: u64,
}

impl std::ops::Sub for Uploaded {
    type Output = Uploaded;
    fn sub(self, before: Uploaded) -> Uploaded {
        Uploaded {
            objects: self.objects - before.objects,
            bytes: self.bytes - before.bytes,
        }
    }
}

/// An object as read: its stored form (`[type][flags][payload]`), and where the objects it
/// references are (one per reference, or empty if unknown).
pub struct Fetched {
    pub stored: Bytes,
    pub hints: Vec<Option<Location>>,
}

/// A finished pack's bytes, and where each object in it is.
type Sealed = (Vec<u8>, Vec<Listed>);

#[derive(Default)]
struct Open {
    data: Option<PackBuilder>,
    meta: Option<PackBuilder>,
    /// Objects in an open pack, or in one being uploaded.
    pending: HashMap<Id, Encoded>,
    /// Every object in this handle's packs that no index segment lists yet.
    located: HashMap<Id, (ObjectType, Location)>,
    /// Objects in uploaded packs that no index segment lists yet, in upload order.
    unindexed: Vec<Listed>,
    /// Packs being uploaded by `put`; `flush` waits for them.
    uploading: usize,
    /// Finished packs whose upload failed, retried by the next `put` or `flush`.
    failed: Vec<Sealed>,
    /// Outgoing mode: packs sealed to local files and not yet pushed, by ID, in sealing order.
    local: Vec<(PackId, std::path::PathBuf, Vec<Listed>)>,
    /// Outgoing mode: local files of packs just pushed, renamed `<pack-id>.pushed`, for the
    /// owner to copy into its caches and delete (`take_pushed`).
    pushed: Vec<std::path::PathBuf>,
}

/// `index.db`: the mirror of the bucket's index segments, and the hints learned from reads.
/// Learned hints are kept apart because they're untrusted: reads use them, dedup never does.
struct IndexDb {
    db: Mutex<Connection>,
}

fn location(pack: Vec<u8>, offset: i64, len: i64) -> Result<Location> {
    Ok(Location {
        pack: PackId(pack.try_into().map_err(|_| bad_row())?),
        offset: u32::try_from(offset).map_err(|_| bad_row())?,
        len: u32::try_from(len).map_err(|_| bad_row())?,
    })
}

impl IndexDb {
    fn open(path: Option<&Path>) -> Result<IndexDb> {
        let db = match path {
            Some(p) => Connection::open(p),
            None => Connection::open_in_memory(),
        }
        .map_err(sqlite)?;
        db.busy_timeout(Duration::from_secs(10)).map_err(sqlite)?;
        db.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")
            .map_err(sqlite)?;
        let version: i64 = db
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(sqlite)?;
        if version != SCHEMA {
            db.execute_batch(&format!(
                "BEGIN;
                 DROP TABLE IF EXISTS objects;
                 DROP TABLE IF EXISTS segments;
                 DROP TABLE IF EXISTS hints;
                 CREATE TABLE objects (
                     id BLOB PRIMARY KEY, pack BLOB NOT NULL, offset INTEGER NOT NULL,
                     len INTEGER NOT NULL, type INTEGER NOT NULL);
                 CREATE TABLE segments (key TEXT PRIMARY KEY);
                 CREATE TABLE hints (
                     id BLOB PRIMARY KEY, pack BLOB NOT NULL, offset INTEGER NOT NULL,
                     len INTEGER NOT NULL);
                 PRAGMA user_version = {SCHEMA};
                 COMMIT;"
            ))
            .map_err(sqlite)?;
        }
        Ok(IndexDb { db: Mutex::new(db) })
    }

    fn indexed(&self, id: &Id) -> Result<Option<(ObjectType, Location)>> {
        let db = self.db.lock().unwrap();
        let row: Option<(Vec<u8>, i64, i64, i64)> = db
            .prepare_cached("SELECT pack, offset, len, type FROM objects WHERE id = ?1")
            .map_err(sqlite)?
            .query_row([&id.0[..]], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })
            .optional()
            .map_err(sqlite)?;
        let Some((pack, offset, len, ty)) = row else {
            return Ok(None);
        };
        let ty = u8::try_from(ty)
            .ok()
            .and_then(ObjectType::from_u8)
            .ok_or_else(bad_row)?;
        Ok(Some((ty, location(pack, offset, len)?)))
    }

    fn learned(&self, id: &Id) -> Result<Option<Location>> {
        let db = self.db.lock().unwrap();
        let row: Option<(Vec<u8>, i64, i64)> = db
            .prepare_cached("SELECT pack, offset, len FROM hints WHERE id = ?1")
            .map_err(sqlite)?
            .query_row([&id.0[..]], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
            .optional()
            .map_err(sqlite)?;
        row.map(|(p, o, l)| location(p, o, l)).transpose()
    }

    fn learn(&self, hints: &[(Id, Location)]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().map_err(sqlite)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO hints (id, pack, offset, len) VALUES (?1, ?2, ?3, ?4)",
                )
                .map_err(sqlite)?;
            for (id, loc) in hints {
                stmt.execute(params![
                    &id.0[..],
                    &loc.pack.0[..],
                    i64::from(loc.offset),
                    i64::from(loc.len)
                ])
                .map_err(sqlite)?;
            }
        }
        tx.commit().map_err(sqlite)
    }

    fn forget(&self, id: &Id) -> Result<()> {
        let db = self.db.lock().unwrap();
        db.execute("DELETE FROM hints WHERE id = ?1", [&id.0[..]])
            .map_err(sqlite)?;
        Ok(())
    }

    fn seen(&self) -> Result<HashSet<String>> {
        let db = self.db.lock().unwrap();
        let mut stmt = db.prepare("SELECT key FROM segments").map_err(sqlite)?;
        let keys = stmt
            .query_map([], |r| r.get(0))
            .map_err(sqlite)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sqlite)?;
        Ok(keys)
    }

    fn insert(&self, segment: &str, entries: &[Listed]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().map_err(sqlite)?;
        {
            let mut stmt = tx
                .prepare_cached(
                    "INSERT OR REPLACE INTO objects (id, pack, offset, len, type)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                )
                .map_err(sqlite)?;
            for (id, ty, loc) in entries {
                stmt.execute(params![
                    &id.0[..],
                    &loc.pack.0[..],
                    i64::from(loc.offset),
                    i64::from(loc.len),
                    i64::from(*ty as u8),
                ])
                .map_err(sqlite)?;
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO segments (key) VALUES (?1)",
            [segment],
        )
        .map_err(sqlite)?;
        tx.commit().map_err(sqlite)
    }

    /// Commits whose ID starts with `prefix` (lowercase hex): a range scan on the key.
    fn commits_with_prefix(&self, prefix: &str) -> Result<Vec<Id>> {
        let bound = |pad: char| {
            let mut hex = prefix.to_string();
            hex.extend(std::iter::repeat_n(pad, 64 - prefix.len().min(64)));
            hex::decode(hex).ok()
        };
        let (Some(lo), Some(hi)) = (bound('0'), bound('f')) else {
            return Ok(Vec::new());
        };
        let db = self.db.lock().unwrap();
        let mut stmt = db
            .prepare("SELECT id FROM objects WHERE id BETWEEN ?1 AND ?2 AND type = ?3")
            .map_err(sqlite)?;
        let ids: Vec<Vec<u8>> = stmt
            .query_map(params![lo, hi, i64::from(ObjectType::Commit as u8)], |r| {
                r.get(0)
            })
            .map_err(sqlite)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sqlite)?;
        Ok(ids
            .into_iter()
            .filter_map(|b| b.try_into().ok().map(Id))
            .collect())
    }
}

fn sqlite(e: rusqlite::Error) -> Error {
    Error::Io(std::io::Error::other(format!("index.db: {e}")))
}

fn bad_row() -> Error {
    Error::Io(std::io::Error::other("index.db: a malformed row"))
}

pub(crate) struct Objects {
    backend: std::sync::Arc<dyn Backend>,
    key: RepoKey,
    index: IndexDb,
    open: Mutex<Open>,
    /// Objects this handle has written or read: they exist.
    known: Mutex<HashSet<Id>>,
    flushing: tokio::sync::Mutex<()>,
    /// Signalled when `Open::uploading` drops to zero.
    idle: tokio::sync::Notify,
    /// When the last completed sync started.
    synced: tokio::sync::Mutex<Option<Instant>>,
    requests: std::sync::Arc<tokio::sync::Semaphore>,
    pack_uploads: tokio::sync::Semaphore,
    uploaded_objects: AtomicU64,
    uploaded_bytes: AtomicU64,
    /// Outgoing mode (mounts): full packs go to local files here instead of the bucket, and are
    /// pushed by `flush`.
    outgoing: Option<std::path::PathBuf>,
}

/// Writes a sealed pack to `dir/<pack-id>.pack` durably: a temporary file, fsync, rename, and
/// an fsync of the directory.
fn write_local(dir: &Path, pack: PackId, bytes: &[u8]) -> std::io::Result<std::path::PathBuf> {
    use std::io::Write;
    let path = dir.join(format!("{pack}.pack"));
    let tmp = dir.join(format!("{pack}.tmp"));
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    f.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(path)
}

fn new_pack() -> PackBuilder {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).expect("the OS random source works");
    PackBuilder::new(PackId(id))
}

impl Objects {
    /// `index_db` is where the index mirror and learned hints live; `None` keeps them in memory.
    pub fn new(
        backend: std::sync::Arc<dyn Backend>,
        key: RepoKey,
        index_db: Option<&Path>,
    ) -> Result<Objects> {
        Ok(Objects {
            backend,
            key,
            index: IndexDb::open(index_db)?,
            open: Mutex::new(Open::default()),
            known: Mutex::new(HashSet::new()),
            flushing: tokio::sync::Mutex::new(()),
            idle: tokio::sync::Notify::new(),
            synced: tokio::sync::Mutex::new(None),
            requests: std::sync::Arc::new(tokio::sync::Semaphore::new(CONCURRENCY)),
            pack_uploads: tokio::sync::Semaphore::new(PACK_UPLOADS),
            uploaded_objects: AtomicU64::new(0),
            uploaded_bytes: AtomicU64::new(0),
            outgoing: None,
        })
    }

    /// Switches to outgoing mode with packs kept in `dir`, and takes over the packs a previous
    /// process sealed there but didn't push. A file that isn't a whole pack was being written
    /// when that process died, before anything referenced it, and is deleted.
    pub fn set_outgoing(&mut self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut found = Vec::new();
        let gone = |e: &std::io::Error| e.kind() == std::io::ErrorKind::NotFound;
        for f in std::fs::read_dir(dir)? {
            let path = f?.path();
            // `.pushed` files are in the bucket already; `.tmp` ones were never sealed. A file
            // can vanish meanwhile (a previous owner still deleting what it cached).
            let pack = match path.extension().is_some_and(|e| e == "pack") {
                true => match std::fs::read(&path) {
                    Ok(bytes) => read_trailer(&bytes).ok().filter(|(_, e)| !e.is_empty()),
                    Err(e) if gone(&e) => continue,
                    Err(e) => return Err(e.into()),
                },
                false => None,
            };
            match pack {
                Some((pack, entries)) => found.push((pack, path, entries)),
                None => match std::fs::remove_file(&path) {
                    Err(e) if !gone(&e) => return Err(e.into()),
                    _ => {}
                },
            }
        }
        let open = self.open.get_mut().unwrap();
        for (_, _, entries) in &found {
            for (id, ty, loc) in entries {
                open.located.insert(*id, (*ty, *loc));
            }
        }
        open.local = found;
        self.outgoing = Some(dir.to_path_buf());
        Ok(())
    }

    /// Whether anything is sealed locally or open and not yet pushed (outgoing mode).
    pub fn has_unpushed(&self) -> bool {
        let open = self.open.lock().unwrap();
        !open.local.is_empty() || open.data.is_some() || open.meta.is_some()
    }

    /// Outgoing mode: writes the open packs to local files, durably. Everything added so far is
    /// then safe on this machine, to be pushed by `flush`.
    pub async fn seal(&self) -> Result<()> {
        let dir = self.outgoing.clone().expect("seal is for outgoing mode");
        let packs: Vec<Sealed> = {
            let mut open = self.open.lock().unwrap();
            [open.data.take(), open.meta.take()]
                .into_iter()
                .flatten()
                .map(PackBuilder::finish)
                .collect()
        };
        for sealed in packs {
            self.keep_local(&dir, sealed).await?;
        }
        Ok(())
    }

    async fn keep_local(&self, dir: &Path, (bytes, entries): Sealed) -> Result<()> {
        let pack = entries[0].2.pack;
        let dir = dir.to_path_buf();
        let path = tokio::task::spawn_blocking(move || write_local(&dir, pack, &bytes))
            .await
            .expect("the write doesn't panic")?;
        let mut open = self.open.lock().unwrap();
        for (id, _, _) in &entries {
            open.pending.remove(id);
        }
        open.local.push((pack, path, entries));
        Ok(())
    }

    /// The local file of a pack sealed in outgoing mode and not yet pushed.
    fn local_file(&self, pack: PackId) -> Option<std::path::PathBuf> {
        let open = self.open.lock().unwrap();
        open.local
            .iter()
            .find(|(p, _, _)| *p == pack)
            .map(|(_, path, _)| path.clone())
    }

    /// A pack's bytes `range` as they arrive: from its local file while it's waiting to be
    /// pushed, else a streamed ranged GET.
    pub async fn pack_stream(
        &self,
        pack: &Location,
        data: bool,
        range: std::ops::Range<u64>,
    ) -> Result<ctm_store::ByteStream> {
        if self.local_file(pack.pack).is_some() {
            let bytes = self.pack_range(pack, data, range).await?;
            return Ok(Box::pin(stream::once(async move { Ok(bytes) })));
        }
        self.get_range_stream(&pack.key(data), range).await
    }

    /// Bytes `range` of a pack: from its local file while it's waiting to be pushed, else from
    /// the bucket (also if the file went away because the pack was just pushed).
    pub async fn pack_range(
        &self,
        pack: &Location,
        data: bool,
        range: std::ops::Range<u64>,
    ) -> Result<Bytes> {
        if let Some(path) = self.local_file(pack.pack) {
            let read = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0; (range.end - range.start) as usize];
                std::fs::File::open(path)?.read_exact_at(&mut buf, range.start)?;
                Ok(buf)
            })
            .await
            .expect("the read doesn't panic");
            match read {
                Ok(buf) => return Ok(Bytes::from(buf)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        self.get_range(&pack.key(data), range).await
    }

    pub fn uploaded(&self) -> Uploaded {
        Uploaded {
            objects: self.uploaded_objects.load(Ordering::Relaxed),
            bytes: self.uploaded_bytes.load(Ordering::Relaxed),
        }
    }

    pub fn mark_known(&self, id: Id) {
        self.known.lock().unwrap().insert(id);
    }

    /// Remembers where objects are, from a ref or from the hints of an object just read.
    pub fn learn(&self, hints: &[(Id, Location)]) -> Result<()> {
        if hints.is_empty() {
            return Ok(());
        }
        self.index.learn(hints)
    }

    /// The best-known location of an object, to write as a hint.
    pub fn hint(&self, id: &Id) -> Result<Option<Location>> {
        if let Some((_, loc)) = self.open.lock().unwrap().located.get(id) {
            return Ok(Some(*loc));
        }
        if let Some((_, loc)) = self.index.indexed(id)? {
            return Ok(Some(loc));
        }
        self.index.learned(id)
    }

    async fn put_counted(&self, key: &str, body: Vec<u8>) -> Result<()> {
        let len = body.len() as u64;
        let _permit = self.requests.acquire().await.expect("never closed");
        self.backend
            .put(key, Bytes::from(body), PutMode::Overwrite)
            .await?;
        self.uploaded_objects.fetch_add(1, Ordering::Relaxed);
        self.uploaded_bytes.fetch_add(len, Ordering::Relaxed);
        Ok(())
    }

    /// Adds objects to the open packs, skipping any already stored. Packs that reach the target
    /// size are uploaded before this returns. Nothing is referenceable until `flush`. Callers
    /// add children before the objects that reference them, so their hints are known.
    pub async fn put(&self, objs: Vec<Encoded>) -> Result<()> {
        let mut full = Vec::new();
        {
            let mut open = self.open.lock().unwrap();
            let known = self.known.lock().unwrap();
            for obj in objs {
                if known.contains(&obj.id)
                    || open.located.contains_key(&obj.id)
                    || self.index.indexed(&obj.id)?.is_some()
                {
                    continue;
                }
                let mut hints = Vec::with_capacity(obj.refs.len());
                for r in &obj.refs {
                    hints.push(match open.located.get(r) {
                        Some((_, loc)) => Some(*loc),
                        None => match self.index.indexed(r)? {
                            Some((_, loc)) => Some(loc),
                            None => self.index.learned(r)?,
                        },
                    });
                }
                let slot = if obj.ty.is_data() {
                    &mut open.data
                } else {
                    &mut open.meta
                };
                let pack = slot.get_or_insert_with(new_pack);
                let loc = pack.add(&obj, &hints);
                if pack.len() >= PACK_TARGET {
                    full.push(slot.take().expect("just used").finish());
                }
                open.located.insert(obj.id, (obj.ty, loc));
                open.pending.insert(obj.id, obj);
            }
            if self.outgoing.is_none() {
                full.append(&mut open.failed);
                open.uploading += full.len();
            }
        }
        match &self.outgoing {
            Some(dir) => {
                for sealed in full {
                    self.keep_local(dir, sealed).await?;
                }
                Ok(())
            }
            None => self.upload_packs(full).await,
        }
    }

    /// Uploads finished packs. Each must already be counted in `Open::uploading`.
    async fn upload_packs(&self, packs: Vec<Sealed>) -> Result<()> {
        stream::iter(packs)
            .map(|p| self.upload_pack(p))
            .buffer_unordered(PACK_UPLOADS)
            .collect::<Vec<Result<()>>>()
            .await
            .into_iter()
            .collect()
    }

    async fn upload_pack(&self, (bytes, entries): Sealed) -> Result<()> {
        let (_, ty, loc) = entries[0];
        let result = {
            let _slot = self.pack_uploads.acquire().await.expect("never closed");
            self.put_counted(&loc.key(ty.is_data()), bytes.clone())
                .await
        };
        let mut open = self.open.lock().unwrap();
        match &result {
            Ok(()) => {
                for (id, _, _) in &entries {
                    open.pending.remove(id);
                }
                open.unindexed.extend(entries);
            }
            Err(_) => open.failed.push((bytes, entries)),
        }
        open.uploading -= 1;
        if open.uploading == 0 {
            self.idle.notify_waiters();
        }
        result
    }

    /// Uploads every open pack, waits for packs other callers are uploading, then uploads an
    /// index segment for everything not yet indexed. Runs before every ref write, so a ref never
    /// points at an object that isn't in an indexed pack.
    pub async fn flush(&self) -> Result<()> {
        let _guard = self.flushing.lock().await;
        if self.outgoing.is_some() {
            return self.push_local().await;
        }
        let packs = {
            let mut open = self.open.lock().unwrap();
            let mut packs: Vec<_> = [open.data.take(), open.meta.take()]
                .into_iter()
                .flatten()
                .map(PackBuilder::finish)
                .collect();
            packs.append(&mut open.failed);
            open.uploading += packs.len();
            packs
        };
        let uploaded = self.upload_packs(packs).await;
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.open.lock().unwrap().uploading == 0 {
                break;
            }
            idle.await;
        }
        uploaded?;
        let entries = {
            let open = self.open.lock().unwrap();
            if !open.failed.is_empty() {
                return Err(Error::Io(std::io::Error::other(
                    "a pack upload failed; try again",
                )));
            }
            open.unindexed.clone()
        };
        if entries.is_empty() {
            return Ok(());
        }
        let mut seg = [0u8; 16];
        getrandom::fill(&mut seg).expect("the OS random source works");
        let key = format!("index/{}", hex::encode(seg));
        self.put_counted(&key, encode_index(&entries)).await?;
        self.index.insert(&key, &entries)?;
        let mut open = self.open.lock().unwrap();
        let mut known = self.known.lock().unwrap();
        open.unindexed.drain(..entries.len());
        for (id, _, _) in &entries {
            open.located.remove(id);
            known.insert(*id);
        }
        Ok(())
    }

    /// Outgoing mode's flush: seals the open packs, uploads every local pack (4 at a time), then
    /// an index segment for them, and only then deletes the local files.
    async fn push_local(&self) -> Result<()> {
        self.seal().await?;
        let local: Vec<(PackId, std::path::PathBuf, Vec<Listed>)> =
            self.open.lock().unwrap().local.clone();
        if local.is_empty() {
            return Ok(());
        }
        let uploads: Vec<(std::path::PathBuf, Listed)> = local
            .iter()
            .map(|(_, path, entries)| (path.clone(), entries[0]))
            .collect();
        stream::iter(uploads)
            .map(|(path, (_, ty, loc))| async move {
                let bytes = tokio::fs::read(&path).await?;
                let _slot = self.pack_uploads.acquire().await.expect("never closed");
                self.put_counted(&loc.key(ty.is_data()), bytes).await
            })
            .buffer_unordered(PACK_UPLOADS)
            .try_collect::<Vec<()>>()
            .await?;
        let entries: Vec<Listed> = local.iter().flat_map(|(_, _, e)| e.clone()).collect();
        let mut seg = [0u8; 16];
        getrandom::fill(&mut seg).expect("the OS random source works");
        let key = format!("index/{}", hex::encode(seg));
        self.put_counted(&key, encode_index(&entries)).await?;
        self.index.insert(&key, &entries)?;
        {
            let mut open = self.open.lock().unwrap();
            let mut known = self.known.lock().unwrap();
            open.local
                .retain(|(p, _, _)| !local.iter().any(|(q, _, _)| q == p));
            for (id, _, _) in &entries {
                open.located.remove(id);
                known.insert(*id);
            }
        }
        // Kept for the owner's caches (`take_pushed`), under a name a restart deletes.
        let mut pushed = Vec::with_capacity(local.len());
        for (_, path, _) in &local {
            let renamed = path.with_extension("pushed");
            std::fs::rename(path, &renamed)?;
            pushed.push(renamed);
        }
        self.open.lock().unwrap().pushed.extend(pushed);
        Ok(())
    }

    /// Outgoing mode: the local files of packs pushed since the last call, now in the bucket.
    /// The caller may copy objects out of them into its caches, and then deletes them.
    pub fn take_pushed(&self) -> Vec<std::path::PathBuf> {
        std::mem::take(&mut self.open.lock().unwrap().pushed)
    }

    /// Fetches index segments the mirror hasn't seen. With `missed_at` (when a lookup missed),
    /// a sync that started after then already covers it, so concurrent misses share one sync.
    pub async fn sync_index(&self, missed_at: Option<Instant>) -> Result<()> {
        let mut last = self.synced.lock().await;
        if let (Some(missed), Some(started)) = (missed_at, *last)
            && started >= missed
        {
            return Ok(());
        }
        let started = Instant::now();
        let seen = self.index.seen()?;
        let new: Vec<String> = self
            .backend
            .list("index/")
            .await?
            .into_iter()
            .filter(|k| !seen.contains(k))
            .collect();
        let segments: Vec<(String, Vec<Listed>)> = stream::iter(new)
            .map(|key| async move {
                let (bytes, _) = {
                    let _permit = self.requests.acquire().await.expect("never closed");
                    self.backend.get(&key).await?
                };
                let entries = decode_index(&bytes).map_err(|e| Error::CorruptJson {
                    what: key.clone(),
                    detail: e.to_string(),
                })?;
                Ok::<_, Error>((key, entries))
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        for (key, entries) in segments {
            self.index.insert(&key, &entries)?;
        }
        *last = Some(started);
        Ok(())
    }

    /// Where an object's entry is, without a request: in this handle's packs, at a learned
    /// hint (untrusted), or in the mirror. `None` for loose objects and ones not known here.
    pub fn locate(&self, id: &Id) -> Result<Option<Location>> {
        if let Some((_, loc)) = self.open.lock().unwrap().located.get(id) {
            return Ok(Some(*loc));
        }
        if let Some(loc) = self.index.learned(id)? {
            return Ok(Some(loc));
        }
        Ok(self.index.indexed(id)?.map(|(_, loc)| loc))
    }

    /// Drops a learned hint that turned out wrong.
    pub fn forget(&self, id: &Id) -> Result<()> {
        self.index.forget(id)
    }

    /// A ranged GET, counted against the repo's request limit.
    pub async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let _permit = self.requests.acquire().await.expect("never closed");
        Ok(self.backend.get_range(key, range).await?)
    }

    /// A ranged GET whose body arrives in pieces. It holds a request permit until the body has
    /// been read or dropped.
    pub async fn get_range_stream(
        &self,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<ctm_store::ByteStream> {
        let permit = self
            .requests
            .clone()
            .acquire_owned()
            .await
            .expect("never closed");
        let body = self.backend.get_range_stream(key, range).await?;
        Ok(Box::pin(body.map(move |piece| {
            let _held = &permit;
            piece
        })))
    }

    /// Reads one pack entry.
    async fn read_entry(&self, id: &Id, data: bool, loc: &Location) -> Result<Fetched> {
        let bytes = self.pack_range(loc, data, loc.range()).await?;
        let entry = Entry::parse(&bytes).map_err(|e| Error::Corrupt {
            id: *id,
            source: VerifyError::Decode(e),
        })?;
        Ok(Fetched {
            stored: Bytes::from(entry.to_stored()),
            hints: entry.hints,
        })
    }

    /// Reads through a learned hint: `None` if the hint is stale or wrong (then forgotten).
    async fn read_hinted(&self, id: &Id, data: bool, loc: &Location) -> Result<Option<Fetched>> {
        let fetched = match self.read_entry(id, data, loc).await {
            Ok(f) => f,
            Err(Error::Corrupt { .. } | Error::Store(ctm_store::Error::NotFound(_))) => {
                self.index.forget(id)?;
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let [ty, _, payload @ ..] = &fetched.stored[..] else {
            unreachable!("a parsed entry has a type and flags");
        };
        let ty = ObjectType::from_u8(*ty).expect("a parsed entry has a known type");
        if Id::compute(&self.key, ty, payload) != *id {
            self.index.forget(id)?;
            return Ok(None);
        }
        Ok(Some(fetched))
    }

    /// Reads an object; `data` says whether it's a chunk (which pack prefix and loose key).
    pub async fn read(&self, data: bool, id: &Id) -> Result<Fetched> {
        let missed_at = Instant::now();
        let mine = {
            let open = self.open.lock().unwrap();
            if let Some(obj) = open.pending.get(id) {
                return Ok(Fetched {
                    stored: Bytes::from(obj.to_stored()),
                    hints: Vec::new(),
                });
            }
            open.located.get(id).copied()
        };
        if let Some((ty, loc)) = mine {
            return self.read_entry(id, ty.is_data(), &loc).await;
        }
        if let Some(loc) = self.index.learned(id)?
            && let Some(f) = self.read_hinted(id, data, &loc).await?
        {
            return Ok(f);
        }
        if let Some((ty, loc)) = self.index.indexed(id)? {
            return self.read_entry(id, ty.is_data(), &loc).await;
        }
        let ty = if data {
            ObjectType::Chunk
        } else {
            ObjectType::Tree
        };
        let loose = {
            let _permit = self.requests.acquire().await.expect("never closed");
            self.backend.get(&object_key(ty, id)).await
        };
        match loose {
            Ok((stored, _)) => {
                return Ok(Fetched {
                    stored,
                    hints: Vec::new(),
                });
            }
            Err(ctm_store::Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        self.sync_index(Some(missed_at)).await?;
        match self.index.indexed(id)? {
            Some((ty, loc)) => self.read_entry(id, ty.is_data(), &loc).await,
            None => Err(ctm_store::Error::NotFound(id.to_string()).into()),
        }
    }

    /// Commit IDs starting with `prefix`: in packs (after a sync), unflushed, or loose.
    pub async fn commits_with_prefix(&self, prefix: &str) -> Result<Vec<Id>> {
        self.sync_index(None).await?;
        let mut ids: HashSet<Id> = self
            .index
            .commits_with_prefix(prefix)?
            .into_iter()
            .collect();
        ids.extend(
            self.open
                .lock()
                .unwrap()
                .located
                .iter()
                .filter(|(id, (ty, _))| {
                    *ty == ObjectType::Commit && id.to_hex().starts_with(prefix)
                })
                .map(|(id, _)| *id),
        );
        let loose = self.backend.list(&format!("meta/{prefix}")).await?;
        let mut out: Vec<Id> = ids.into_iter().collect();
        out.extend(
            loose
                .iter()
                .filter_map(|k| k["meta/".len()..].parse::<Id>().ok()),
        );
        out.sort();
        out.dedup();
        Ok(out)
    }
}
