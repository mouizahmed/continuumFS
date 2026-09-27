//! Where objects live (R1): written into packs, found through the index.
//!
//! Writes go into two open packs (data and metadata). A pack is uploaded when it reaches 32 MiB,
//! and every open pack is closed and uploaded, followed by one index segment for everything
//! uploaded since, by `flush`, which runs before every ref write. Until then, objects are read
//! from memory (still in an open pack) or through their known location (uploaded, not yet
//! indexed).
//!
//! Reads look in this process's unflushed objects, then the index mirror (a local SQLite copy of
//! the bucket's index segments). On a miss the mirror is synced and consulted again; objects
//! written before R1 are found under their loose key.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use rusqlite::{Connection, OptionalExtension, params};

use ctm_core::pack::{Location, PACK_TARGET, PackBuilder, PackId, decode_index, encode_index};
use ctm_core::{Encoded, Id, ObjectType};
use ctm_store::{Backend, PutMode};

use crate::{Error, Result};

/// Requests in flight, shared by every operation on a repo.
const CONCURRENCY: usize = 64;
/// Packs uploading at once (each up to 32 MiB in memory).
const PACK_UPLOADS: usize = 4;

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

/// A finished pack's bytes, and where each object in it is.
type Sealed = (Vec<u8>, Vec<(Id, Location)>);

#[derive(Default)]
struct Open {
    data: Option<PackBuilder>,
    meta: Option<PackBuilder>,
    /// Objects in an open pack, or in one being uploaded.
    pending: HashMap<Id, Encoded>,
    /// Objects in uploaded packs that no index segment lists yet.
    unindexed: Vec<(Id, Location)>,
    unindexed_at: HashMap<Id, Location>,
    /// Packs being uploaded by `put`; `flush` waits for them.
    uploading: usize,
    /// Finished packs whose upload failed, retried by the next `put` or `flush`.
    failed: Vec<Sealed>,
}

/// The local mirror of the bucket's index segments.
struct Mirror {
    db: Mutex<Connection>,
}

impl Mirror {
    fn open(path: Option<&Path>) -> Result<Mirror> {
        let db = match path {
            Some(p) => Connection::open(p),
            None => Connection::open_in_memory(),
        }
        .map_err(sqlite)?;
        db.busy_timeout(Duration::from_secs(10)).map_err(sqlite)?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS objects (
                 id BLOB PRIMARY KEY, pack BLOB NOT NULL, offset INTEGER NOT NULL,
                 len INTEGER NOT NULL, type INTEGER NOT NULL, flags INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS segments (key TEXT PRIMARY KEY);",
        )
        .map_err(sqlite)?;
        Ok(Mirror { db: Mutex::new(db) })
    }

    fn get(&self, id: &Id) -> Result<Option<Location>> {
        let db = self.db.lock().unwrap();
        let row: Option<(Vec<u8>, i64, i64, i64, i64)> = db
            .query_row(
                "SELECT pack, offset, len, type, flags FROM objects WHERE id = ?1",
                [&id.0[..]],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
            )
            .optional()
            .map_err(sqlite)?;
        let Some((pack, offset, len, ty, flags)) = row else {
            return Ok(None);
        };
        Ok(Some(Location {
            pack: PackId(pack.try_into().map_err(|_| bad_mirror())?),
            offset: offset as u32,
            stored_len: len as u32,
            ty: ObjectType::from_u8(ty as u8).ok_or_else(bad_mirror)?,
            flags: flags as u8,
        }))
    }

    fn contains(&self, id: &Id) -> Result<bool> {
        Ok(self.get(id)?.is_some())
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

    fn insert(&self, segment: &str, entries: &[(Id, Location)]) -> Result<()> {
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction().map_err(sqlite)?;
        for (id, loc) in entries {
            tx.execute(
                "INSERT OR REPLACE INTO objects (id, pack, offset, len, type, flags)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    &id.0[..],
                    &loc.pack.0[..],
                    i64::from(loc.offset),
                    i64::from(loc.stored_len),
                    i64::from(loc.ty as u8),
                    i64::from(loc.flags),
                ],
            )
            .map_err(sqlite)?;
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

fn bad_mirror() -> Error {
    Error::Io(std::io::Error::other("index.db: a malformed row"))
}

pub(crate) struct Objects {
    backend: std::sync::Arc<dyn Backend>,
    mirror: Mirror,
    open: Mutex<Open>,
    /// Objects this handle has written or read: they exist.
    known: Mutex<HashSet<Id>>,
    flushing: tokio::sync::Mutex<()>,
    /// Signalled when `Open::uploading` drops to zero.
    idle: tokio::sync::Notify,
    /// When the last completed sync started.
    synced: tokio::sync::Mutex<Option<Instant>>,
    requests: tokio::sync::Semaphore,
    pack_uploads: tokio::sync::Semaphore,
    uploaded_objects: AtomicU64,
    uploaded_bytes: AtomicU64,
}

fn new_pack() -> PackBuilder {
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).expect("the OS random source works");
    PackBuilder::new(PackId(id))
}

impl Objects {
    /// `index_db` is where the index mirror lives; `None` keeps it in memory.
    pub fn new(backend: std::sync::Arc<dyn Backend>, index_db: Option<&Path>) -> Result<Objects> {
        Ok(Objects {
            backend,
            mirror: Mirror::open(index_db)?,
            open: Mutex::new(Open::default()),
            known: Mutex::new(HashSet::new()),
            flushing: tokio::sync::Mutex::new(()),
            idle: tokio::sync::Notify::new(),
            synced: tokio::sync::Mutex::new(None),
            requests: tokio::sync::Semaphore::new(CONCURRENCY),
            pack_uploads: tokio::sync::Semaphore::new(PACK_UPLOADS),
            uploaded_objects: AtomicU64::new(0),
            uploaded_bytes: AtomicU64::new(0),
        })
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
    /// size are uploaded before this returns. Nothing is referenceable until `flush`.
    pub async fn put(&self, objs: Vec<Encoded>) -> Result<()> {
        let mut full = Vec::new();
        {
            let mut open = self.open.lock().unwrap();
            let known = self.known.lock().unwrap();
            for obj in objs {
                if known.contains(&obj.id)
                    || open.pending.contains_key(&obj.id)
                    || open.unindexed_at.contains_key(&obj.id)
                    || self.mirror.contains(&obj.id)?
                {
                    continue;
                }
                let slot = if obj.ty.is_data() {
                    &mut open.data
                } else {
                    &mut open.meta
                };
                let pack = slot.get_or_insert_with(new_pack);
                pack.add(&obj);
                if pack.len() >= PACK_TARGET {
                    full.push(slot.take().expect("just used").finish());
                }
                open.pending.insert(obj.id, obj);
            }
            full.append(&mut open.failed);
            open.uploading += full.len();
        }
        self.upload_packs(full).await
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
        let result = {
            let _slot = self.pack_uploads.acquire().await.expect("never closed");
            self.put_counted(&entries[0].1.pack_key(), bytes.clone())
                .await
        };
        let mut open = self.open.lock().unwrap();
        match &result {
            Ok(()) => {
                for (id, loc) in &entries {
                    open.pending.remove(id);
                    open.unindexed_at.insert(*id, *loc);
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
        self.mirror.insert(&key, &entries)?;
        let mut open = self.open.lock().unwrap();
        let mut known = self.known.lock().unwrap();
        open.unindexed.drain(..entries.len());
        for (id, _) in &entries {
            open.unindexed_at.remove(id);
            known.insert(*id);
        }
        Ok(())
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
        let seen = self.mirror.seen()?;
        let new: Vec<String> = self
            .backend
            .list("index/")
            .await?
            .into_iter()
            .filter(|k| !seen.contains(k))
            .collect();
        let segments: Vec<(String, Vec<(Id, Location)>)> = stream::iter(new)
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
            self.mirror.insert(&key, &entries)?;
        }
        *last = Some(started);
        Ok(())
    }

    async fn read_packed(&self, loc: &Location) -> Result<Bytes> {
        let payload = {
            let _permit = self.requests.acquire().await.expect("never closed");
            self.backend.get_range(&loc.pack_key(), loc.range()).await?
        };
        let mut stored = Vec::with_capacity(2 + payload.len());
        stored.push(loc.ty as u8);
        stored.push(loc.flags);
        stored.extend_from_slice(&payload);
        Ok(Bytes::from(stored))
    }

    /// An object in its stored form (`[type][flags][payload]`); `data` says whether it's a
    /// chunk, for the loose key.
    ///
    /// Looks in this process's unflushed objects, then the mirror, then the loose key (objects
    /// written before R1), and only then syncs the mirror: reads of old objects never wait for
    /// a sync, and a new machine pays one extra 404 before its first sync.
    pub async fn read_stored(&self, data: bool, id: &Id) -> Result<Bytes> {
        let missed_at = Instant::now();
        let unindexed = {
            let open = self.open.lock().unwrap();
            if let Some(obj) = open.pending.get(id) {
                return Ok(Bytes::from(obj.to_stored()));
            }
            open.unindexed_at.get(id).copied()
        };
        if let Some(loc) = unindexed.or(self.mirror.get(id)?) {
            return self.read_packed(&loc).await;
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
            Ok((bytes, _)) => return Ok(bytes),
            Err(ctm_store::Error::NotFound(_)) => {}
            Err(e) => return Err(e.into()),
        }
        self.sync_index(Some(missed_at)).await?;
        match self.mirror.get(id)? {
            Some(loc) => self.read_packed(&loc).await,
            None => Err(ctm_store::Error::NotFound(id.to_string()).into()),
        }
    }

    /// Commit IDs starting with `prefix`: in packs (after a sync), unflushed, or loose.
    pub async fn commits_with_prefix(&self, prefix: &str) -> Result<Vec<Id>> {
        self.sync_index(None).await?;
        let mut ids: HashSet<Id> = self
            .mirror
            .commits_with_prefix(prefix)?
            .into_iter()
            .collect();
        {
            let open = self.open.lock().unwrap();
            let unflushed = open
                .pending
                .iter()
                .filter(|(_, o)| o.ty == ObjectType::Commit)
                .map(|(id, _)| *id)
                .chain(
                    open.unindexed_at
                        .iter()
                        .filter(|(_, l)| l.ty == ObjectType::Commit)
                        .map(|(id, _)| *id),
                );
            ids.extend(unflushed.filter(|id| id.to_hex().starts_with(prefix)));
        }
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
