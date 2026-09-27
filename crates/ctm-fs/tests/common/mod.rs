//! Shared helpers: a GET-counting backend, deterministic data, and mounting a repo.

#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use ctm_core::pack::{Location, read_trailer};
use ctm_fs::{Errno, Fh, MountOptions, MountState};
use ctm_repo::{Identity, Repo};
use ctm_store::{Backend, ETag, MemBackend, PutMode};

/// Counts reads of chunks and metadata objects (ranged GETs of data and meta packs).
#[derive(Default)]
pub struct Counting {
    pub inner: MemBackend,
    pub chunk_gets: AtomicUsize,
    pub meta_gets: AtomicUsize,
}

impl Counting {
    pub fn reset(&self) {
        self.chunk_gets.store(0, Ordering::SeqCst);
        self.meta_gets.store(0, Ordering::SeqCst);
    }

    pub fn chunk_gets(&self) -> usize {
        self.chunk_gets.load(Ordering::SeqCst)
    }

    fn count(&self, key: &str) {
        if key.starts_with("packs/data/") {
            self.chunk_gets.fetch_add(1, Ordering::SeqCst);
        } else if key.starts_with("packs/meta/") {
            self.meta_gets.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// Every chunk stored in `backend`'s data packs, with where it is.
pub async fn packed_chunks(backend: &dyn Backend) -> Vec<(String, Location)> {
    let mut out = Vec::new();
    for key in backend.list("packs/data/").await.unwrap() {
        let (pack, _) = backend.get(&key).await.unwrap();
        let (_, entries) = read_trailer(&pack).unwrap();
        out.extend(entries.into_iter().map(|(_, loc)| (key.clone(), loc)));
    }
    out
}

/// Flips the last payload byte of every packed chunk.
pub async fn corrupt_chunks(backend: &dyn Backend) {
    for key in backend.list("packs/data/").await.unwrap() {
        let (pack, _) = backend.get(&key).await.unwrap();
        let mut pack = pack.to_vec();
        for (_, loc) in read_trailer(&pack).unwrap().1 {
            pack[loc.range().end as usize - 1] ^= 1;
        }
        backend
            .put(&key, pack.into(), PutMode::Overwrite)
            .await
            .unwrap();
    }
}

#[async_trait]
impl Backend for Counting {
    async fn get(&self, key: &str) -> ctm_store::Result<(Bytes, ETag)> {
        self.count(key);
        self.inner.get(key).await
    }
    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> ctm_store::Result<Bytes> {
        self.count(key);
        self.inner.get_range(key, range).await
    }
    async fn head(&self, key: &str) -> ctm_store::Result<Option<ETag>> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> ctm_store::Result<ETag> {
        self.inner.put(key, body, mode).await
    }
    async fn list(&self, prefix: &str) -> ctm_store::Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> ctm_store::Result<()> {
        self.inner.delete(key).await
    }
}

pub fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

pub fn identity(host: &str) -> Identity {
    Identity {
        user: "me".into(),
        hostname: host.into(),
        machine_id: [1; 16],
    }
}

/// Opens (or reopens) a mount of `spec` with its working state in `dir/state`.
pub async fn open(repo: &Arc<Repo>, dir: &Path, spec: &str, read_only: bool) -> MountState {
    MountState::open(
        repo.clone(),
        &dir.join("cache"),
        &dir.join("state"),
        &spec.parse().unwrap(),
        MountOptions {
            read_only,
            ..MountOptions::default()
        },
    )
    .await
    .unwrap()
}

pub async fn lookup_path(state: &MountState, path: &str) -> Result<u64, Errno> {
    let mut ino = 1;
    for part in path.split('/').filter(|p| !p.is_empty()) {
        ino = state.lookup(ino, part.as_bytes()).await?.ino;
    }
    Ok(ino)
}

pub async fn read_all(state: &MountState, ino: u64) -> Vec<u8> {
    let fh = state.open_file(ino, false).await.unwrap();
    let data = read_fh(state, fh, ino).await;
    state.release(fh).await.unwrap();
    data
}

pub async fn read_fh(state: &MountState, fh: Fh, ino: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let chunk = state
            .read(fh, ino, out.len() as u64, 1 << 20)
            .await
            .unwrap();
        if chunk.is_empty() {
            return out;
        }
        out.extend_from_slice(&chunk);
    }
}
