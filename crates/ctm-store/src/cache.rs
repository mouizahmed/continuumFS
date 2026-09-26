//! Local caches under `~/.cache/continuum/<repo-id>/`, shared by every mount process:
//!
//! - `meta/<id>`: metadata objects, never evicted in v0;
//! - `chunks/<ab>/<id>`: whole chunks, LRU-evicted by size, tracked in `cache.db`.
//!
//! Objects are written to `tmp/`, verified, then renamed into place, so readers never see a
//! partial file. `last_access` updates are batched and flushed every 5 s.

use std::fs::File;
use std::path::PathBuf;

use ctm_core::Id;

use crate::Result;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub bytes: u64,
    pub objects: u64,
    pub hits: u64,
    pub misses: u64,
    /// Bytes downloaded to fill misses, since the cache was opened.
    pub fetched_bytes: u64,
}

pub struct ChunkCache {
    root: PathBuf,
    limit: u64,
}

impl ChunkCache {
    pub fn open(root: impl Into<PathBuf>, limit: u64) -> Result<ChunkCache> {
        let _ = (root.into(), limit);
        todo!("M2: ChunkCache::open")
    }

    /// An open handle to a cached chunk, or `None` on a miss.
    pub fn get(&self, id: &Id) -> Result<Option<File>> {
        let _ = (id, &self.root, self.limit);
        todo!("M2: ChunkCache::get")
    }

    /// Stores an already-verified chunk, evicting least-recently-used chunks over the limit.
    pub fn insert(&self, id: &Id, bytes: &[u8]) -> Result<()> {
        let _ = (id, bytes);
        todo!("M2: ChunkCache::insert")
    }

    pub fn stats(&self) -> Result<CacheStats> {
        todo!("M2: ChunkCache::stats")
    }
}

pub struct MetaCache {
    root: PathBuf,
}

impl MetaCache {
    pub fn open(root: impl Into<PathBuf>) -> Result<MetaCache> {
        let _ = root.into();
        todo!("M2: MetaCache::open")
    }

    pub fn get(&self, id: &Id) -> Result<Option<Vec<u8>>> {
        let _ = (id, &self.root);
        todo!("M2: MetaCache::get")
    }

    pub fn insert(&self, id: &Id, stored: &[u8]) -> Result<()> {
        let _ = (id, stored);
        todo!("M2: MetaCache::insert")
    }
}
