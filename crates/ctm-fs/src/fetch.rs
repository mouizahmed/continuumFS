//! Fetches objects through the local caches: verify on download, then serve from disk.
//!
//! Every download holds a permit from the mount's request limit. Background work (metadata
//! prefetch, readahead) also holds one of a smaller pool, so foreground reads always find
//! free permits. Concurrent requests for the same chunk share one download.

use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use tokio::sync::Semaphore;

use ctm_core::encoding::decode_verified;
use ctm_core::{Chunk, ChunkRef, Id, Object, Tree};
use ctm_repo::Repo;
use ctm_store::cache::{CacheStats, ChunkCache, MetaCache};

/// Decoded trees kept in memory; the whole map is dropped when it fills up.
const TREE_MEMORY: usize = 4096;
/// Background requests in flight, out of the mount's total.
const BACKGROUND: usize = 32;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct FetchError(pub Arc<str>);

impl FetchError {
    fn new(e: impl ToString) -> FetchError {
        FetchError(e.to_string().into())
    }
}

type Pending = Shared<BoxFuture<'static, Result<(), FetchError>>>;

/// A chunk's bytes: an open cache file, or the downloaded bytes if the cache had no room.
pub enum ChunkBytes {
    Cached(File),
    Fresh(Arc<[u8]>),
}

impl ChunkBytes {
    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        match self {
            ChunkBytes::Cached(f) => f.read_exact_at(buf, offset),
            ChunkBytes::Fresh(b) => {
                let start = offset as usize;
                buf.copy_from_slice(&b[start..start + buf.len()]);
                Ok(())
            }
        }
    }
}

pub struct Fetcher {
    repo: Arc<Repo>,
    meta: MetaCache,
    chunks: ChunkCache,
    foreground: Semaphore,
    background: Semaphore,
    trees: Mutex<HashMap<Id, Arc<Tree>>>,
    inflight: Mutex<HashMap<Id, Pending>>,
}

impl Fetcher {
    pub fn new(
        repo: Arc<Repo>,
        cache_dir: &Path,
        chunk_cache_max: u64,
        max_concurrency: usize,
    ) -> ctm_store::Result<Fetcher> {
        Ok(Fetcher {
            repo,
            meta: MetaCache::open(cache_dir)?,
            chunks: ChunkCache::open(cache_dir, chunk_cache_max)?,
            foreground: Semaphore::new(max_concurrency),
            background: Semaphore::new(BACKGROUND.min(max_concurrency / 2).max(1)),
            trees: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
        })
    }

    pub fn cache_stats(&self) -> ctm_store::Result<CacheStats> {
        self.chunks.stats()
    }

    /// Downloads a stored object, retrying once if it fails verification.
    async fn download<T: Object>(
        &self,
        id: &Id,
        background: bool,
    ) -> Result<(T, Vec<u8>), FetchError> {
        let _bg = if background {
            Some(self.background.acquire().await.expect("never closed"))
        } else {
            None
        };
        let _permit = self.foreground.acquire().await.expect("never closed");
        let (obj, stored) = self
            .repo
            .fetch::<T>(id)
            .await
            .map_err(|e| FetchError::new(format!("object {id}: {e}")))?;
        Ok((obj, stored.to_vec()))
    }

    /// A metadata object, from the cache or the bucket.
    pub async fn meta<T: Object>(&self, id: &Id, background: bool) -> Result<T, FetchError> {
        if let Some(stored) = self.meta.get(id).map_err(FetchError::new)? {
            let r = self.repo.as_ref();
            if let Ok(obj) = decode_verified::<T>(r.key(), id, &stored, r.params()) {
                return Ok(obj);
            }
        }
        let (obj, stored) = self.download::<T>(id, background).await?;
        self.meta.insert(id, &stored).map_err(FetchError::new)?;
        Ok(obj)
    }

    pub async fn tree(&self, id: &Id, background: bool) -> Result<Arc<Tree>, FetchError> {
        if let Some(t) = self.trees.lock().unwrap().get(id) {
            return Ok(t.clone());
        }
        let tree = Arc::new(self.meta::<Tree>(id, background).await?);
        let mut trees = self.trees.lock().unwrap();
        if trees.len() >= TREE_MEMORY {
            trees.clear();
        }
        trees.insert(*id, tree.clone());
        Ok(tree)
    }

    /// A chunk's bytes, downloading it (once, however many readers ask) on a miss.
    pub async fn chunk(
        self: &Arc<Self>,
        c: ChunkRef,
        background: bool,
    ) -> Result<ChunkBytes, FetchError> {
        if let Some(f) = self.chunks.get(&c.id, c.len).map_err(FetchError::new)? {
            return Ok(ChunkBytes::Cached(f));
        }
        let (pending, fresh) = {
            let mut inflight = self.inflight.lock().unwrap();
            match inflight.get(&c.id) {
                Some(p) => (p.clone(), None),
                None => {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let this = self.clone();
                    let fut = async move {
                        let result = this.download::<Chunk>(&c.id, background).await;
                        let result = result.map(|(chunk, _)| {
                            this.chunks.record_fetch(u64::from(c.len));
                            let bytes: Arc<[u8]> = chunk.0.into();
                            if let Err(e) = this.chunks.insert(&c.id, &bytes) {
                                tracing::warn!("caching chunk {}: {e}", c.id);
                            }
                            bytes
                        });
                        // Only now: a reader arriving earlier waits on this download.
                        this.inflight.lock().unwrap().remove(&c.id);
                        let _ = tx.send(result?);
                        Ok(())
                    }
                    .boxed()
                    .shared();
                    inflight.insert(c.id, fut.clone());
                    (fut, Some(rx))
                }
            }
        };
        // Run the download in its own task, so a cancelled read doesn't cancel it.
        let task = tokio::spawn(pending);
        task.await.map_err(FetchError::new)??;
        if let Some(rx) = fresh
            && let Ok(bytes) = rx.await
        {
            return Ok(ChunkBytes::Fresh(bytes));
        }
        match self.chunks.get(&c.id, c.len).map_err(FetchError::new)? {
            Some(f) => Ok(ChunkBytes::Cached(f)),
            // Evicted straight away (a tiny cache): download again for this reader.
            None => Ok(ChunkBytes::Fresh(
                self.download::<Chunk>(&c.id, background).await?.0.0.into(),
            )),
        }
    }

    /// Adds a chunk this machine just wrote to the cache.
    pub fn cache_chunk(&self, id: &Id, bytes: &[u8]) {
        if let Err(e) = self.chunks.insert(id, bytes) {
            tracing::warn!("caching chunk {id}: {e}");
        }
    }

    /// Starts downloading a chunk in the background, if it isn't cached or already coming.
    pub fn prefetch_chunk(self: &Arc<Self>, c: ChunkRef) {
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.chunk(c, true).await {
                tracing::debug!("readahead of chunk {}: {e}", c.id);
            }
        });
    }
}
