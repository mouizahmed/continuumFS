//! Fetches objects through the local caches: verify on download, then serve from disk.
//!
//! Every download holds a permit from the mount's request limit. Background work (metadata
//! prefetch, readahead) also holds one of a smaller pool, so foreground reads always find
//! free permits. Concurrent requests for the same whole chunk share one download.
//!
//! A chunk is read one of two ways (R4). Sequential streams and commits fetch whole chunks,
//! hash-verified, and readahead fetches runs of chunks stored next to each other in a pack with
//! one ranged GET. Other reads fetch only the 64 KiB blocks they touch; those are cached
//! unverified until the chunk is complete, and then verified.

use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use futures::future::{BoxFuture, Shared};
use futures::{FutureExt, StreamExt};
use tokio::sync::Semaphore;

use ctm_core::encoding::decode_verified;
use ctm_core::{Chunk, ChunkRef, Id, Object, ObjectType, Tree};
use ctm_repo::Repo;
use ctm_store::cache::{BLOCK, CacheStats, ChunkCache, MetaCache};

/// Decoded trees kept in memory; the whole map is dropped when it fills up.
const TREE_MEMORY: usize = 4096;
/// Background requests in flight, out of the mount's total.
const BACKGROUND: usize = 32;
/// Readahead batches in flight per mount, each one or a few ranged GETs.
const READAHEAD_BATCHES: usize = 8;
/// The most bytes of a pack one readahead GET covers.
pub const READAHEAD_SPAN: u64 = 16 << 20;

#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct FetchError(pub Arc<str>);

impl FetchError {
    fn new(e: impl ToString) -> FetchError {
        FetchError(e.to_string().into())
    }
}

/// A whole chunk being downloaded; everyone who needs it waits on the same future.
type Pending = Shared<BoxFuture<'static, Result<Arc<[u8]>, FetchError>>>;

pub struct Fetcher {
    repo: Arc<Repo>,
    meta: MetaCache,
    chunks: ChunkCache,
    foreground: Semaphore,
    background: Semaphore,
    readahead: Semaphore,
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
            readahead: Semaphore::new(READAHEAD_BATCHES),
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

    /// Bytes `range` of a chunk. `whole` fetches the whole chunk on a miss (sequential reads
    /// and commits); otherwise only the 64 KiB blocks `range` touches are fetched, unless the
    /// whole chunk is already on its way.
    pub async fn read(
        self: &Arc<Self>,
        c: ChunkRef,
        range: Range<u32>,
        whole: bool,
    ) -> Result<Vec<u8>, FetchError> {
        if let Some(b) = self
            .chunks
            .read(&c.id, c.len, range.clone())
            .map_err(FetchError::new)?
        {
            return Ok(b);
        }
        let coming = self.inflight.lock().unwrap().contains_key(&c.id);
        // A read that needs every block gets the whole chunk, verified.
        let every_block = range.start < BLOCK && range.end > (c.len - 1) / BLOCK * BLOCK;
        if whole || coming || every_block {
            let bytes = self.whole(c, false).await?;
            return Ok(bytes[range.start as usize..range.end as usize].to_vec());
        }
        let first = range.start / BLOCK;
        let end = (range.end.div_ceil(BLOCK) * BLOCK).min(c.len);
        let bytes = {
            let _permit = self.foreground.acquire().await.expect("never closed");
            self.repo
                .chunk_range(&c.id, first * BLOCK..end)
                .await
                .map_err(|e| FetchError::new(format!("chunk {}: {e}", c.id)))?
        };
        self.chunks.record_fetch(bytes.len() as u64);
        // Cache the blocks off the read path; once the chunk is complete, check its hash.
        let (this, blocks) = (self.clone(), bytes.clone());
        tokio::task::spawn_blocking(move || {
            match this.chunks.insert_blocks(&c.id, c.len, first, &blocks) {
                Ok(true) => {
                    let key = this.repo.key();
                    let ok = this.chunks.verify(&c.id, c.len, |b| {
                        Id::compute(key, ObjectType::Chunk, b) == c.id
                    });
                    if !matches!(ok, Ok(true)) {
                        tracing::error!("chunk {} failed verification; evicted", c.id);
                    }
                }
                Ok(false) => {}
                Err(e) => tracing::warn!("caching blocks of chunk {}: {e}", c.id),
            }
        });
        let at = (range.start - first * BLOCK) as usize;
        Ok(bytes[at..at + range.len()].to_vec())
    }

    /// A whole chunk, downloading it (once, however many readers ask) on a miss.
    async fn whole(
        self: &Arc<Self>,
        c: ChunkRef,
        background: bool,
    ) -> Result<Arc<[u8]>, FetchError> {
        if let Some(b) = self
            .chunks
            .read(&c.id, c.len, 0..c.len)
            .map_err(FetchError::new)?
        {
            return Ok(b.into());
        }
        let pending = {
            let mut inflight = self.inflight.lock().unwrap();
            match inflight.get(&c.id) {
                Some(p) => p.clone(),
                None => {
                    let this = self.clone();
                    let fut = async move {
                        let result = this.download_chunk(c, background).await;
                        this.cache_and_release(c, &result);
                        result.map(|(bytes, _)| bytes)
                    }
                    .boxed()
                    .shared();
                    inflight.insert(c.id, fut.clone());
                    fut
                }
            }
        };
        // Run the download in its own task, so a cancelled read doesn't cancel it.
        tokio::spawn(pending.clone());
        pending.await
    }

    /// Downloads and verifies one whole chunk. If some of its blocks are cached already, only
    /// the missing ones are fetched, then the chunk is verified in the cache; the flag says
    /// whether the chunk is cached already.
    async fn download_chunk(
        &self,
        c: ChunkRef,
        background: bool,
    ) -> Result<(Arc<[u8]>, bool), FetchError> {
        let cached = self.chunks.blocks(&c.id).map_err(FetchError::new)?;
        if cached != 0
            && let Some(bytes) = self.fill_blocks(c, cached).await?
        {
            return Ok((bytes, true));
        }
        let (chunk, _) = self.download::<Chunk>(&c.id, background).await?;
        self.chunks.record_fetch(u64::from(c.len));
        Ok((chunk.0.into(), false))
    }

    /// After a whole-chunk download, whose waiting readers already have the bytes: caches the
    /// chunk off the async threads, and only then stops routing readers to the download.
    fn cache_and_release(
        self: &Arc<Self>,
        c: ChunkRef,
        result: &Result<(Arc<[u8]>, bool), FetchError>,
    ) {
        match result {
            Ok((bytes, false)) => {
                let (this, bytes) = (self.clone(), bytes.clone());
                tokio::task::spawn_blocking(move || {
                    if let Err(e) = this.chunks.insert(&c.id, &bytes) {
                        tracing::warn!("caching chunk {}: {e}", c.id);
                    }
                    this.inflight.lock().unwrap().remove(&c.id);
                });
            }
            _ => {
                self.inflight.lock().unwrap().remove(&c.id);
            }
        }
    }

    /// Writes the cache's batched counters and access times every few seconds, so reads never
    /// write `cache.db` themselves. Stops when the fetcher is dropped.
    pub fn spawn_flusher(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                let Some(this) = weak.upgrade() else {
                    break;
                };
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(e) = this.chunks.flush() {
                        tracing::warn!("flushing the chunk cache: {e}");
                    }
                })
                .await;
            }
        });
    }

    /// Completes a partly cached chunk by fetching its missing blocks, then verifies it.
    /// `None` if that didn't work out (the caller downloads the whole chunk).
    async fn fill_blocks(&self, c: ChunkRef, cached: u64) -> Result<Option<Arc<[u8]>>, FetchError> {
        let n = c.len.div_ceil(BLOCK);
        let mut b = 0;
        while b < n {
            if cached & (1 << b) != 0 {
                b += 1;
                continue;
            }
            let first = b;
            while b < n && cached & (1 << b) == 0 {
                b += 1;
            }
            let range = first * BLOCK..(b * BLOCK).min(c.len);
            let bytes = {
                let _permit = self.foreground.acquire().await.expect("never closed");
                self.repo
                    .chunk_range(&c.id, range)
                    .await
                    .map_err(|e| FetchError::new(format!("chunk {}: {e}", c.id)))?
            };
            self.chunks.record_fetch(bytes.len() as u64);
            self.chunks
                .insert_blocks(&c.id, c.len, first, &bytes)
                .map_err(FetchError::new)?;
        }
        let key = self.repo.key();
        let ok = self
            .chunks
            .verify(&c.id, c.len, |b| {
                Id::compute(key, ObjectType::Chunk, b) == c.id
            })
            .map_err(FetchError::new)?;
        if !ok {
            tracing::error!("chunk {} failed verification; evicted", c.id);
            return Ok(None);
        }
        Ok(self
            .chunks
            .read(&c.id, c.len, 0..c.len)
            .map_err(FetchError::new)?
            .map(Arc::from))
    }

    /// Adds a chunk this machine just wrote to the cache.
    pub fn cache_chunk(&self, id: &Id, bytes: &[u8]) {
        if let Err(e) = self.chunks.insert(id, bytes) {
            tracing::warn!("caching chunk {id}: {e}");
        }
    }

    /// Starts downloading whole chunks in the background, as one batch: those stored next to
    /// each other in a pack come with one ranged GET. Chunks already cached or on their way are
    /// skipped. A reader that needs one of them waits for the batch.
    pub fn prefetch(self: &Arc<Self>, chunks: Vec<ChunkRef>) {
        let mut senders = Vec::new();
        {
            let mut inflight = self.inflight.lock().unwrap();
            for c in chunks {
                if inflight.contains_key(&c.id)
                    || self.chunks.has_all(&c.id, c.len).unwrap_or(false)
                {
                    continue;
                }
                let (tx, rx) = tokio::sync::oneshot::channel::<Result<Arc<[u8]>, FetchError>>();
                let this = self.clone();
                // If the batch dies without answering, the reader downloads the chunk itself.
                let fut = async move {
                    match rx.await {
                        Ok(r) => r,
                        Err(_) => {
                            let result = this.download_chunk(c, false).await;
                            this.cache_and_release(c, &result);
                            result.map(|(bytes, _)| bytes)
                        }
                    }
                }
                .boxed()
                .shared();
                inflight.insert(c.id, fut);
                senders.push((c, tx));
            }
        }
        if senders.is_empty() {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            let _slot = this.readahead.acquire().await.expect("never closed");
            let _bg = this.background.acquire().await.expect("never closed");
            let ids: Vec<Id> = senders.iter().map(|(c, _)| c.id).collect();
            let mut waiting: HashMap<Id, _> =
                senders.into_iter().map(|(c, tx)| (c.id, (c, tx))).collect();
            let mut arriving = this.repo.fetch_chunks(&ids, READAHEAD_SPAN);
            // Each chunk goes to its reader as soon as it's verified, then into the cache off
            // the async threads.
            while let Some((id, result)) = arriving.next().await {
                let Some((c, tx)) = waiting.remove(&id) else {
                    continue;
                };
                match result {
                    Ok(chunk) => {
                        let bytes = Arc::<[u8]>::from(chunk.0);
                        let _ = tx.send(Ok(bytes.clone()));
                        let cacher = this.clone();
                        tokio::task::spawn_blocking(move || {
                            cacher.chunks.record_fetch(u64::from(c.len));
                            if let Err(e) = cacher.chunks.insert(&c.id, &bytes) {
                                tracing::warn!("caching chunk {}: {e}", c.id);
                            }
                            // Only now: until it's cached, a reader waits on this answer.
                            cacher.inflight.lock().unwrap().remove(&c.id);
                        });
                    }
                    Err(e) => {
                        this.inflight.lock().unwrap().remove(&c.id);
                        let _ = tx.send(Err(FetchError::new(format!("chunk {}: {e}", c.id))));
                    }
                }
            }
        });
    }
}
