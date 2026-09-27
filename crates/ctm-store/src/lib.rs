//! Storage backends (S3-compatible, local filesystem, in-memory, fault-injecting) and the
//! local object caches.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

pub mod cache;
pub mod faulty;
pub mod file;
pub mod mem;
pub mod s3;

pub use faulty::FaultyBackend;
pub use file::FileBackend;
pub use mem::MemBackend;
pub use s3::S3Backend;

/// An object version, as returned by the backend and sent back in `If-Match`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ETag(pub String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PutMode {
    Overwrite,
    /// `If-None-Match: *`
    CreateOnly,
    /// `If-Match: <etag>`
    IfMatch(ETag),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}: not found")]
    NotFound(String),
    /// The backend answered 412: the object exists (`CreateOnly`) or changed (`IfMatch`).
    #[error("{0}: precondition failed")]
    PreconditionFailed(String),
    #[error("unsupported repo URL {0:?} (expected s3://bucket/prefix or file:///path)")]
    BadUrl(String),
    #[error("backend refused: {0}")]
    ProbeFailed(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// The operations Continuum needs from a bucket. Keys are relative to the repo prefix.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)>;
    async fn head(&self, key: &str) -> Result<Option<ETag>>;
    /// Returns `Error::PreconditionFailed` when the precondition in `mode` doesn't hold.
    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag>;
    /// All keys starting with `prefix` (a plain string prefix), in lexicographic order.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    /// Removes a key; deleting a missing key is not an error.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Bytes `range` of an object (for reading one object out of a pack). The default reads
    /// the whole object; real backends ask for just the range.
    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let (body, _) = self.get(key).await?;
        let (start, end) = (range.start as usize, range.end as usize);
        if end > body.len() || start > end {
            return Err(Error::Backend(format!(
                "{key}: range {range:?} past the end"
            )));
        }
        Ok(body.slice(start..end))
    }
}

/// Opens the backend for a repo URL: `s3://bucket/prefix` (AWS S3, or any S3-compatible
/// service when `endpoint` is set) or `file:///path`.
pub fn open(url: &str, endpoint: Option<&str>) -> Result<Arc<dyn Backend>> {
    let bad = || Error::BadUrl(url.to_string());
    if let Some(path) = url.strip_prefix("file://") {
        if !path.starts_with('/') {
            return Err(bad());
        }
        return Ok(Arc::new(FileBackend::new(path)));
    }
    if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(bad());
        }
        return Ok(Arc::new(S3Backend::new(
            bucket,
            prefix.trim_matches('/'),
            endpoint,
        )?));
    }
    Err(bad())
}

/// Exercises `If-None-Match: *` and `If-Match` against the backend, and fails if either
/// is not honored.
pub async fn probe(backend: &dyn Backend) -> Result<()> {
    let key = format!("probe/{}", uuid::Uuid::new_v4().simple());
    let result = probe_key(backend, &key).await;
    backend.delete(&key).await?;
    result
}

async fn probe_key(backend: &dyn Backend, key: &str) -> Result<()> {
    let refused = |what: &str| Error::ProbeFailed(format!("the backend ignores {what}"));
    let first = backend
        .put(key, Bytes::from_static(b"1"), PutMode::CreateOnly)
        .await?;
    match backend
        .put(key, Bytes::from_static(b"2"), PutMode::CreateOnly)
        .await
    {
        Err(Error::PreconditionFailed(_)) => {}
        Ok(_) => return Err(refused("If-None-Match: *")),
        Err(e) => return Err(e),
    }
    // A well-formed ETag that no object has.
    let stale = ETag("\"ctm-probe-stale\"".to_string());
    match backend
        .put(key, Bytes::from_static(b"3"), PutMode::IfMatch(stale))
        .await
    {
        Err(Error::PreconditionFailed(_)) => {}
        Ok(_) => return Err(refused("If-Match")),
        Err(e) => return Err(e),
    }
    backend
        .put(key, Bytes::from_static(b"4"), PutMode::IfMatch(first))
        .await?;
    match backend.get(key).await?.0.as_ref() {
        b"4" => Ok(()),
        _ => Err(refused("If-Match")),
    }
}
