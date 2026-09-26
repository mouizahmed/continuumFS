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
    /// All keys under `prefix`, in lexicographic order.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Opens the backend for a repo URL: `s3://bucket/prefix` (AWS S3, or any S3-compatible
/// service when `endpoint` is set) or `file:///path`.
pub fn open(url: &str, endpoint: Option<&str>) -> Result<Arc<dyn Backend>> {
    let _ = (url, endpoint);
    todo!("M1: open backend from URL")
}

/// Exercises `If-None-Match: *` and `If-Match` against the backend, and fails if either
/// is not honored.
pub async fn probe(backend: &dyn Backend) -> Result<()> {
    let _ = backend;
    todo!("M1: conditional-write probe")
}
