//! S3 and S3-compatible services (R2, MinIO) through `object_store`, with conditional puts
//! (`S3ConditionalPut::ETagMatch`) enabled.

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, PutMode, Result};

pub struct S3Backend {
    store: object_store::aws::AmazonS3,
    prefix: String,
}

impl S3Backend {
    /// Credentials come from the standard AWS chain. An `http://` endpoint allows plain HTTP.
    pub fn new(bucket: &str, prefix: &str, endpoint: Option<&str>) -> Result<S3Backend> {
        let _ = (bucket, prefix, endpoint);
        todo!("M1: S3Backend::new")
    }
}

#[async_trait]
impl Backend for S3Backend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let _ = (key, &self.store, &self.prefix);
        todo!("M1: S3Backend::get")
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let _ = key;
        todo!("M1: S3Backend::head")
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let _ = (key, body, mode);
        todo!("M1: S3Backend::put")
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        todo!("M1: S3Backend::list")
    }
}
