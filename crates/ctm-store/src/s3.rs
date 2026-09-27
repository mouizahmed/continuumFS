//! S3 and S3-compatible services (R2 and others) through `object_store`, with conditional puts
//! (`S3ConditionalPut::ETagMatch`) enabled.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::list::{PaginatedListOptions, PaginatedListStore};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutOptions, UpdateVersion};

use crate::{Backend, ETag, Error, PutMode, Result};

pub struct S3Backend {
    store: AmazonS3,
    /// The repo prefix inside the bucket, without leading or trailing `/`; may be empty.
    prefix: String,
}

impl S3Backend {
    /// Credentials come from the standard AWS chain. An `http://` endpoint allows plain HTTP.
    pub fn new(bucket: &str, prefix: &str, endpoint: Option<&str>) -> Result<S3Backend> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_conditional_put(S3ConditionalPut::ETagMatch);
        if let Some(endpoint) = endpoint {
            builder = builder
                .with_endpoint(endpoint)
                .with_allow_http(endpoint.starts_with("http://"));
        }
        Ok(S3Backend {
            store: builder.build().map_err(store_error)?,
            prefix: prefix.to_string(),
        })
    }

    fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }

    fn path(&self, key: &str) -> Path {
        Path::from(self.full_key(key))
    }
}

fn store_error(e: object_store::Error) -> Error {
    Error::Backend(e.to_string())
}

fn etag(e: Option<String>, key: &str) -> Result<ETag> {
    e.map(ETag)
        .ok_or_else(|| Error::Backend(format!("{key}: the backend returned no ETag")))
}

#[async_trait]
impl Backend for S3Backend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        match self.store.get(&self.path(key)).await {
            Ok(r) => {
                let e = etag(r.meta.e_tag.clone(), key)?;
                Ok((r.bytes().await.map_err(store_error)?, e))
            }
            Err(object_store::Error::NotFound { .. }) => Err(Error::NotFound(key.to_string())),
            Err(e) => Err(store_error(e)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        match self.store.head(&self.path(key)).await {
            Ok(meta) => Ok(Some(etag(meta.e_tag, key)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(store_error(e)),
        }
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let mode = match mode {
            PutMode::Overwrite => object_store::PutMode::Overwrite,
            PutMode::CreateOnly => object_store::PutMode::Create,
            PutMode::IfMatch(e) => object_store::PutMode::Update(UpdateVersion {
                e_tag: Some(e.0),
                version: None,
            }),
        };
        let opts = PutOptions {
            mode,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&self.path(key), body.into(), opts)
            .await
        {
            Ok(r) => etag(r.e_tag, key),
            // If-Match on a missing object answers 404 on some services.
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }
                | object_store::Error::NotFound { .. },
            ) => Err(Error::PreconditionFailed(key.to_string())),
            Err(e) => Err(store_error(e)),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        // A plain string prefix: `ObjectStore::list` would treat it as a directory.
        let full = self.full_key(prefix);
        let strip = if self.prefix.is_empty() {
            0
        } else {
            self.prefix.len() + 1
        };
        let mut keys = Vec::new();
        let mut page_token = None;
        loop {
            let page = self
                .store
                .list_paginated(
                    Some(&full),
                    PaginatedListOptions {
                        page_token,
                        ..PaginatedListOptions::default()
                    },
                )
                .await
                .map_err(store_error)?;
            keys.extend(
                page.result
                    .objects
                    .into_iter()
                    .map(|o| o.location.as_ref()[strip..].to_string()),
            );
            match page.page_token {
                Some(t) => page_token = Some(t),
                None => break,
            }
        }
        keys.sort();
        Ok(keys)
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        // S3 has no empty ranges; answer like the other backends.
        if range.is_empty() {
            self.head(key)
                .await?
                .ok_or_else(|| Error::NotFound(key.to_string()))?;
            return Ok(Bytes::new());
        }
        match self.store.get_range(&self.path(key), range).await {
            Ok(b) => Ok(b),
            Err(object_store::Error::NotFound { .. }) => Err(Error::NotFound(key.to_string())),
            Err(e) => Err(store_error(e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(store_error(e)),
        }
    }
}
