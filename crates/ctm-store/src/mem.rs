use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, PutMode, Result};

/// An in-memory backend for tests, with the same conditional-write semantics as S3.
#[derive(Default)]
pub struct MemBackend {
    objects: Mutex<BTreeMap<String, (Bytes, ETag)>>,
}

impl MemBackend {
    pub fn new() -> MemBackend {
        MemBackend::default()
    }
}

#[async_trait]
impl Backend for MemBackend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let _ = (key, &self.objects);
        todo!("M1: MemBackend::get")
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let _ = key;
        todo!("M1: MemBackend::head")
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let _ = (key, body, mode);
        todo!("M1: MemBackend::put")
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        todo!("M1: MemBackend::list")
    }
}
