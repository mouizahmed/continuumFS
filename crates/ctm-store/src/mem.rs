use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, Error, PutMode, Result};

/// An in-memory backend for tests, with the same conditional-write semantics as S3.
#[derive(Default)]
pub struct MemBackend {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    objects: BTreeMap<String, (Bytes, ETag)>,
    /// Every write gets a new ETag, as on S3.
    version: u64,
}

impl MemBackend {
    pub fn new() -> MemBackend {
        MemBackend::default()
    }
}

#[async_trait]
impl Backend for MemBackend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let inner = self.inner.lock().unwrap();
        inner
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| Error::NotFound(key.to_string()))
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.objects.get(key).map(|(_, e)| e.clone()))
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let mut inner = self.inner.lock().unwrap();
        let current = inner.objects.get(key).map(|(_, e)| e);
        let ok = match &mode {
            PutMode::Overwrite => true,
            PutMode::CreateOnly => current.is_none(),
            PutMode::IfMatch(expected) => current == Some(expected),
        };
        if !ok {
            return Err(Error::PreconditionFailed(key.to_string()));
        }
        inner.version += 1;
        let etag = ETag(format!("v{}", inner.version));
        inner.objects.insert(key.to_string(), (body, etag.clone()));
        Ok(etag)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .objects
            .range(prefix.to_string()..)
            .map(|(k, _)| k)
            .take_while(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

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

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.lock().unwrap().objects.remove(key);
        Ok(())
    }
}
