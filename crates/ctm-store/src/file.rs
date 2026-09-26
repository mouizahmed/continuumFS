//! The `file://` backend, written by hand because `object_store`'s local store doesn't
//! implement `If-Match` updates.
//!
//! - The ETag is the hex BLAKE3 hash of the file's contents.
//! - Every PUT writes a temp file in the same directory, fsyncs it, then renames it over the key.
//! - A conditional PUT holds an exclusive `flock` on `<root>/.locks/<key>` while it checks the
//!   precondition and renames. That is safe between processes on one machine.

use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, PutMode, Result};

pub struct FileBackend {
    root: PathBuf,
}

impl FileBackend {
    pub fn new(root: impl Into<PathBuf>) -> FileBackend {
        FileBackend { root: root.into() }
    }
}

#[async_trait]
impl Backend for FileBackend {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let _ = (key, &self.root);
        todo!("M1: FileBackend::get")
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let _ = key;
        todo!("M1: FileBackend::head")
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let _ = (key, body, mode);
        todo!("M1: FileBackend::put")
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        todo!("M1: FileBackend::list")
    }
}
