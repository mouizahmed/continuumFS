//! Wraps any backend to inject errors, latency, and crashes, for crash-safety tests.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, PutMode, Result};

#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Added before every call.
    pub latency: Duration,
    /// After this many successful PUTs, the backend "crashes": every later call fails.
    pub crash_after_puts: Option<usize>,
    /// Each call fails with this probability (0.0–1.0), deterministically from `seed`.
    pub error_rate: f64,
    pub seed: u64,
}

pub struct FaultyBackend<B> {
    inner: B,
    faults: Mutex<Faults>,
}

impl<B: Backend> FaultyBackend<B> {
    pub fn new(inner: B, faults: Faults) -> FaultyBackend<B> {
        FaultyBackend {
            inner,
            faults: Mutex::new(faults),
        }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Clears every fault, as if the process restarted with a healthy network.
    pub fn heal(&self) {
        let _ = &self.faults;
        todo!("M1: FaultyBackend::heal")
    }
}

#[async_trait]
impl<B: Backend> Backend for FaultyBackend<B> {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        let _ = key;
        todo!("M1: FaultyBackend::get")
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        let _ = key;
        todo!("M1: FaultyBackend::head")
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let _ = (key, body, mode);
        todo!("M1: FaultyBackend::put")
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        todo!("M1: FaultyBackend::list")
    }
}
