//! Wraps any backend to inject errors, latency, and crashes, for crash-safety tests.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Backend, ETag, Error, PutMode, Result};

#[derive(Clone, Debug, Default)]
pub struct Faults {
    /// Added before every call.
    pub latency: Duration,
    /// After this many successful PUTs, the backend "crashes": every later call fails.
    pub crash_after_puts: Option<usize>,
    /// The crashing PUT reaches the bucket before the crash, so only its answer is lost.
    pub crash_lands_put: bool,
    /// Each call fails with this probability (0.0–1.0), deterministically from `seed`.
    pub error_rate: f64,
    pub seed: u64,
}

pub struct FaultyBackend<B> {
    inner: B,
    state: Mutex<State>,
}

struct State {
    faults: Faults,
    puts: usize,
    crashed: bool,
    /// Set by the PUT that crashed the backend, until that PUT has seen it.
    crashing_put: bool,
    rng: u64,
}

impl State {
    fn puts_at_crash(&mut self) -> bool {
        std::mem::take(&mut self.crashing_put)
    }
}

impl<B: Backend> FaultyBackend<B> {
    pub fn new(inner: B, faults: Faults) -> FaultyBackend<B> {
        let rng = faults.seed ^ 0x9e37_79b9_7f4a_7c15;
        FaultyBackend {
            inner,
            state: Mutex::new(State {
                faults,
                puts: 0,
                crashed: false,
                crashing_put: false,
                rng,
            }),
        }
    }

    pub fn inner(&self) -> &B {
        &self.inner
    }

    /// Replaces the faults; `crash_after_puts` counts from now.
    pub fn set_faults(&self, faults: Faults) {
        let mut s = self.state.lock().unwrap();
        s.rng = faults.seed ^ 0x9e37_79b9_7f4a_7c15;
        s.faults = faults;
        s.puts = 0;
        s.crashed = false;
    }

    /// Clears every fault, as if the process restarted with a healthy network.
    pub fn heal(&self) {
        let mut s = self.state.lock().unwrap();
        s.faults = Faults::default();
        s.crashed = false;
    }

    /// Runs the fault checks for one call; `put` says whether it's a write.
    async fn before(&self, put: bool) -> Result<()> {
        let latency = self.state.lock().unwrap().faults.latency;
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        let mut s = self.state.lock().unwrap();
        if s.crashed {
            return Err(injected("crashed"));
        }
        if put && s.faults.crash_after_puts.is_some_and(|n| s.puts >= n) {
            s.crashed = true;
            s.crashing_put = true;
            return Err(injected("crashed"));
        }
        if s.faults.error_rate > 0.0 {
            // splitmix64
            s.rng = s.rng.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = s.rng;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            z ^= z >> 31;
            if ((z >> 11) as f64 / (1u64 << 53) as f64) < s.faults.error_rate {
                return Err(injected("error"));
            }
        }
        Ok(())
    }
}

fn injected(what: &str) -> Error {
    Error::Backend(format!("injected {what}"))
}

#[async_trait]
impl<B: Backend> Backend for FaultyBackend<B> {
    async fn get(&self, key: &str) -> Result<(Bytes, ETag)> {
        self.before(false).await?;
        self.inner.get(key).await
    }

    async fn head(&self, key: &str) -> Result<Option<ETag>> {
        self.before(false).await?;
        self.inner.head(key).await
    }

    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> Result<ETag> {
        let lands = self.state.lock().unwrap().faults.crash_lands_put;
        if let Err(e) = self.before(true).await {
            if lands && self.state.lock().unwrap().puts_at_crash() {
                let _ = self.inner.put(key, body, mode).await;
            }
            return Err(e);
        }
        let etag = self.inner.put(key, body, mode).await?;
        self.state.lock().unwrap().puts += 1;
        Ok(etag)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.before(false).await?;
        self.inner.list(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.before(true).await?;
        self.inner.delete(key).await
    }
}
