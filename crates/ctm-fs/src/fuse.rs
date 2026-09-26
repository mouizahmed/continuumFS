//! Adapts [`MountState`] to FUSE through `fuser`.
//!
//! Callbacks are synchronous: each one moves its `Reply` into a tokio task and replies from
//! there, so the callback thread never blocks on I/O. Never touch the mountpoint from inside
//! the mount process.
//!
//! Kernel settings: entry and attr TTL 1 s, no writeback cache, `FOPEN_KEEP_CACHE` for clean
//! files, `max_readahead` and `max_write` 1 MiB.

use std::path::Path;
use std::sync::Arc;

use crate::MountState;

pub struct FuseAdapter {
    state: Arc<MountState>,
    rt: tokio::runtime::Handle,
}

impl FuseAdapter {
    pub fn new(state: Arc<MountState>, rt: tokio::runtime::Handle) -> FuseAdapter {
        FuseAdapter { state, rt }
    }

    pub fn state(&self) -> &Arc<MountState> {
        &self.state
    }

    pub fn runtime(&self) -> &tokio::runtime::Handle {
        &self.rt
    }
}

// Every callback still answers ENOSYS (fuser's defaults) until M2.
impl fuser::Filesystem for FuseAdapter {}

/// Mounts `adapter` at `mountpoint` with `auto_unmount`, returning when the session ends.
pub fn run(adapter: FuseAdapter, mountpoint: &Path, read_only: bool) -> std::io::Result<()> {
    let _ = (adapter, mountpoint, read_only);
    todo!("M2: FUSE session")
}
