use std::path::Path;
use std::sync::Arc;

use ctm_core::Id;
use ctm_repo::{BranchName, RefSpec, Repo};

use crate::{FsResult, Result};

#[derive(Clone, Debug)]
pub struct MountOptions {
    pub read_only: bool,
    /// Requests in flight per mount, shared by reads, prefetch, and uploads.
    pub max_concurrency: usize,
    pub chunk_cache_max: u64,
}

impl Default for MountOptions {
    fn default() -> MountOptions {
        MountOptions {
            read_only: false,
            max_concurrency: 64,
            chunk_cache_max: 20 << 30,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    File,
    Dir,
    Symlink,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attr {
    pub ino: u64,
    pub kind: FileKind,
    pub size: u64,
    pub mode: u16,
    pub mtime_ns: i64,
    pub nlink: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SetAttr {
    pub size: Option<u64>,
    pub mode: Option<u16>,
    pub mtime_ns: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirItem {
    pub ino: u64,
    pub name: Vec<u8>,
    pub kind: FileKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatFs {
    pub total_bytes: u64,
    pub free_bytes: u64,
}

/// An open file handle; readahead state is tracked per handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fh(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    Pushed {
        commit: Id,
    },
    /// The branch moved on another machine; the commit went to `branch`, and the mount follows it.
    AutoForked {
        branch: BranchName,
        commit: Id,
    },
    NothingToCommit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub branch: BranchName,
    pub base_commit: Id,
    pub dirty_files: u64,
    /// The remote ref's ETag differs from this mount's base.
    pub behind: bool,
    /// Set after an auto-fork: the branch the mount was on before.
    pub auto_forked_from: Option<BranchName>,
}

/// All filesystem logic for one mounted ref.
pub struct MountState {
    repo: Arc<Repo>,
}

impl MountState {
    /// Opens (or recovers) the working state in `state_dir` for `spec`, finishing any
    /// interrupted commit first. Snapshots and commit IDs always mount read-only.
    pub async fn open(
        repo: Arc<Repo>,
        state_dir: &Path,
        spec: &RefSpec,
        opts: MountOptions,
    ) -> Result<MountState> {
        let _ = (repo, state_dir, spec, opts);
        todo!("M2: MountState::open")
    }

    pub fn repo(&self) -> &Arc<Repo> {
        &self.repo
    }

    pub async fn lookup(&self, parent: u64, name: &[u8]) -> FsResult<Attr> {
        let _ = (parent, name);
        todo!("M2: lookup")
    }

    pub fn forget(&self, ino: u64, nlookup: u64) {
        let _ = (ino, nlookup);
        todo!("M2: forget")
    }

    pub async fn getattr(&self, ino: u64) -> FsResult<Attr> {
        let _ = ino;
        todo!("M2: getattr")
    }

    pub async fn setattr(&self, ino: u64, set: SetAttr) -> FsResult<Attr> {
        let _ = (ino, set);
        todo!("M3: setattr")
    }

    pub async fn readdir(&self, ino: u64) -> FsResult<Vec<DirItem>> {
        let _ = ino;
        todo!("M2: readdir")
    }

    pub async fn open_file(&self, ino: u64, write: bool) -> FsResult<Fh> {
        let _ = (ino, write);
        todo!("M2: open")
    }

    pub async fn read(&self, fh: Fh, ino: u64, offset: u64, len: u32) -> FsResult<Vec<u8>> {
        let _ = (fh, ino, offset, len);
        todo!("M2: read")
    }

    pub async fn write(&self, fh: Fh, ino: u64, offset: u64, data: &[u8]) -> FsResult<u32> {
        let _ = (fh, ino, offset, data);
        todo!("M3: write")
    }

    pub async fn create(&self, parent: u64, name: &[u8], mode: u16) -> FsResult<(Attr, Fh)> {
        let _ = (parent, name, mode);
        todo!("M3: create")
    }

    pub async fn mkdir(&self, parent: u64, name: &[u8], mode: u16) -> FsResult<Attr> {
        let _ = (parent, name, mode);
        todo!("M3: mkdir")
    }

    pub async fn unlink(&self, parent: u64, name: &[u8]) -> FsResult<()> {
        let _ = (parent, name);
        todo!("M3: unlink")
    }

    pub async fn rmdir(&self, parent: u64, name: &[u8]) -> FsResult<()> {
        let _ = (parent, name);
        todo!("M3: rmdir")
    }

    pub async fn rename(
        &self,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
    ) -> FsResult<()> {
        let _ = (parent, name, new_parent, new_name);
        todo!("M3: rename")
    }

    pub async fn symlink(&self, parent: u64, name: &[u8], target: &[u8]) -> FsResult<Attr> {
        let _ = (parent, name, target);
        todo!("M3: symlink")
    }

    pub async fn readlink(&self, ino: u64) -> FsResult<Vec<u8>> {
        let _ = ino;
        todo!("M2: readlink")
    }

    /// Makes the file's written bytes and extent map durable on this machine.
    pub async fn fsync(&self, ino: u64) -> FsResult<()> {
        let _ = ino;
        todo!("M3: fsync")
    }

    pub async fn release(&self, fh: Fh) -> FsResult<()> {
        let _ = fh;
        todo!("M2: release")
    }

    pub fn statfs(&self) -> FsResult<StatFs> {
        todo!("M2: statfs")
    }

    /// Re-chunks dirty files, uploads, and CASes the branch ref; auto-forks on a lost race.
    pub async fn commit(&self, message: &str) -> Result<CommitOutcome> {
        let _ = message;
        todo!("M3: commit")
    }

    pub async fn status(&self) -> Result<Status> {
        todo!("M3: status")
    }

    /// Replaces the entry at `path` with the one in `from`, and invalidates the kernel's cache of it.
    pub async fn restore(&self, path: &[u8], from: &RefSpec) -> Result<()> {
        let _ = (path, from);
        todo!("M3: restore")
    }
}
