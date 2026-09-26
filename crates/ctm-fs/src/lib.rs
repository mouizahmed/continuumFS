//! A mounted branch: working state, lazy reads, partial writes, and commit.
//!
//! [`MountState`] holds all filesystem logic and is tested directly, without FUSE.
//! [`fuse`] adapts it to the kernel.

pub mod fuse;
mod state;

pub use state::{
    Attr, CommitOutcome, DirItem, Fh, FileKind, MountOptions, MountState, SetAttr, StatFs, Status,
};

/// A filesystem error, carried to the kernel as an errno.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Errno(pub i32);

impl Errno {
    pub const ENOENT: Errno = Errno(libc::ENOENT);
    pub const EEXIST: Errno = Errno(libc::EEXIST);
    pub const ENOTDIR: Errno = Errno(libc::ENOTDIR);
    pub const EISDIR: Errno = Errno(libc::EISDIR);
    pub const ENOTEMPTY: Errno = Errno(libc::ENOTEMPTY);
    pub const EPERM: Errno = Errno(libc::EPERM);
    pub const EROFS: Errno = Errno(libc::EROFS);
    pub const EIO: Errno = Errno(libc::EIO);
    pub const ENOTSUP: Errno = Errno(libc::ENOTSUP);
    pub const ENAMETOOLONG: Errno = Errno(libc::ENAMETOOLONG);
}

pub type FsResult<T> = std::result::Result<T, Errno>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Repo(#[from] ctm_repo::Error),
    #[error("{0} has uncommitted changes for branch {1}; mount that branch to commit them")]
    StateForOtherBranch(String, String),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
