//! A Continuum repository in a bucket: its config, refs, and history, plus the operations
//! that run directly against the bucket (import, export, fork, snapshot, log, diff).

mod check;
pub mod config;
pub mod refs;
pub mod refspec;
mod repo;
mod time;

pub use check::check;
pub use config::RepoConfig;
pub use refs::{BranchName, BranchRef, ForkedFrom, SnapshotRef};
pub use refspec::RefSpec;
pub use repo::{Identity, Imported, InitOutcome, PathChange, Repo, Resolved, Uploaded, object_key};
pub use time::rfc3339;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Store(#[from] ctm_store::Error),
    #[error("corrupt object {id}: {source}")]
    Corrupt {
        id: ctm_core::Id,
        source: ctm_core::encoding::VerifyError,
    },
    #[error("corrupt {what}: {detail}")]
    CorruptJson { what: String, detail: String },
    #[error("repo format_version {0} is newer than this ctm supports (1)")]
    UnsupportedFormat(u32),
    #[error("invalid name {0:?}: use 1–100 characters from [A-Za-z0-9._-]")]
    InvalidName(String),
    #[error("unknown ref {0:?}")]
    UnknownRef(String),
    #[error("ambiguous commit prefix {0:?}")]
    AmbiguousRef(String),
    #[error("{0} already exists")]
    AlreadyExists(String),
    #[error("path {0:?} not found")]
    PathNotFound(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
