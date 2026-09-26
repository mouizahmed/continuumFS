use std::path::Path;
use std::sync::Arc;

use ctm_core::diff::Change;
use ctm_core::{Encoded, FormatParams, Id, LogEntry, Object, RepoKey};
use ctm_store::{Backend, ETag};

use crate::refs::{BranchName, BranchRef, SnapshotRef};
use crate::refspec::RefSpec;
use crate::{RepoConfig, Result};

/// Who is writing: recorded in commits and refs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub user: String,
    pub hostname: String,
    pub machine_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitOutcome {
    Created,
    /// The prefix already held a repo; this machine is now connected to it.
    Connected,
}

/// A ref resolved to a commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub commit: Id,
    pub root_tree: Id,
    /// Set when the ref names a branch: its current ref and ETag.
    pub branch: Option<(BranchName, BranchRef, ETag)>,
}

/// One changed path in a diff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathChange {
    /// `/`-separated, relative to the root.
    pub path: Vec<u8>,
    pub change: Change,
}

pub struct Repo {
    backend: Arc<dyn Backend>,
    config: RepoConfig,
    key: RepoKey,
    params: FormatParams,
    identity: Identity,
}

impl Repo {
    /// Creates a repo at an empty prefix, or connects to the one already there. Runs the
    /// conditional-write probe either way.
    pub async fn init(
        backend: Arc<dyn Backend>,
        identity: Identity,
    ) -> Result<(Repo, InitOutcome)> {
        let _ = (backend, identity);
        todo!("M1: Repo::init")
    }

    pub async fn open(backend: Arc<dyn Backend>, identity: Identity) -> Result<Repo> {
        let _ = (backend, identity);
        todo!("M1: Repo::open")
    }

    pub fn config(&self) -> &RepoConfig {
        &self.config
    }

    pub fn params(&self) -> &FormatParams {
        &self.params
    }

    pub fn key(&self) -> &RepoKey {
        &self.key
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn backend(&self) -> &Arc<dyn Backend> {
        &self.backend
    }

    // Refs

    pub async fn read_ref(&self, name: &BranchName) -> Result<(BranchRef, ETag)> {
        let _ = name;
        todo!("M1: Repo::read_ref")
    }

    /// Create-only (`If-None-Match: *`).
    pub async fn create_ref(&self, name: &BranchName, new: &BranchRef) -> Result<ETag> {
        let _ = (name, new);
        todo!("M1: Repo::create_ref")
    }

    /// `If-Match: expected`. A lost race is `Error::Store(PreconditionFailed)`.
    pub async fn cas_ref(
        &self,
        name: &BranchName,
        new: &BranchRef,
        expected: &ETag,
    ) -> Result<ETag> {
        let _ = (name, new, expected);
        todo!("M1: Repo::cas_ref")
    }

    pub async fn list_branches(&self) -> Result<Vec<BranchName>> {
        todo!("M1: Repo::list_branches")
    }

    pub async fn read_snapshot(&self, name: &BranchName) -> Result<SnapshotRef> {
        let _ = name;
        todo!("M1: Repo::read_snapshot")
    }

    pub async fn list_snapshots(&self) -> Result<Vec<BranchName>> {
        todo!("M1: Repo::list_snapshots")
    }

    // Objects

    /// Fetches, hash-verifies, and decodes an object.
    pub async fn get<T: Object>(&self, id: &Id) -> Result<T> {
        let _ = id;
        todo!("M1: Repo::get")
    }

    /// Uploads objects that aren't already stored (HEAD, then PUT if missing), chunks first.
    pub async fn put_objects(&self, objs: Vec<Encoded>) -> Result<()> {
        let _ = objs;
        todo!("M1: Repo::put_objects")
    }

    pub async fn resolve(&self, spec: &RefSpec) -> Result<Resolved> {
        let _ = spec;
        todo!("M1: Repo::resolve")
    }

    // Operations that run directly against the bucket

    /// Commits a local directory to a branch (created if missing, CAS otherwise). Returns the commit.
    pub async fn import(&self, dir: &Path, branch: &BranchName, message: &str) -> Result<Id> {
        let _ = (dir, branch, message);
        todo!("M1: Repo::import")
    }

    pub async fn export(&self, spec: &RefSpec, dir: &Path) -> Result<()> {
        let _ = (spec, dir);
        todo!("M1: Repo::export")
    }

    /// Create-only PUT of a new branch sharing `from`'s head (and log, when `from` is a branch).
    pub async fn fork(&self, from: &RefSpec, new: &BranchName) -> Result<BranchRef> {
        let _ = (from, new);
        todo!("M1: Repo::fork")
    }

    pub async fn snapshot(&self, name: &BranchName, from: &RefSpec) -> Result<SnapshotRef> {
        let _ = (name, from);
        todo!("M1: Repo::snapshot")
    }

    /// Newest first. With `path`, only the entries where that path's entry changed.
    pub async fn log(&self, spec: &RefSpec, path: Option<&[u8]>) -> Result<Vec<LogEntry>> {
        let _ = (spec, path);
        todo!("M1: Repo::log")
    }

    pub async fn diff(&self, a: &RefSpec, b: &RefSpec) -> Result<Vec<PathChange>> {
        let _ = (a, b);
        todo!("M1: Repo::diff")
    }
}
