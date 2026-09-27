use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};

use ctm_core::chunker::next_cut;
use ctm_core::diff::{Change, diff_trees};
use ctm_core::encoding::{VerifyError, decode_verified};
use ctm_core::layout::paginate;
use ctm_core::{
    Chunk, ChunkList, ChunkRef, Commit, CommitKind, Content, DirEntry, Encoded, FormatParams, Id,
    Kind, LogEntry, LogSegment, Object, ObjectType, PageRef, RepoKey, Tree,
};
use ctm_store::{Backend, ETag, PutMode};

use crate::refs::{BranchName, BranchRef, ForkedFrom, SnapshotRef};
use crate::refspec::{RefSpec, RefTarget};
use crate::time::{now_ns, rfc3339};
use crate::{Error, RepoConfig, Result};

/// Requests in flight per repo, shared by every operation.
const CONCURRENCY: usize = 64;
/// Files imported at once, and chunks each reads ahead of its uploads: at most
/// 16 × 4 × 4 MiB = 256 MiB in memory.
const IMPORT_FILES: usize = 16;
const IMPORT_BATCH: usize = 4;

/// Who is writing: recorded in commits and refs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub user: String,
    pub hostname: String,
    pub machine_id: [u8; 16],
}

impl Identity {
    /// `"<user>@<hostname>"`, as recorded in commits.
    pub fn author(&self) -> String {
        format!("{}@{}", self.user, self.hostname)
    }

    /// `"<user>@<hostname>/<machine-id>"`, as recorded in refs.
    pub fn updated_by(&self) -> String {
        let h = hex::encode(self.machine_id);
        format!(
            "{}/{}-{}-{}-{}-{}",
            self.author(),
            &h[..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..]
        )
    }
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

/// The result of `import`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Imported {
    pub commit: Id,
    /// FIFOs, sockets, and device nodes, which are skipped with a warning each.
    pub skipped: usize,
    pub uploaded: Uploaded,
}

/// Objects and bytes this repo handle has PUT (objects already stored are skipped).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Uploaded {
    pub objects: u64,
    pub bytes: u64,
}

impl std::ops::Sub for Uploaded {
    type Output = Uploaded;
    fn sub(self, before: Uploaded) -> Uploaded {
        Uploaded {
            objects: self.objects - before.objects,
            bytes: self.bytes - before.bytes,
        }
    }
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
    /// Objects known to be stored, so uploads skip them without a HEAD.
    known: Mutex<HashSet<Id>>,
    requests: tokio::sync::Semaphore,
    uploaded_objects: AtomicU64,
    uploaded_bytes: AtomicU64,
}

/// Where an object is stored: `chunks/<id>` for chunks, `meta/<id>` for everything else.
pub fn object_key(ty: ObjectType, id: &Id) -> String {
    let dir = if ty.is_data() { "chunks" } else { "meta" };
    format!("{dir}/{id}")
}

fn corrupt_json(what: &str, e: impl ToString) -> Error {
    Error::CorruptJson {
        what: what.to_string(),
        detail: e.to_string(),
    }
}

fn not_found(e: &Error) -> bool {
    matches!(e, Error::Store(ctm_store::Error::NotFound(_)))
}

/// Entries compare equal for diff and `log -- <path>` when only times differ.
fn same_content(a: &DirEntry, b: &DirEntry) -> bool {
    a.content == b.content && a.mode == b.mode
}

impl Repo {
    /// Creates a repo at an empty prefix, or connects to the one already there. Runs the
    /// conditional-write probe either way.
    pub async fn init(
        backend: Arc<dyn Backend>,
        identity: Identity,
    ) -> Result<(Repo, InitOutcome)> {
        ctm_store::probe(backend.as_ref()).await?;
        match Repo::open(backend.clone(), identity.clone()).await {
            Ok(repo) => return Ok((repo, InitOutcome::Connected)),
            Err(e) if not_found(&e) => {}
            Err(e) => return Err(e),
        }
        let config = RepoConfig::generate();
        let body = serde_json::to_vec_pretty(&config).expect("config serializes");
        match backend
            .put("config", body.into(), PutMode::CreateOnly)
            .await
        {
            Ok(_) => Ok((Repo::new(backend, config, identity)?, InitOutcome::Created)),
            // Another machine created it first.
            Err(ctm_store::Error::PreconditionFailed(_)) => {
                Ok((Repo::open(backend, identity).await?, InitOutcome::Connected))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn open(backend: Arc<dyn Backend>, identity: Identity) -> Result<Repo> {
        let (bytes, _) = backend.get("config").await?;
        let config = RepoConfig::parse(&bytes)?;
        Repo::new(backend, config, identity)
    }

    fn new(backend: Arc<dyn Backend>, config: RepoConfig, identity: Identity) -> Result<Repo> {
        Ok(Repo {
            key: config.key()?,
            params: config.params(),
            config,
            backend,
            identity,
            known: Mutex::new(HashSet::new()),
            requests: tokio::sync::Semaphore::new(CONCURRENCY),
            uploaded_objects: AtomicU64::new(0),
            uploaded_bytes: AtomicU64::new(0),
        })
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
        let (bytes, etag) = match self.backend.get(&name.branch_key()).await {
            Ok(x) => x,
            Err(ctm_store::Error::NotFound(_)) => {
                return Err(Error::UnknownRef(name.to_string()));
            }
            Err(e) => return Err(e.into()),
        };
        let r = serde_json::from_slice(&bytes).map_err(|e| corrupt_json(&name.branch_key(), e))?;
        Ok((r, etag))
    }

    /// Create-only (`If-None-Match: *`).
    pub async fn create_ref(&self, name: &BranchName, new: &BranchRef) -> Result<ETag> {
        let body = serde_json::to_vec_pretty(new).expect("ref serializes");
        match self
            .backend
            .put(&name.branch_key(), body.into(), PutMode::CreateOnly)
            .await
        {
            Err(ctm_store::Error::PreconditionFailed(_)) => {
                Err(Error::AlreadyExists(format!("branch {name}")))
            }
            r => Ok(r?),
        }
    }

    /// `If-Match: expected`. A lost race is `Error::Store(PreconditionFailed)`.
    pub async fn cas_ref(
        &self,
        name: &BranchName,
        new: &BranchRef,
        expected: &ETag,
    ) -> Result<ETag> {
        let body = serde_json::to_vec_pretty(new).expect("ref serializes");
        Ok(self
            .backend
            .put(
                &name.branch_key(),
                body.into(),
                PutMode::IfMatch(expected.clone()),
            )
            .await?)
    }

    pub async fn list_branches(&self) -> Result<Vec<BranchName>> {
        self.list_names("refs/branches/").await
    }

    pub async fn read_snapshot(&self, name: &BranchName) -> Result<SnapshotRef> {
        let key = name.snapshot_key();
        let bytes = match self.backend.get(&key).await {
            Ok((b, _)) => b,
            Err(ctm_store::Error::NotFound(_)) => {
                return Err(Error::UnknownRef(format!("snap/{name}")));
            }
            Err(e) => return Err(e.into()),
        };
        serde_json::from_slice(&bytes).map_err(|e| corrupt_json(&key, e))
    }

    pub async fn list_snapshots(&self) -> Result<Vec<BranchName>> {
        self.list_names("refs/snapshots/").await
    }

    async fn list_names(&self, prefix: &str) -> Result<Vec<BranchName>> {
        Ok(self
            .backend
            .list(prefix)
            .await?
            .iter()
            .filter_map(|k| BranchName::new(&k[prefix.len()..]).ok())
            .collect())
    }

    // Objects

    /// Everything this handle has uploaded so far.
    pub fn uploaded(&self) -> Uploaded {
        Uploaded {
            objects: self.uploaded_objects.load(Ordering::Relaxed),
            bytes: self.uploaded_bytes.load(Ordering::Relaxed),
        }
    }

    /// Fetches, hash-verifies, and decodes an object. A hash mismatch is retried once.
    pub async fn get<T: Object>(&self, id: &Id) -> Result<T> {
        let key = object_key(T::TYPE, id);
        let mut retried = false;
        loop {
            let (bytes, _) = {
                let _permit = self.requests.acquire().await.expect("never closed");
                self.backend.get(&key).await?
            };
            match decode_verified::<T>(&self.key, id, &bytes, &self.params) {
                Ok(obj) => {
                    self.known.lock().unwrap().insert(*id);
                    return Ok(obj);
                }
                Err(VerifyError::HashMismatch { .. }) if !retried => retried = true,
                Err(source) => return Err(Error::Corrupt { id: *id, source }),
            }
        }
    }

    /// Uploads objects that aren't already stored (HEAD, then PUT if missing), concurrently.
    /// Callers upload children before the objects that reference them.
    pub async fn put_objects(&self, objs: Vec<Encoded>) -> Result<()> {
        stream::iter(objs)
            .map(|obj| self.put_object(obj))
            .buffer_unordered(CONCURRENCY)
            .try_collect::<()>()
            .await
    }

    async fn put_object(&self, obj: Encoded) -> Result<()> {
        if self.known.lock().unwrap().contains(&obj.id) {
            return Ok(());
        }
        let key = object_key(obj.ty, &obj.id);
        let _permit = self.requests.acquire().await.expect("never closed");
        if self.backend.head(&key).await?.is_none() {
            let stored = obj.to_stored();
            let len = stored.len() as u64;
            self.backend
                .put(&key, Bytes::from(stored), PutMode::Overwrite)
                .await?;
            self.uploaded_objects.fetch_add(1, Ordering::Relaxed);
            self.uploaded_bytes.fetch_add(len, Ordering::Relaxed);
        }
        self.known.lock().unwrap().insert(obj.id);
        Ok(())
    }

    fn encode<T: Object>(&self, obj: &T) -> Encoded {
        Encoded::new(&self.key, obj)
    }

    async fn put<T: Object>(&self, obj: &T) -> Result<Id> {
        let enc = self.encode(obj);
        let id = enc.id;
        self.put_objects(vec![enc]).await?;
        Ok(id)
    }

    // Resolving refs

    /// The commit a ref names, without fetching it (so `fork` from a branch costs one GET).
    async fn resolve_commit(
        &self,
        target: &RefTarget,
    ) -> Result<(Id, Option<(BranchName, BranchRef, ETag)>)> {
        match target {
            RefTarget::Branch(name) => {
                let (r, etag) = self.read_ref(name).await?;
                Ok((r.head, Some((name.clone(), r, etag))))
            }
            RefTarget::Snapshot(name) => Ok((self.read_snapshot(name).await?.commit, None)),
            RefTarget::CommitPrefix(prefix) => Ok((self.resolve_prefix(prefix).await?, None)),
        }
    }

    async fn resolve_prefix(&self, prefix: &str) -> Result<Id> {
        let mut found = Vec::new();
        for key in self.backend.list(&format!("meta/{prefix}")).await? {
            let Ok(id) = key["meta/".len()..].parse::<Id>() else {
                continue;
            };
            match self.get::<Commit>(&id).await {
                Ok(_) => found.push(id),
                Err(Error::Corrupt {
                    source: VerifyError::Decode(ctm_core::DecodeError::WrongType { .. }),
                    ..
                }) => {}
                Err(e) => return Err(e),
            }
        }
        match found.as_slice() {
            [id] => Ok(*id),
            [] => Err(Error::UnknownRef(prefix.to_string())),
            _ => Err(Error::AmbiguousRef(prefix.to_string())),
        }
    }

    pub async fn resolve(&self, spec: &RefSpec) -> Result<Resolved> {
        let (commit, branch) = self.resolve_commit(&spec.target).await?;
        let root_tree = self.get::<Commit>(&commit).await?.root_tree;
        Ok(Resolved {
            commit,
            root_tree,
            branch,
        })
    }

    /// The entry at `path` inside a tree; the root is a synthetic directory entry.
    pub async fn entry_at(&self, root: Id, path: Option<&[u8]>) -> Result<Option<DirEntry>> {
        let mut entry = DirEntry {
            name: Vec::new(),
            mode: 0o755,
            mtime_ns: 0,
            size: 0,
            content: Content::Dir(root),
            btime_ns: None,
            xattrs: None,
        };
        for part in path.unwrap_or_default().split(|&b| b == b'/') {
            if part.is_empty() {
                continue;
            }
            let Content::Dir(tree) = entry.content else {
                return Ok(None);
            };
            let tree = self.get::<Tree>(&tree).await?;
            match tree.entries.into_iter().find(|e| e.name == part) {
                Some(e) => entry = e,
                None => return Ok(None),
            }
        }
        Ok(Some(entry))
    }

    async fn resolve_entry(&self, spec: &RefSpec) -> Result<DirEntry> {
        let resolved = self.resolve(spec).await?;
        let path = spec.path.as_deref().map(str::as_bytes);
        self.entry_at(resolved.root_tree, path)
            .await?
            .ok_or_else(|| Error::PathNotFound(spec.path.clone().unwrap_or_default()))
    }

    // History

    /// Appends a commit to a branch's log and returns the ref to CAS. Uploads the new segment.
    pub async fn append_log(
        &self,
        current: &BranchRef,
        commit: Id,
        kind: CommitKind,
        message: &str,
    ) -> Result<BranchRef> {
        let head = self.get::<LogSegment>(&current.log).await?;
        let entry = LogEntry {
            time_ns: now_ns(),
            commit,
            kind,
            message: message.to_string(),
        };
        let segment = if head.entries.len() < LogSegment::MAX_ENTRIES {
            let mut entries = head.entries;
            entries.push(entry);
            LogSegment {
                prev: head.prev,
                entries,
            }
        } else {
            LogSegment {
                prev: Some(current.log),
                entries: vec![entry],
            }
        };
        let log = self.put(&segment).await?;
        Ok(BranchRef {
            head: commit,
            log,
            forked_from: current.forked_from.clone(),
            updated_at: rfc3339(now_ns()),
            updated_by: self.identity.updated_by(),
        })
    }

    /// A new branch ref whose log holds a single entry for `commit`.
    async fn fresh_ref(
        &self,
        commit: Id,
        entry: LogEntry,
        forked_from: Option<ForkedFrom>,
    ) -> Result<BranchRef> {
        let log = self
            .put(&LogSegment {
                prev: None,
                entries: vec![entry],
            })
            .await?;
        Ok(BranchRef {
            head: commit,
            log,
            forked_from,
            updated_at: rfc3339(now_ns()),
            updated_by: self.identity.updated_by(),
        })
    }

    /// Newest first. With `path`, only the entries where that path's entry changed.
    pub async fn log(&self, spec: &RefSpec, path: Option<&[u8]>) -> Result<Vec<LogEntry>> {
        let (commit, branch) = self.resolve_commit(&spec.target).await?;
        let mut entries = Vec::new();
        match branch {
            Some((_, r, _)) => {
                let mut next = Some(r.log);
                while let Some(id) = next {
                    let seg = self.get::<LogSegment>(&id).await?;
                    entries.extend(seg.entries.into_iter().rev());
                    next = seg.prev;
                }
            }
            None => {
                let c = self.get::<Commit>(&commit).await?;
                entries.push(LogEntry {
                    time_ns: c.time_ns,
                    commit,
                    kind: c.kind,
                    message: c.message,
                });
            }
        }
        let Some(path) = path else {
            return Ok(entries);
        };
        let mut at = Vec::with_capacity(entries.len());
        for e in &entries {
            let root = self.get::<Commit>(&e.commit).await?.root_tree;
            at.push(self.entry_at(root, Some(path)).await?);
        }
        Ok(entries
            .into_iter()
            .enumerate()
            .filter(|(i, _)| match (&at[*i], at.get(i + 1)) {
                (None, None | Some(None)) => false,
                (Some(_), None | Some(None)) | (None, Some(Some(_))) => true,
                (Some(a), Some(Some(b))) => !same_content(a, b),
            })
            .map(|(_, e)| e)
            .collect())
    }

    // Operations that run directly against the bucket

    /// Create-only PUT of a new branch sharing `from`'s head (and log, when `from` is a branch).
    pub async fn fork(&self, from: &RefSpec, new: &BranchName) -> Result<BranchRef> {
        let (commit, branch) = self.resolve_commit(&from.target).await?;
        let now = rfc3339(now_ns());
        let r = match branch {
            Some((name, r, _)) => BranchRef {
                head: r.head,
                log: r.log,
                forked_from: Some(ForkedFrom {
                    from: name.to_string(),
                    commit,
                    at: now.clone(),
                }),
                updated_at: now,
                updated_by: self.identity.updated_by(),
            },
            None => {
                let c = self.get::<Commit>(&commit).await?;
                let from = match &from.target {
                    RefTarget::Snapshot(name) => format!("snap/{name}"),
                    _ => commit.to_hex(),
                };
                let entry = LogEntry {
                    time_ns: c.time_ns,
                    commit,
                    kind: c.kind,
                    message: c.message,
                };
                let forked = ForkedFrom {
                    from,
                    commit,
                    at: now,
                };
                self.fresh_ref(commit, entry, Some(forked)).await?
            }
        };
        self.create_ref(new, &r).await?;
        Ok(r)
    }

    pub async fn snapshot(&self, name: &BranchName, from: &RefSpec) -> Result<SnapshotRef> {
        let (commit, _) = self.resolve_commit(&from.target).await?;
        let snap = SnapshotRef {
            commit,
            created_at: rfc3339(now_ns()),
            created_by: self.identity.updated_by(),
        };
        let body = serde_json::to_vec_pretty(&snap).expect("snapshot serializes");
        match self
            .backend
            .put(&name.snapshot_key(), body.into(), PutMode::CreateOnly)
            .await
        {
            Ok(_) => Ok(snap),
            Err(ctm_store::Error::PreconditionFailed(_)) => {
                Err(Error::AlreadyExists(format!("snapshot {name}")))
            }
            Err(e) => Err(e.into()),
        }
    }

    pub async fn diff(&self, a: &RefSpec, b: &RefSpec) -> Result<Vec<PathChange>> {
        let (ea, eb) = (self.resolve_entry(a).await?, self.resolve_entry(b).await?);
        let mut out = Vec::new();
        match (&ea.content, &eb.content) {
            (Content::Dir(x), Content::Dir(y)) => {
                self.diff_dirs(*x, *y, Vec::new(), &mut out).await?
            }
            _ if !same_content(&ea, &eb) => out.push(PathChange {
                path: ea.name.clone(),
                change: Change::Modified { old: ea, new: eb },
            }),
            _ => {}
        }
        Ok(out)
    }

    async fn diff_dirs(
        &self,
        a: Id,
        b: Id,
        prefix: Vec<u8>,
        out: &mut Vec<PathChange>,
    ) -> Result<()> {
        if a == b {
            return Ok(());
        }
        let (ta, tb) = (self.get::<Tree>(&a).await?, self.get::<Tree>(&b).await?);
        for change in diff_trees(&ta, &tb) {
            let mut path = prefix.clone();
            if !path.is_empty() {
                path.push(b'/');
            }
            path.extend_from_slice(change.name());
            if let Change::Modified { old, new } = &change {
                if let (Content::Dir(x), Content::Dir(y)) = (&old.content, &new.content) {
                    Box::pin(self.diff_dirs(*x, *y, path, out)).await?;
                    continue;
                }
                if same_content(old, new) {
                    continue;
                }
            }
            out.push(PathChange { path, change });
        }
        Ok(())
    }

    // Import

    /// Commits a local directory to a branch (created if missing, CAS otherwise).
    pub async fn import(&self, dir: &Path, branch: &BranchName, message: &str) -> Result<Imported> {
        let before = self.uploaded();
        let skipped = AtomicUsize::new(0);
        let slots = tokio::sync::Semaphore::new(IMPORT_FILES);
        let root = self.import_dir(dir, &skipped, &slots).await?;
        let skipped = skipped.into_inner();
        let commit = Commit {
            root_tree: root,
            time_ns: now_ns(),
            author: self.identity.author(),
            machine_id: self.identity.machine_id,
            kind: CommitKind::Import,
            message: message.to_string(),
        };
        let id = self.put(&commit).await?;
        match self.read_ref(branch).await {
            Ok((current, etag)) => {
                let next = self
                    .append_log(&current, id, CommitKind::Import, message)
                    .await?;
                self.cas_ref(branch, &next, &etag).await?;
            }
            Err(Error::UnknownRef(_)) => {
                let entry = LogEntry {
                    time_ns: commit.time_ns,
                    commit: id,
                    kind: CommitKind::Import,
                    message: message.to_string(),
                };
                let r = self.fresh_ref(id, entry, None).await?;
                self.create_ref(branch, &r).await?;
            }
            Err(e) => return Err(e),
        }
        Ok(Imported {
            commit: id,
            skipped,
            uploaded: self.uploaded() - before,
        })
    }

    /// Imports a directory: its entries are imported concurrently (files hold one of `slots`
    /// while they're read and uploaded), then its tree is uploaded.
    async fn import_dir(
        &self,
        dir: &Path,
        skipped: &AtomicUsize,
        slots: &tokio::sync::Semaphore,
    ) -> Result<Id> {
        let mut children: Vec<_> = fs::read_dir(dir)?.collect::<io::Result<_>>()?;
        children.sort_by(|a, b| a.file_name().as_bytes().cmp(b.file_name().as_bytes()));
        let entries: Vec<Option<DirEntry>> = stream::iter(children)
            .map(|child| self.import_entry(child, skipped, slots))
            .buffered(IMPORT_FILES)
            .try_collect()
            .await?;
        self.put(&Tree {
            entries: entries.into_iter().flatten().collect(),
        })
        .await
    }

    async fn import_entry(
        &self,
        child: fs::DirEntry,
        skipped: &AtomicUsize,
        slots: &tokio::sync::Semaphore,
    ) -> Result<Option<DirEntry>> {
        let path = child.path();
        let meta = fs::symlink_metadata(&path)?;
        let ft = meta.file_type();
        let mtime_ns = meta.mtime() * 1_000_000_000 + meta.mtime_nsec();
        let (content, size, mode) = if ft.is_symlink() {
            let target = fs::read_link(&path)?.into_os_string().into_encoded_bytes();
            if target.len() > 4095 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: symlink target too long", path.display()),
                )));
            }
            let size = target.len() as u64;
            (Content::Symlink(target), size, 0o777)
        } else if ft.is_dir() {
            let tree = Box::pin(self.import_dir(&path, skipped, slots)).await?;
            (Content::Dir(tree), 0, perm(&meta))
        } else if ft.is_file() {
            let _slot = slots.acquire().await.expect("never closed");
            let (content, size) = self.import_file(&path).await?;
            (content, size, perm(&meta))
        } else {
            tracing::warn!(
                "skipped {} (not a file, directory, or symlink)",
                path.display()
            );
            skipped.fetch_add(1, Ordering::Relaxed);
            return Ok(None);
        };
        Ok(Some(DirEntry {
            name: child.file_name().as_bytes().to_vec(),
            mode,
            mtime_ns,
            size,
            content,
            btime_ns: None,
            xattrs: None,
        }))
    }

    async fn import_file(&self, path: &Path) -> Result<(Content, u64)> {
        let max = self.params.chunker.max as usize;
        let mut file = fs::File::open(path)?;
        // Read up to two windows at a time, but never zero more than the file needs.
        let read_size = (max * 2).min(file.metadata()?.len() as usize + 1);
        let mut buf: Vec<u8> = Vec::with_capacity(read_size);
        let mut eof = false;
        let mut chunks: Vec<ChunkRef> = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0u64;
        loop {
            // Keep at least one full window buffered, unless the file ends first.
            while !eof && buf.len() < max {
                let old = buf.len();
                buf.resize(old + read_size, 0);
                let n = file.read(&mut buf[old..])?;
                buf.truncate(old + n);
                eof = n == 0;
            }
            if buf.is_empty() {
                break;
            }
            if chunks.is_empty() && eof && buf.len() as u64 <= u64::from(self.params.inline_max) {
                let n = buf.len() as u64;
                return Ok((Content::Inline(buf), n));
            }
            let n = next_cut(&self.params.chunker, &buf, eof).expect("a full window has a cut");
            let chunk = Chunk(buf.drain(..n).collect());
            let enc = self.encode(&chunk);
            chunks.push(ChunkRef {
                id: enc.id,
                len: n as u32,
            });
            size += n as u64;
            batch.push(enc);
            if batch.len() >= IMPORT_BATCH {
                self.put_objects(std::mem::take(&mut batch)).await?;
            }
        }
        self.put_objects(batch).await?;
        match chunks.as_slice() {
            [] => Ok((Content::Inline(Vec::new()), 0)),
            [one] => Ok((Content::Chunk(one.id), size)),
            _ => {
                let pages: Vec<Encoded> =
                    paginate(&chunks).iter().map(|p| self.encode(p)).collect();
                let list = ChunkList {
                    pages: pages
                        .iter()
                        .zip(chunks.chunks(ctm_core::layout::PAGE_MAX))
                        .map(|(p, c)| PageRef {
                            id: p.id,
                            total_len: c.iter().map(|c| u64::from(c.len)).sum(),
                        })
                        .collect(),
                };
                self.put_objects(pages).await?;
                Ok((Content::ChunkList(self.put(&list).await?), size))
            }
        }
    }

    // Export

    pub async fn export(&self, spec: &RefSpec, dir: &Path) -> Result<()> {
        let entry = self.resolve_entry(spec).await?;
        if dir.exists() && fs::read_dir(dir)?.next().is_some() {
            return Err(Error::AlreadyExists(format!(
                "{} (not empty)",
                dir.display()
            )));
        }
        fs::create_dir_all(dir)?;
        match entry.content {
            Content::Dir(tree) => self.export_tree(tree, dir).await,
            _ => {
                let name = String::from_utf8_lossy(&entry.name).into_owned();
                self.export_entry(&entry, &dir.join(name)).await
            }
        }
    }

    async fn export_tree(&self, tree: Id, dir: &Path) -> Result<()> {
        for e in self.get::<Tree>(&tree).await?.entries {
            let path = dir.join(std::ffi::OsStr::from_bytes(&e.name));
            self.export_entry(&e, &path).await?;
        }
        Ok(())
    }

    async fn export_entry(&self, e: &DirEntry, path: &Path) -> Result<()> {
        match &e.content {
            Content::Symlink(target) => {
                std::os::unix::fs::symlink(std::ffi::OsStr::from_bytes(target), path)?;
                return Ok(());
            }
            Content::Dir(tree) => {
                fs::create_dir(path)?;
                Box::pin(self.export_tree(*tree, path)).await?;
            }
            _ => self.export_file(e, path).await?,
        }
        // Mode and mtime last: a read-only directory must be filled first.
        let f = fs::File::open(path)?;
        f.set_permissions(fs::Permissions::from_mode(u32::from(e.mode)))?;
        f.set_modified(time_from_ns(e.mtime_ns))?;
        Ok(())
    }

    /// The chunks of a file entry, in order.
    pub async fn file_chunks(&self, e: &DirEntry) -> Result<Vec<ChunkRef>> {
        match &e.content {
            Content::Chunk(id) => Ok(vec![ChunkRef {
                id: *id,
                len: e.size as u32,
            }]),
            Content::ChunkList(id) => {
                let list = self.get::<ChunkList>(id).await?;
                let pages: Vec<ctm_core::ChunkPage> = stream::iter(&list.pages)
                    .map(|p| self.get(&p.id))
                    .buffered(CONCURRENCY)
                    .try_collect()
                    .await?;
                Ok(pages.into_iter().flat_map(|p| p.chunks).collect())
            }
            _ => Ok(Vec::new()),
        }
    }

    /// Streams a file's bytes to `out`, fetching chunks ahead in order.
    pub async fn write_file_to(&self, e: &DirEntry, out: &mut impl Write) -> Result<()> {
        if let Content::Inline(b) = &e.content {
            out.write_all(b)?;
            return Ok(());
        }
        let chunks = self.file_chunks(e).await?;
        let mut fetched = stream::iter(chunks)
            .map(|c| async move { self.get::<Chunk>(&c.id).await })
            .buffered(16);
        while let Some(chunk) = fetched.next().await {
            out.write_all(&chunk?.0)?;
        }
        Ok(())
    }

    async fn export_file(&self, e: &DirEntry, path: &Path) -> Result<()> {
        let mut f = io::BufWriter::new(fs::File::create(path)?);
        self.write_file_to(e, &mut f).await?;
        f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        Ok(())
    }

    /// The tree at `spec` (`ctm ls`), or the entry itself when it isn't a directory.
    pub async fn ls(&self, spec: &RefSpec) -> Result<Vec<DirEntry>> {
        let entry = self.resolve_entry(spec).await?;
        match entry.content {
            Content::Dir(tree) => Ok(self.get::<Tree>(&tree).await?.entries),
            _ => Ok(vec![entry]),
        }
    }

    /// The file at `spec` (`ctm cat`).
    pub async fn cat(&self, spec: &RefSpec, out: &mut impl Write) -> Result<()> {
        let entry = self.resolve_entry(spec).await?;
        match entry.content.kind() {
            Kind::File => self.write_file_to(&entry, out).await,
            _ => Err(Error::PathNotFound(format!(
                "{} (not a file)",
                spec.path.clone().unwrap_or_default()
            ))),
        }
    }
}

fn perm(meta: &fs::Metadata) -> u16 {
    (meta.mode() & 0o7777) as u16
}

fn time_from_ns(ns: i64) -> SystemTime {
    let d = Duration::new(
        ns.unsigned_abs() / 1_000_000_000,
        (ns.unsigned_abs() % 1_000_000_000) as u32,
    );
    if ns >= 0 {
        SystemTime::UNIX_EPOCH + d
    } else {
        SystemTime::UNIX_EPOCH - d
    }
}
