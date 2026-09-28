use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::{self, Read, Write};
use std::ops::Range;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt, stream};

use ctm_core::chunker::next_cut;
use ctm_core::diff::{Change, diff_trees};
use ctm_core::encoding::{VerifyError, decode_verified};
use ctm_core::layout::paginate;
use ctm_core::pack::{ENTRY_HEADER, Entry, Location};
use ctm_core::{
    Chunk, ChunkList, ChunkRef, Commit, CommitKind, Content, DirEntry, Encoded, FormatParams, Id,
    Kind, LogEntry, LogSegment, Object, ObjectType, PageRef, RepoKey, Tree,
};
use ctm_store::{Backend, ETag, PutMode};

use crate::config::FORMAT_VERSION;
use crate::objects::{Objects, Uploaded, object_key};
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
    format_version: AtomicU32,
    objects: Objects,
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

    /// The format version of the repo's `config` as far as this handle knows.
    pub fn format_version(&self) -> u32 {
        self.format_version.load(Ordering::Relaxed)
    }

    /// Raises an older repo's `config` to the current format version (a CAS), so older clients
    /// refuse the repo instead of failing on objects they can't decode. Call before writing a
    /// `Tree`.
    pub async fn upgrade_format(&self) -> Result<()> {
        if self.format_version() >= FORMAT_VERSION {
            return Ok(());
        }
        loop {
            let (bytes, etag) = self.backend.get("config").await?;
            let mut config = RepoConfig::parse(&bytes)?;
            if config.format_version >= FORMAT_VERSION {
                break;
            }
            config.format_version = FORMAT_VERSION;
            let body = serde_json::to_vec_pretty(&config).expect("config serializes");
            match self
                .backend
                .put("config", body.into(), PutMode::IfMatch(etag))
                .await
            {
                Ok(_) => break,
                Err(ctm_store::Error::PreconditionFailed(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        self.format_version.store(FORMAT_VERSION, Ordering::Relaxed);
        Ok(())
    }

    fn new(backend: Arc<dyn Backend>, config: RepoConfig, identity: Identity) -> Result<Repo> {
        let key = config.key()?;
        Ok(Repo {
            objects: Objects::new(backend.clone(), key.clone(), None)?,
            key,
            params: config.params(),
            format_version: AtomicU32::new(config.format_version),
            config,
            backend,
            identity,
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
        let r: BranchRef =
            serde_json::from_slice(&bytes).map_err(|e| corrupt_json(&name.branch_key(), e))?;
        let hints = [(r.head, r.head_hint), (r.log, r.log_hint)];
        self.objects.learn(
            &hints
                .into_iter()
                .filter_map(|(id, h)| Some((id, h?)))
                .collect::<Vec<_>>(),
        )?;
        Ok((r, etag))
    }

    /// Uploads everything pending, then serializes `r` with hints for its head and log.
    async fn ref_body(&self, r: &BranchRef) -> Result<Vec<u8>> {
        self.flush().await?;
        let r = BranchRef {
            head_hint: self.objects.hint(&r.head)?,
            log_hint: self.objects.hint(&r.log)?,
            ..r.clone()
        };
        Ok(serde_json::to_vec_pretty(&r).expect("ref serializes"))
    }

    /// Create-only (`If-None-Match: *`).
    pub async fn create_ref(&self, name: &BranchName, new: &BranchRef) -> Result<ETag> {
        let body = self.ref_body(new).await?;
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
        let body = self.ref_body(new).await?;
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
        let s: SnapshotRef = serde_json::from_slice(&bytes).map_err(|e| corrupt_json(&key, e))?;
        if let Some(hint) = s.commit_hint {
            self.objects.learn(&[(s.commit, hint)])?;
        }
        Ok(s)
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
        self.objects.uploaded()
    }

    /// Outgoing mode, for mounts: packs are sealed into local files in `dir` (durably) instead
    /// of uploaded as they fill, and pushed by the next ref write. Packs a previous process left
    /// there are taken over. Call after `with_index_at`.
    pub fn with_outgoing(mut self, dir: &Path) -> Result<Repo> {
        self.objects.set_outgoing(dir)?;
        Ok(self)
    }

    /// Outgoing mode: writes everything added so far to local packs, durably.
    pub async fn seal(&self) -> Result<()> {
        self.objects.seal().await
    }

    /// Outgoing mode: the local files of packs pushed since the last call. The caller may copy
    /// objects out of them (`ctm_core::pack::read_trailer`) into its caches, then deletes them.
    pub fn take_pushed(&self) -> Vec<std::path::PathBuf> {
        self.objects.take_pushed()
    }

    /// Outgoing mode: whether anything written is not yet pushed.
    pub fn has_unpushed(&self) -> bool {
        self.objects.has_unpushed()
    }

    /// Keeps the index mirror in `path` (a SQLite file) instead of in memory, so later
    /// processes start with it. Call right after opening.
    pub fn with_index_at(mut self, path: &Path) -> Result<Repo> {
        self.objects = Objects::new(self.backend.clone(), self.key.clone(), Some(path))?;
        Ok(self)
    }

    /// Fetches, hash-verifies, and decodes an object. A hash mismatch is retried once.
    pub async fn get<T: Object>(&self, id: &Id) -> Result<T> {
        Ok(self.fetch(id).await?.0)
    }

    /// Like `get`, and also returns the object's stored form (`[type][flags][payload]`), for
    /// caching. Remembers where the objects it references are, from its hints.
    pub async fn fetch<T: Object>(&self, id: &Id) -> Result<(T, Bytes)> {
        let mut retried = false;
        loop {
            let fetched = self.objects.read(T::TYPE.is_data(), id).await?;
            match decode_verified::<T>(&self.key, id, &fetched.stored, &self.params) {
                Ok(obj) => {
                    self.objects.mark_known(*id);
                    let refs = obj.refs();
                    if fetched.hints.len() == refs.len() {
                        let learned: Vec<_> = refs
                            .into_iter()
                            .zip(fetched.hints)
                            .filter_map(|(id, h)| Some((id, h?)))
                            .collect();
                        self.objects.learn(&learned)?;
                    }
                    return Ok((obj, fetched.stored));
                }
                Err(VerifyError::HashMismatch { .. }) if !retried => retried = true,
                Err(source) => return Err(Error::Corrupt { id: *id, source }),
            }
        }
    }

    /// Bytes `range` of a chunk's data with one ranged GET where the chunk is stored, without
    /// verifying them (a part of a chunk can't be hash-checked; the caller verifies the chunk
    /// once it has all of it). Falls back to fetching the whole chunk when its location is
    /// unknown or wrong.
    pub async fn chunk_range(&self, id: &Id, range: Range<u32>) -> Result<Bytes> {
        let (start, end) = (u64::from(range.start), u64::from(range.end));
        let direct = match self.objects.locate(id)? {
            Some(loc) => {
                let payload = u64::from(loc.offset) + ENTRY_HEADER as u64;
                (payload + end <= loc.range().end)
                    .then_some((Some(loc), payload + start..payload + end))
            }
            // Loose (written before R1): `[type][flags]` then the chunk.
            None => Some((None, 2 + start..2 + end)),
        };
        if let Some((loc, bytes)) = direct {
            let hinted = loc.is_some();
            let got = match loc {
                Some(loc) => self.objects.pack_range(&loc, true, bytes).await,
                None => {
                    self.objects
                        .get_range(&object_key(ObjectType::Chunk, id), bytes)
                        .await
                }
            };
            match got {
                Ok(b) if b.len() == range.len() => return Ok(b),
                Ok(_) | Err(Error::Store(ctm_store::Error::NotFound(_))) => {
                    if hinted {
                        self.objects.forget(id)?;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        let (chunk, _) = self.fetch::<Chunk>(id).await?;
        let r = range.start as usize..range.end as usize;
        chunk
            .0
            .get(r)
            .map(Bytes::copy_from_slice)
            .ok_or_else(|| Error::PathNotFound(format!("bytes {range:?} of chunk {id}")))
    }

    /// Whole chunks, hash-verified, each as soon as it has arrived (in no particular order).
    /// Chunks stored one after another in a pack come from one streamed ranged GET of up to
    /// `max_span` bytes; the rest, and any a run fails to deliver, are fetched one by one.
    pub fn fetch_chunks(&self, ids: &[Id], max_span: u64) -> BoxStream<'_, (Id, Result<Chunk>)> {
        // Runs of chunks adjacent in one pack; `None` for chunks whose location isn't known.
        type Run = (Option<Location>, VecDeque<(Id, Option<Location>)>);
        let mut runs: Vec<Run> = Vec::new();
        for id in ids {
            let loc = self.objects.locate(id).ok().flatten();
            if let (Some(l), Some((Some(run), members))) = (loc, runs.last_mut())
                && l.pack == run.pack
                && l.offset == run.offset + run.len
                && u64::from(run.len) + u64::from(l.len) <= max_span
            {
                run.len += l.len;
                members.push_back((*id, loc));
                continue;
            }
            runs.push((loc, VecDeque::from([(*id, loc)])));
        }
        let streams = runs
            .into_iter()
            .map(|(run, members)| self.run_chunks(run, members));
        Box::pin(stream::iter(streams).flatten_unordered(None))
    }

    /// The chunks of one run, read from a single streamed GET as their bytes arrive.
    fn run_chunks(
        &self,
        run: Option<Location>,
        members: VecDeque<(Id, Option<Location>)>,
    ) -> BoxStream<'_, (Id, Result<Chunk>)> {
        struct Run {
            span: Option<Location>,
            body: Option<ctm_store::ByteStream>,
            opened: bool,
            /// Bytes of the run received and not yet consumed, starting at `consumed`.
            buf: Vec<u8>,
            consumed: usize,
            members: VecDeque<(Id, Option<Location>)>,
        }
        let state = Run {
            span: run,
            body: None,
            opened: false,
            buf: Vec::new(),
            consumed: 0,
            members,
        };
        Box::pin(stream::unfold(state, move |mut st| async move {
            let (id, loc) = st.members.pop_front()?;
            if !st.opened {
                st.opened = true;
                if let Some(span) = st.span {
                    st.body = self
                        .objects
                        .pack_stream(&span, true, span.range())
                        .await
                        .ok();
                }
            }
            let mut from_run = None;
            if let (Some(span), Some(loc), Some(body)) = (st.span, loc, st.body.as_mut()) {
                let start = (loc.offset - span.offset) as usize;
                let end = start + loc.len as usize;
                while st.consumed + st.buf.len() < end {
                    match body.next().await {
                        Some(Ok(piece)) => st.buf.extend_from_slice(&piece),
                        _ => break,
                    }
                }
                if st.consumed + st.buf.len() >= end {
                    from_run = Entry::parse(&st.buf[start - st.consumed..end - st.consumed])
                        .ok()
                        .and_then(|e| {
                            decode_verified::<Chunk>(&self.key, &id, &e.to_stored(), &self.params)
                                .ok()
                        });
                    st.buf.drain(..end - st.consumed);
                    st.consumed = end;
                } else {
                    // The body ended early: the rest of the run is fetched one by one.
                    st.body = None;
                }
            }
            let result = match from_run {
                Some(c) => {
                    self.objects.mark_known(id);
                    Ok(c)
                }
                None => self.fetch::<Chunk>(&id).await.map(|(c, _)| c),
            };
            Some(((id, result), st))
        }))
    }

    /// Adds objects that aren't already stored to this handle's packs. They're uploaded as packs
    /// fill, and are referenceable once `flush` returns (every ref write flushes first).
    pub async fn put_objects(&self, objs: Vec<Encoded>) -> Result<()> {
        self.objects.put(objs).await
    }

    /// Uploads the open packs and an index segment listing them.
    pub async fn flush(&self) -> Result<()> {
        self.objects.flush().await
    }

    /// Fetches index segments other writers have added since the last sync.
    pub async fn sync_index(&self) -> Result<()> {
        self.objects.sync_index(None).await
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
        for id in self.objects.commits_with_prefix(prefix).await? {
            // Loose `meta/` keys (before R1) may be any metadata type.
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
            file_id: None,
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

    /// Appends entries (oldest first) to a branch's log and returns the ref to CAS, with its head
    /// at the last entry's commit. Adds the new log segments to the packs.
    pub async fn append_log(
        &self,
        current: &BranchRef,
        entries: Vec<LogEntry>,
    ) -> Result<BranchRef> {
        let head_commit = entries.last().expect("at least one entry").commit;
        let mut segment = self.get::<LogSegment>(&current.log).await?;
        let mut prev_id = current.log;
        let mut fresh = false;
        for entry in entries {
            if segment.entries.len() >= LogSegment::MAX_ENTRIES {
                // Full: it's stored (it was either read or put below), and a new one starts.
                if fresh {
                    prev_id = self.put(&segment).await?;
                }
                segment = LogSegment {
                    prev: Some(prev_id),
                    entries: Vec::new(),
                };
            }
            segment.entries.push(entry);
            fresh = true;
        }
        let log = self.put(&segment).await?;
        Ok(BranchRef {
            head: head_commit,
            log,
            head_hint: None,
            log_hint: None,
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
            head_hint: None,
            log_hint: None,
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
                head_hint: None,
                log_hint: None,
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
            commit_hint: None,
            created_at: rfc3339(now_ns()),
            created_by: self.identity.updated_by(),
        };
        self.flush().await?;
        let snap = SnapshotRef {
            commit_hint: self.objects.hint(&snap.commit)?,
            ..snap
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
        self.upgrade_format().await?;
        // Objects other machines stored are skipped too.
        self.sync_index().await?;
        let before = self.uploaded();
        let skipped = AtomicUsize::new(0);
        let slots = tokio::sync::Semaphore::new(IMPORT_FILES);
        // Entries at the same path as on the branch keep their file IDs.
        let base = match self.read_ref(branch).await {
            Ok((r, _)) => Some(self.get::<Commit>(&r.head).await?.root_tree),
            Err(Error::UnknownRef(_)) => None,
            Err(e) => return Err(e),
        };
        let root = self.import_dir(dir, base, &skipped, &slots).await?;
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
                    .append_log(
                        &current,
                        vec![LogEntry {
                            time_ns: now_ns(),
                            commit: id,
                            kind: CommitKind::Import,
                            message: message.to_string(),
                        }],
                    )
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
        base: Option<Id>,
        skipped: &AtomicUsize,
        slots: &tokio::sync::Semaphore,
    ) -> Result<Id> {
        let mut children: Vec<_> = fs::read_dir(dir)?.collect::<io::Result<_>>()?;
        children.sort_by(|a, b| a.file_name().as_bytes().cmp(b.file_name().as_bytes()));
        let base: HashMap<Vec<u8>, DirEntry> = match base {
            Some(t) => self
                .get::<Tree>(&t)
                .await?
                .entries
                .into_iter()
                .map(|e| (e.name.clone(), e))
                .collect(),
            None => HashMap::new(),
        };
        let entries: Vec<Option<DirEntry>> = stream::iter(children)
            .map(|child| {
                let old = base.get(child.file_name().as_bytes()).cloned();
                self.import_entry(child, old, skipped, slots)
            })
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
        old: Option<DirEntry>,
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
            let old_tree = match old.as_ref().map(|e| &e.content) {
                Some(Content::Dir(t)) => Some(*t),
                _ => None,
            };
            let tree = Box::pin(self.import_dir(&path, old_tree, skipped, slots)).await?;
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
        // Same path and kind as on the branch: the same file, so the same ID.
        let file_id = old
            .filter(|o| o.content.kind() == content.kind())
            .and_then(|o| o.file_id)
            .unwrap_or_else(new_file_id);
        Ok(Some(DirEntry {
            name: child.file_name().as_bytes().to_vec(),
            mode,
            mtime_ns,
            size,
            content,
            btime_ns: None,
            xattrs: None,
            file_id: Some(file_id),
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

/// A new random file ID ([`ctm_core::FILE_ID_MIN`] and up).
pub fn new_file_id() -> u64 {
    ctm_core::file_id_from(getrandom::u64().expect("the OS random source works"))
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
