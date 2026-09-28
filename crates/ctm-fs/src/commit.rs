//! Commit and push (R2). A commit is local: re-chunk dirty files, rebuild changed trees
//! bottom-up, write the new objects into the mount's outgoing packs, and queue the commit in
//! `state.db`. A push, run in the background, publishes every queued commit with one CAS on
//! the branch ref (auto-forking when another machine moved it).
//!
//! Before the ref update, the last commit pushed, the target ref, and its queue position are
//! recorded in `pending_commit` / `pending_ref` / `pending_seq`. If the process dies (or the
//! request gets no answer) after the update landed, the next push sees the ref already
//! pointing at that commit and just drops the pushed commits from the queue, instead of
//! pushing again and forking against its own push.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::ops::Range;
use std::path::Path;

use futures::{StreamExt, TryStreamExt};
use rusqlite::Transaction;

use ctm_core::layout::paginate;
use ctm_core::rechunk::{Changes, ReadAt, Segment, rechunk_partial};
use ctm_core::{
    Chunk, ChunkList, ChunkRef, Commit, CommitKind, Content, DirEntry, Encoded, Id, Kind, LogEntry,
    PageRef, Tree,
};
use ctm_repo::{BranchName, BranchRef, ForkedFrom, Repo, rfc3339};
use ctm_store::ETag;

use crate::db::{self, WorkDb};
use crate::inode::ROOT;
use crate::state::{Base, FileView, MountState, now_ns};
use crate::{CommitOutcome, Errno, Error, PushOutcome, Result};

/// Files committed at once, and chunks each holds before uploading them: at most
/// 16 × 4 × 4 MiB = 256 MiB in memory.
const COMMIT_FILES: usize = 16;
const UPLOAD_BATCH: usize = 4;
/// Auto-fork names tried: `<branch>.<host>`, then `-2` … up to this.
const FORK_ATTEMPTS: u32 = 100;

/// What building the trees produces, gathered from concurrent tasks.
#[derive(Default)]
struct Built {
    files: HashMap<u64, DirEntry>,
    dirty: Vec<u64>,
    dirs: HashMap<u64, Id>,
    /// File IDs given to changed entries that had none (from format-1 trees), by inode.
    new_ids: HashMap<u64, u64>,
    /// File IDs given to unchanged entries of rewritten format-1 trees: (dir, name, id).
    base_ids: Vec<(u64, Vec<u8>, u64)>,
}

/// A changed node, as of the start of the commit.
struct SnapNode {
    entry: DirEntry,
    dirty: bool,
    base_len: u64,
    base_visible: u64,
    extents: Vec<Range<u64>>,
}

struct Snapshot {
    nodes: HashMap<u64, SnapNode>,
    children: HashMap<u64, Vec<(Vec<u8>, u64)>>,
    whiteouts: HashSet<(u64, Vec<u8>)>,
}

fn errno(e: Errno) -> Error {
    Error::Io(io::Error::from_raw_os_error(e.0))
}

fn parse_id(s: &str) -> Result<Id> {
    s.parse().map_err(|_| {
        Error::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad id {s:?} in state.db"),
        ))
    })
}

pub(crate) fn save_base(t: &Transaction<'_>, base: &Base) -> Result<()> {
    let branch = base
        .branch
        .as_ref()
        .expect("read-write mounts have a branch");
    db::set_meta(t, "branch", branch.as_str())?;
    db::set_meta(t, "base_commit", &base.commit.to_hex())?;
    db::set_meta(t, "base_root", &base.root.to_hex())?;
    let r = base
        .branch_ref
        .as_ref()
        .expect("read-write mounts have a ref");
    db::set_meta(
        t,
        "base_ref",
        &serde_json::to_string(r).expect("ref serializes"),
    )?;
    db::set_meta(
        t,
        "base_etag",
        &base
            .etag
            .as_ref()
            .expect("read-write mounts have an ETag")
            .0,
    )?;
    match &base.auto_forked_from {
        Some(b) => db::set_meta(t, "auto_forked_from", b.as_str()),
        None => db::delete_meta(t, "auto_forked_from"),
    }
}

pub(crate) fn load_base(db: &WorkDb) -> Result<Option<Base>> {
    let Some(branch) = db.meta("branch")? else {
        return Ok(None);
    };
    let get = |k: &str| -> Result<String> {
        db.meta(k)?.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("state.db has no {k}"),
            ))
        })
    };
    let base_ref: BranchRef = serde_json::from_str(&get("base_ref")?)
        .map_err(|e| Error::Io(io::Error::new(io::ErrorKind::InvalidData, e)))?;
    Ok(Some(Base {
        branch: Some(BranchName::new(&branch)?),
        commit: parse_id(&get("base_commit")?)?,
        root: parse_id(&get("base_root")?)?,
        branch_ref: Some(base_ref),
        etag: Some(ETag(get("base_etag")?)),
        auto_forked_from: db
            .meta("auto_forked_from")?
            .map(|b| BranchName::new(&b))
            .transpose()?,
    }))
}

fn clear_pending(t: &Transaction<'_>) -> Result<()> {
    db::delete_meta(t, "pending_commit")?;
    db::delete_meta(t, "pending_ref")?;
    db::delete_meta(t, "pending_seq")
}

/// Whether `commit` is the head of `target`: `Some` with the ref and its ETag if so.
async fn landed(repo: &Repo, target: &BranchName, commit: Id) -> Result<Option<(BranchRef, ETag)>> {
    match repo.read_ref(target).await {
        Ok((r, etag)) if r.head == commit => Ok(Some((r, etag))),
        Ok(_) | Err(ctm_repo::Error::UnknownRef(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Finishes a commit interrupted by a crash in a v0.2 mount, whose commits pushed as they were
/// made: if its ref update landed, the working state is already in the bucket, so it's dropped
/// and the mount moves to the new commit.
pub(crate) async fn recover_at_open(
    repo: &Repo,
    db: &mut WorkDb,
    state_dir: &Path,
    base: Base,
) -> Result<Base> {
    let (Some(commit), Some(target)) = (db.meta("pending_commit")?, db.meta("pending_ref")?) else {
        return Ok(base);
    };
    // A push's marker (R2): the next push resolves it.
    if db.meta("pending_seq")?.is_some() {
        return Ok(base);
    }
    let commit = parse_id(&commit)?;
    let target = BranchName::new(&target)?;
    let Some((r, etag)) = landed(repo, &target, commit).await? else {
        db.tx(clear_pending)?;
        return Ok(base);
    };
    let root = repo.get::<Commit>(&commit).await?.root_tree;
    let forked_from = if base.branch.as_ref() != Some(&target) {
        base.branch.clone()
    } else {
        base.auto_forked_from.clone()
    };
    let new = Base {
        branch: Some(target),
        commit,
        root,
        branch_ref: Some(r),
        etag: Some(etag),
        auto_forked_from: forked_from,
    };
    db.tx(|t| {
        db::clear_working_state(t)?;
        save_base(t, &new)?;
        clear_pending(t)
    })?;
    for f in std::fs::read_dir(state_dir.join("staging"))?.flatten() {
        let _ = std::fs::remove_file(f.path());
    }
    Ok(new)
}

/// Reads a file's merged bytes for the (synchronous) re-chunker.
struct MergedReader<'a> {
    state: &'a MountState,
    view: &'a FileView,
    rt: tokio::runtime::Handle,
}

impl ReadAt for MergedReader<'_> {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let bytes = self
            .rt
            .block_on(self.state.read_view(self.view, offset, buf.len() as u64))
            .map_err(|e| io::Error::from_raw_os_error(e.0))?;
        if bytes.len() != buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "short merged read",
            ));
        }
        buf.copy_from_slice(&bytes);
        Ok(())
    }
}

/// After a local commit: changed entries point at their new content, and nothing is dirty.
fn apply_built(inner: &mut crate::state::Inner, p: &Built) {
    for (ino, e) in &p.files {
        if let Some(n) = inner.inodes.get_mut(*ino) {
            n.entry.content = e.content.clone();
            n.entry.size = e.size;
            n.base_len = e.size;
            n.base_visible = e.size;
            n.dirty = false;
        }
    }
    for (ino, tree) in &p.dirs {
        if let Some(n) = inner.inodes.get_mut(*ino) {
            n.entry.content = Content::Dir(*tree);
        }
    }
    // Entries that just got file IDs keep the inode numbers the kernel knows them by until
    // they're next looked up fresh; the IDs are what later mounts will use.
    for (ino, id) in &p.new_ids {
        if let Some(n) = inner.inodes.get_mut(*ino) {
            n.entry.file_id = Some(*id);
        }
    }
    for (dir, name, id) in &p.base_ids {
        if let Some(ino) = inner.inodes.child(*dir, name)
            && let Some(n) = inner.inodes.get_mut(ino)
            && n.entry.file_id.is_none()
        {
            n.entry.file_id = Some(*id);
        }
    }
    let mut orphans = HashSet::new();
    for (ino, n) in inner.inodes.iter_mut() {
        if n.unlinked {
            orphans.insert(ino);
        } else {
            n.changed = false;
        }
    }
    inner.whiteouts.clear();
    inner.extents.retain(|ino, _| orphans.contains(ino));
}

impl MountState {
    /// Commits the working state locally (R2): re-chunks dirty files, rebuilds the changed trees,
    /// writes the new objects into this mount's outgoing packs durably, and queues the commit to
    /// be pushed. No network. Writes wait while it runs; reads don't. Needs a multi-threaded
    /// tokio runtime.
    pub async fn commit(&self, message: &str) -> Result<CommitOutcome> {
        self.commit_as(CommitKind::Manual, message).await
    }

    /// A commit made because the mount went quiet (or stayed dirty too long).
    pub async fn auto_commit(&self) -> Result<CommitOutcome> {
        self.commit_as(CommitKind::Auto, "").await
    }

    async fn commit_as(&self, kind: CommitKind, message: &str) -> Result<CommitOutcome> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let _gate = self.gate.write().await;
        let snap = self.snapshot();
        if !snap.nodes.contains_key(&ROOT) {
            *self.activity.lock().unwrap() = Default::default();
            return Ok(CommitOutcome::NothingToCommit);
        }
        // Trees are written in the current format; older clients must refuse the repo first.
        self.repo.upgrade_format().await?;
        let built = std::sync::Mutex::new(Built::default());
        let slots = tokio::sync::Semaphore::new(COMMIT_FILES);
        let root = self.build_tree(&snap, ROOT, &built, &slots).await?;
        let built = built.into_inner().unwrap();
        let identity = self.repo.identity();
        let commit = Commit {
            root_tree: root,
            time_ns: now_ns(),
            author: identity.author(),
            machine_id: identity.machine_id,
            kind,
            message: message.to_string(),
        };
        let enc = Encoded::new(self.repo.key(), &commit);
        let id = enc.id;
        self.repo.put_objects(vec![enc]).await?;
        // The objects are on disk before the commit that needs them is queued.
        self.repo.seal().await?;
        {
            let mut inner = self.lock();
            let base = Base {
                commit: id,
                root,
                ..inner.base.clone()
            };
            inner.db().tx(|t| {
                db::add_pending(t, &id, kind, message, commit.time_ns)?;
                db::clear_working_state(t)?;
                save_base(t, &base)
            })?;
            inner.db().sync()?;
            apply_built(&mut inner, &built);
            inner.base = base;
        }
        // Writes wait on the gate, so none happened since the snapshot.
        *self.activity.lock().unwrap() = Default::default();
        for ino in &built.dirty {
            self.drop_staging(*ino);
        }
        self.pushable.notify_one();
        Ok(CommitOutcome::Committed { commit: id })
    }

    /// Wakes when a local commit is waiting to be pushed.
    pub async fn wait_for_commits(&self) {
        self.pushable.notified().await
    }

    /// Commits made locally and not yet pushed: how many, and when the oldest was made.
    pub fn unpushed(&self) -> Result<(usize, Option<i64>)> {
        if self.read_only {
            return Ok((0, None));
        }
        let pending = self.lock().db().pending()?;
        Ok((pending.len(), pending.first().map(|p| p.time_ns)))
    }

    /// One attempt to push every queued commit: uploads the outgoing packs and an index
    /// segment, appends the commits to the branch's log, and CASes the ref, auto-forking if
    /// another machine moved it. After an error, the next attempt first finds out whether this
    /// one's ref update landed.
    pub async fn push(&self) -> Result<PushOutcome> {
        if self.read_only {
            return Ok(PushOutcome::NothingToPush);
        }
        let _one = self.pushing.lock().await;
        // A ref update with no answer: find out whether it landed before trying again.
        let marker = {
            let mut inner = self.lock();
            let db = inner.db();
            match (
                db.meta("pending_commit")?,
                db.meta("pending_ref")?,
                db.meta("pending_seq")?,
            ) {
                (Some(c), Some(r), Some(seq)) => Some((
                    parse_id(&c)?,
                    BranchName::new(&r)?,
                    seq.parse::<i64>().map_err(|_| {
                        Error::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad pending_seq {seq:?} in state.db"),
                        ))
                    })?,
                )),
                _ => None,
            }
        };
        if let Some((commit, target, seq)) = marker {
            match landed(&self.repo, &target, commit).await? {
                Some((r, etag)) => {
                    // An auto-fork's ref landed if the target isn't the mount's branch.
                    let branch = self.lock().base.branch.clone();
                    let from = branch.filter(|b| *b != target);
                    return self.pushed(seq, commit, target, r, etag, from);
                }
                None => self.lock().db().tx(clear_pending)?,
            }
        }
        let pending = self.lock().db().pending()?;
        let Some(last) = pending.last() else {
            return Ok(PushOutcome::NothingToPush);
        };
        let base = self.lock().base.clone();
        let branch = base
            .branch
            .clone()
            .expect("read-write mounts have a branch");
        let current = base
            .branch_ref
            .clone()
            .expect("read-write mounts have a ref");
        let entries = pending
            .iter()
            .map(|p| LogEntry {
                time_ns: p.time_ns,
                commit: p.commit,
                kind: p.kind,
                message: p.message.clone(),
            })
            .collect();
        let new_ref = self.repo.append_log(&current, entries).await?;
        self.set_pending(last.commit, &branch, last.seq)?;
        let etag = base.etag.clone().expect("read-write mounts have an ETag");
        match self.repo.cas_ref(&branch, &new_ref, &etag).await {
            Ok(etag) => self.pushed(last.seq, last.commit, branch, new_ref, etag, None),
            Err(ctm_repo::Error::Store(ctm_store::Error::PreconditionFailed(_))) => {
                self.auto_fork(last, new_ref, &base).await
            }
            Err(e) => {
                // Did it land anyway? If that can't be told either, the next attempt asks again.
                if let Ok(Some((r, etag))) = landed(&self.repo, &branch, last.commit).await {
                    return self.pushed(last.seq, last.commit, branch, r, etag, None);
                }
                Err(e.into())
            }
        }
    }

    fn set_pending(&self, commit: Id, target: &BranchName, seq: i64) -> Result<()> {
        let mut inner = self.lock();
        inner.db().tx(|t| {
            db::set_meta(t, "pending_commit", &commit.to_hex())?;
            db::set_meta(t, "pending_ref", target.as_str())?;
            db::set_meta(t, "pending_seq", &seq.to_string())
        })?;
        inner.db().sync()
    }

    /// The branch moved on another machine: publish the queued commits as `<branch>.<host>`
    /// instead, and follow that branch from now on.
    async fn auto_fork(
        &self,
        last: &db::PendingCommit,
        pushed: BranchRef,
        base: &Base,
    ) -> Result<PushOutcome> {
        let branch = base
            .branch
            .clone()
            .expect("read-write mounts have a branch");
        let from_commit = base
            .branch_ref
            .as_ref()
            .expect("read-write mounts have a ref")
            .head;
        let host = self.repo.identity().hostname.clone();
        for attempt in 1..=FORK_ATTEMPTS {
            let name = branch.auto_fork(&host, attempt)?;
            self.set_pending(last.commit, &name, last.seq)?;
            let now = rfc3339(now_ns());
            let fork = BranchRef {
                head: last.commit,
                log: pushed.log,
                head_hint: None,
                log_hint: None,
                forked_from: Some(ForkedFrom {
                    from: branch.to_string(),
                    commit: from_commit,
                    at: now.clone(),
                }),
                updated_at: now,
                updated_by: self.repo.identity().updated_by(),
            };
            match self.repo.create_ref(&name, &fork).await {
                Ok(etag) => {
                    return self.pushed(last.seq, last.commit, name, fork, etag, Some(branch));
                }
                Err(ctm_repo::Error::AlreadyExists(_)) => continue,
                Err(e) => {
                    if let Ok(Some((r, etag))) = landed(&self.repo, &name, last.commit).await {
                        return self.pushed(last.seq, last.commit, name, r, etag, Some(branch));
                    }
                    return Err(e.into());
                }
            }
        }
        Err(Error::Io(io::Error::other(format!(
            "no free auto-fork name for {branch} after {FORK_ATTEMPTS} tries"
        ))))
    }

    /// The queued commits up to `seq` are on `target`: they leave the queue, and the mount's
    /// pushed ref becomes `r`.
    fn pushed(
        &self,
        seq: i64,
        commit: Id,
        target: BranchName,
        r: BranchRef,
        etag: ETag,
        forked_from: Option<BranchName>,
    ) -> Result<PushOutcome> {
        let outcome = match &forked_from {
            Some(from) => PushOutcome::AutoForked {
                from: from.clone(),
                branch: target.clone(),
                commit,
            },
            None => PushOutcome::Pushed { commit },
        };
        {
            let mut inner = self.lock();
            let base = Base {
                branch: Some(target),
                branch_ref: Some(r),
                etag: Some(etag),
                auto_forked_from: forked_from.or_else(|| inner.base.auto_forked_from.clone()),
                ..inner.base.clone()
            };
            inner.db().tx(|t| {
                db::drop_pending(t, seq)?;
                save_base(t, &base)?;
                clear_pending(t)
            })?;
            inner.base = base;
        }
        self.pushed_notify.notify_waiters();
        Ok(outcome)
    }

    /// Waits until every commit made so far is pushed (the mount process pushes in the
    /// background).
    pub async fn wait_pushed(&self) -> Result<()> {
        loop {
            let pushed = self.pushed_notify.notified();
            tokio::pin!(pushed);
            pushed.as_mut().enable();
            if self.unpushed()?.0 == 0 {
                return Ok(());
            }
            pushed.await;
        }
    }

    /// The changed part of the tree, copied so the commit can work without the lock.
    fn snapshot(&self) -> Snapshot {
        let inner = self.lock();
        let mut nodes = HashMap::new();
        let mut children: HashMap<u64, Vec<(Vec<u8>, u64)>> = HashMap::new();
        for (ino, n) in inner.inodes.iter() {
            if !n.changed || n.unlinked {
                continue;
            }
            nodes.insert(
                ino,
                SnapNode {
                    entry: n.entry.clone(),
                    dirty: n.dirty,
                    base_len: n.base_len,
                    base_visible: n.base_visible,
                    extents: inner.extents.get(&ino).cloned().unwrap_or_default(),
                },
            );
            if ino != ROOT {
                children
                    .entry(n.parent)
                    .or_default()
                    .push((n.entry.name.clone(), ino));
            }
        }
        Snapshot {
            nodes,
            children,
            whiteouts: inner.whiteouts.clone(),
        }
    }

    /// Builds and uploads a changed directory's tree. Changed children are built
    /// concurrently (dirty files each hold one of `slots`), and all of them are uploaded
    /// before the tree that references them.
    async fn build_tree(
        &self,
        snap: &Snapshot,
        dir: u64,
        built: &std::sync::Mutex<Built>,
        slots: &tokio::sync::Semaphore,
    ) -> Result<Id> {
        let node = &snap.nodes[&dir];
        let Content::Dir(base_tree) = node.entry.content else {
            unreachable!("directories hold a tree");
        };
        let base = self.tree(base_tree).await.map_err(errno)?;
        let mut entries: BTreeMap<Vec<u8>, DirEntry> = BTreeMap::new();
        for e in &base.entries {
            if snap.whiteouts.contains(&(dir, e.name.clone())) {
                continue;
            }
            let mut e = e.clone();
            if e.file_id.is_none() {
                // An unchanged entry of a format-1 tree: it gets its file ID now.
                let id = ctm_repo::new_file_id();
                e.file_id = Some(id);
                built
                    .lock()
                    .unwrap()
                    .base_ids
                    .push((dir, e.name.clone(), id));
            }
            entries.insert(e.name.clone(), e);
        }
        let children = snap.children.get(&dir).cloned().unwrap_or_default();
        let changed: Vec<(Vec<u8>, DirEntry)> = futures::stream::iter(children)
            .map(|(name, child)| async move {
                let child = &child;
                let c = &snap.nodes[child];
                let mut entry = match c.entry.content.kind() {
                    Kind::Dir => {
                        let tree = Box::pin(self.build_tree(snap, *child, built, slots)).await?;
                        DirEntry {
                            content: Content::Dir(tree),
                            size: 0,
                            ..c.entry.clone()
                        }
                    }
                    Kind::File if c.dirty => {
                        let _slot = slots.acquire().await.expect("never closed");
                        let entry = self.commit_file(c, *child).await?;
                        built.lock().unwrap().dirty.push(*child);
                        entry
                    }
                    _ => c.entry.clone(),
                };
                if entry.file_id.is_none() {
                    let id = ctm_repo::new_file_id();
                    entry.file_id = Some(id);
                    built.lock().unwrap().new_ids.insert(*child, id);
                }
                if entry.content.kind() != Kind::Dir {
                    built.lock().unwrap().files.insert(*child, entry.clone());
                }
                Ok::<_, Error>((name, entry))
            })
            .buffer_unordered(COMMIT_FILES)
            .try_collect()
            .await?;
        entries.extend(changed);
        let tree = Tree {
            entries: entries.into_values().collect(),
        };
        let enc = Encoded::new(self.repo.key(), &tree);
        let id = enc.id;
        self.repo.put_objects(vec![enc]).await?;
        built.lock().unwrap().dirs.insert(dir, id);
        Ok(id)
    }

    /// Re-chunks a dirty file around its changes, uploads the new chunks (and pages), and
    /// returns its new entry in canonical form.
    async fn commit_file(&self, c: &SnapNode, ino: u64) -> Result<DirEntry> {
        let view = FileView {
            entry: c.entry.clone(),
            size: c.entry.size,
            base_len: c.base_len,
            base_visible: c.base_visible,
            dirty: true,
            extents: c.extents.clone(),
            staging: self.staging_file(ino, false).map_err(errno)?,
        };
        let params = *self.repo.params();
        let content = if view.size <= u64::from(params.inline_max) {
            Content::Inline(self.read_view(&view, 0, view.size).await.map_err(errno)?)
        } else {
            let chunked_base = matches!(
                view.entry.content,
                Content::Chunk(_) | Content::ChunkList(_)
            );
            let (base, changes) = if chunked_base && view.base_visible > 0 {
                let base = self
                    .all_chunks(&view.entry.content, view.base_len)
                    .await
                    .map_err(errno)?;
                let changes = Changes {
                    base_len: view.base_len,
                    base_visible: view.base_visible,
                    size: view.size,
                    extents: view.extents.clone(),
                };
                (base, changes)
            } else {
                // Inline or fully truncated base: chunk the whole file.
                let changes = Changes {
                    base_len: 0,
                    base_visible: 0,
                    size: view.size,
                    extents: Vec::new(),
                };
                (Vec::new(), changes)
            };
            let lens: Vec<u32> = base.iter().map(|c| c.len).collect();
            let segments = tokio::task::block_in_place(|| {
                let mut reader = MergedReader {
                    state: self,
                    view: &view,
                    rt: tokio::runtime::Handle::current(),
                };
                rechunk_partial(&params.chunker, &lens, &changes, &mut reader)
            })?;
            let mut chunks = Vec::with_capacity(segments.len());
            let mut batch = Vec::new();
            for s in segments {
                match s {
                    Segment::Base(i) => chunks.push(base[i]),
                    Segment::New { offset, len } => {
                        let bytes = self
                            .read_view(&view, offset, u64::from(len))
                            .await
                            .map_err(errno)?;
                        let chunk = Chunk(bytes);
                        let enc = Encoded::new(self.repo.key(), &chunk);
                        self.fetcher.cache_chunk(&enc.id, &chunk.0);
                        chunks.push(ChunkRef { id: enc.id, len });
                        batch.push(enc);
                        if batch.len() >= UPLOAD_BATCH {
                            self.repo.put_objects(std::mem::take(&mut batch)).await?;
                        }
                    }
                }
            }
            self.repo.put_objects(batch).await?;
            if let [one] = chunks.as_slice() {
                Content::Chunk(one.id)
            } else {
                let pages = paginate(&chunks);
                let list = ChunkList {
                    pages: pages
                        .iter()
                        .map(|p| PageRef {
                            id: Encoded::new(self.repo.key(), p).id,
                            total_len: p.chunks.iter().map(|c| u64::from(c.len)).sum(),
                        })
                        .collect(),
                };
                let encoded: Vec<Encoded> = pages
                    .iter()
                    .map(|p| Encoded::new(self.repo.key(), p))
                    .collect();
                self.repo.put_objects(encoded).await?;
                let enc = Encoded::new(self.repo.key(), &list);
                let id = enc.id;
                self.repo.put_objects(vec![enc]).await?;
                Content::ChunkList(id)
            }
        };
        Ok(DirEntry {
            name: c.entry.name.clone(),
            mode: c.entry.mode,
            mtime_ns: c.entry.mtime_ns,
            size: view.size,
            content,
            btime_ns: None,
            xattrs: None,
            file_id: c.entry.file_id,
        })
    }
}
