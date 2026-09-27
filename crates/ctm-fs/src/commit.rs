//! Commit: re-chunk dirty files, rebuild changed trees bottom-up, then publish with a CAS on
//! the branch ref (auto-forking when another machine moved it).
//!
//! Before the ref update, the commit ID and target ref are recorded in `pending_commit` /
//! `pending_ref`. If the process dies after the update landed but before the working state
//! was cleaned, the next mount sees the ref already pointing at the pending commit and
//! finishes the job, instead of committing again and forking against its own push.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::ops::Range;
use std::path::Path;

use futures::{StreamExt, TryStreamExt};
use rusqlite::Transaction;

use ctm_core::layout::paginate;
use ctm_core::rechunk::{Changes, ReadAt, Segment, rechunk_partial};
use ctm_core::{
    Chunk, ChunkList, ChunkRef, Commit, CommitKind, Content, DirEntry, Encoded, Id, Kind, PageRef,
    Tree,
};
use ctm_repo::{BranchName, BranchRef, ForkedFrom, Repo, rfc3339};
use ctm_store::ETag;

use crate::db::{self, WorkDb};
use crate::inode::ROOT;
use crate::state::{Base, FileView, MountState, Unresolved, now_ns};
use crate::{CommitOutcome, Errno, Error, Result};

/// Files committed at once, and chunks each holds before uploading them: at most
/// 16 × 4 × 4 MiB = 256 MiB in memory.
const COMMIT_FILES: usize = 16;
const UPLOAD_BATCH: usize = 4;
/// Auto-fork names tried: `<branch>.<host>`, then `-2` … up to this.
const FORK_ATTEMPTS: u32 = 100;

/// A commit built and uploaded, not yet published.
pub(crate) struct Prepared {
    commit: Id,
    root: Id,
    /// New entries for changed files and symlinks, by inode.
    files: HashMap<u64, DirEntry>,
    /// Files whose staging can go once the commit lands.
    dirty: Vec<u64>,
    /// New trees for changed directories, by inode.
    dirs: HashMap<u64, Id>,
    /// The ref being written, and where.
    new_ref: BranchRef,
    target: BranchName,
    forked_from: Option<BranchName>,
}

/// What building the trees produces, gathered from concurrent tasks.
#[derive(Default)]
struct Built {
    files: HashMap<u64, DirEntry>,
    dirty: Vec<u64>,
    dirs: HashMap<u64, Id>,
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
    base: Base,
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
    db::delete_meta(t, "pending_ref")
}

/// Whether `commit` is the head of `target`: `Some` with the ref and its ETag if so.
async fn landed(repo: &Repo, target: &BranchName, commit: Id) -> Result<Option<(BranchRef, ETag)>> {
    match repo.read_ref(target).await {
        Ok((r, etag)) if r.head == commit => Ok(Some((r, etag))),
        Ok(_) | Err(ctm_repo::Error::UnknownRef(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Finishes a commit interrupted by a crash: if its ref update landed, the working state is
/// already in the bucket, so it's dropped and the mount moves to the new commit.
pub(crate) async fn recover_at_open(
    repo: &Repo,
    db: &mut WorkDb,
    state_dir: &Path,
    base: Base,
) -> Result<Base> {
    let (Some(commit), Some(target)) = (db.meta("pending_commit")?, db.meta("pending_ref")?) else {
        return Ok(base);
    };
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

impl MountState {
    /// Re-chunks dirty files, uploads, and CASes the branch ref; auto-forks on a lost race.
    /// Writes wait while it runs; reads don't. Needs a multi-threaded tokio runtime.
    pub async fn commit(&self, message: &str) -> Result<CommitOutcome> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let _gate = self.gate.write().await;
        let unresolved = self.lock().unresolved.take();
        if let Some(u) = unresolved {
            match landed(&self.repo, &u.prepared.target, u.prepared.commit).await {
                Ok(Some((r, etag))) => return self.apply(u.prepared, r, etag),
                Ok(None) => self.lock().db().tx(clear_pending)?,
                Err(e) => {
                    self.lock().unresolved = Some(u);
                    return Err(e);
                }
            }
        }
        let snap = self.snapshot();
        if !snap.nodes.contains_key(&ROOT) {
            return Ok(CommitOutcome::NothingToCommit);
        }
        let base = snap.base.clone();
        let branch = base
            .branch
            .clone()
            .expect("read-write mounts have a branch");
        let mut prepared = Prepared {
            commit: Id([0; 32]),
            root: Id([0; 32]),
            files: HashMap::new(),
            dirty: Vec::new(),
            dirs: HashMap::new(),
            new_ref: base
                .branch_ref
                .clone()
                .expect("read-write mounts have a ref"),
            target: branch.clone(),
            forked_from: None,
        };
        let built = std::sync::Mutex::new(Built::default());
        let slots = tokio::sync::Semaphore::new(COMMIT_FILES);
        prepared.root = self.build_tree(&snap, ROOT, &built, &slots).await?;
        let built = built.into_inner().unwrap();
        prepared.files = built.files;
        prepared.dirty = built.dirty;
        prepared.dirs = built.dirs;
        let identity = self.repo.identity();
        let commit = Commit {
            root_tree: prepared.root,
            time_ns: now_ns(),
            author: identity.author(),
            machine_id: identity.machine_id,
            kind: CommitKind::Manual,
            message: message.to_string(),
        };
        let enc = Encoded::new(self.repo.key(), &commit);
        prepared.commit = enc.id;
        self.repo.put_objects(vec![enc]).await?;
        prepared.new_ref = self
            .repo
            .append_log(
                &prepared.new_ref,
                prepared.commit,
                CommitKind::Manual,
                message,
            )
            .await?;

        self.set_pending(prepared.commit, &branch)?;
        let etag = base.etag.clone().expect("read-write mounts have an ETag");
        match self.repo.cas_ref(&branch, &prepared.new_ref, &etag).await {
            Ok(etag) => {
                let r = prepared.new_ref.clone();
                self.apply(prepared, r, etag)
            }
            Err(ctm_repo::Error::Store(ctm_store::Error::PreconditionFailed(_))) => {
                self.auto_fork(prepared, &base).await
            }
            Err(e) => self.unknown_outcome(prepared, e.into()).await,
        }
    }

    fn set_pending(&self, commit: Id, target: &BranchName) -> Result<()> {
        let mut inner = self.lock();
        inner.db().tx(|t| {
            db::set_meta(t, "pending_commit", &commit.to_hex())?;
            db::set_meta(t, "pending_ref", target.as_str())
        })?;
        inner.db().sync()
    }

    /// The branch moved: publish the commit as `<branch>.<host>` instead.
    async fn auto_fork(&self, mut prepared: Prepared, base: &Base) -> Result<CommitOutcome> {
        let branch = prepared.target.clone();
        let host = self.repo.identity().hostname.clone();
        for attempt in 1..=FORK_ATTEMPTS {
            let name = branch.auto_fork(&host, attempt)?;
            self.set_pending(prepared.commit, &name)?;
            let now = rfc3339(now_ns());
            let fork = BranchRef {
                head: prepared.commit,
                log: prepared.new_ref.log,
                forked_from: Some(ForkedFrom {
                    from: branch.to_string(),
                    commit: base.commit,
                    at: now.clone(),
                }),
                updated_at: now,
                updated_by: self.repo.identity().updated_by(),
            };
            prepared.target = name.clone();
            prepared.forked_from = Some(branch.clone());
            prepared.new_ref = fork.clone();
            match self.repo.create_ref(&name, &fork).await {
                Ok(etag) => return self.apply(prepared, fork, etag),
                Err(ctm_repo::Error::AlreadyExists(_)) => continue,
                Err(e) => return self.unknown_outcome(prepared, e.into()).await,
            }
        }
        Err(Error::Io(io::Error::other(format!(
            "no free auto-fork name for {branch} after {FORK_ATTEMPTS} tries"
        ))))
    }

    /// The ref update failed without an answer: look at the ref to find out whether it
    /// landed. If that can't be told either, writes stop until the next `commit` resolves it.
    async fn unknown_outcome(&self, prepared: Prepared, error: Error) -> Result<CommitOutcome> {
        for _ in 0..3 {
            match landed(&self.repo, &prepared.target, prepared.commit).await {
                Ok(Some((r, etag))) => return self.apply(prepared, r, etag),
                Ok(None) => {
                    self.lock().db().tx(clear_pending)?;
                    return Err(error);
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
        self.lock().unresolved = Some(Unresolved { prepared });
        Err(error)
    }

    /// The commit landed: the working state becomes the new base.
    fn apply(&self, p: Prepared, r: BranchRef, etag: ETag) -> Result<CommitOutcome> {
        let outcome = match &p.forked_from {
            Some(from) => CommitOutcome::AutoForked {
                from: from.clone(),
                branch: p.target.clone(),
                commit: p.commit,
            },
            None => CommitOutcome::Pushed { commit: p.commit },
        };
        {
            let mut inner = self.lock();
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
            let base = Base {
                branch: Some(p.target.clone()),
                commit: p.commit,
                root: p.root,
                branch_ref: Some(r),
                etag: Some(etag),
                auto_forked_from: p
                    .forked_from
                    .clone()
                    .or_else(|| inner.base.auto_forked_from.clone()),
            };
            inner.db().tx(|t| {
                db::clear_working_state(t)?;
                save_base(t, &base)?;
                clear_pending(t)
            })?;
            inner.base = base;
        }
        for ino in &p.dirty {
            self.drop_staging(*ino);
        }
        Ok(outcome)
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
            base: inner.base.clone(),
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
        let mut entries: BTreeMap<Vec<u8>, DirEntry> = base
            .entries
            .iter()
            .filter(|e| !snap.whiteouts.contains(&(dir, e.name.clone())))
            .map(|e| (e.name.clone(), e.clone()))
            .collect();
        let children = snap
            .children
            .get(&dir)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let changed: Vec<(Vec<u8>, DirEntry)> = futures::stream::iter(children)
            .map(|(name, child)| async move {
                let c = &snap.nodes[child];
                let entry = match c.entry.content.kind() {
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
                if entry.content.kind() != Kind::Dir {
                    built.lock().unwrap().files.insert(*child, entry.clone());
                }
                Ok::<_, Error>((name.clone(), entry))
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
        })
    }
}
