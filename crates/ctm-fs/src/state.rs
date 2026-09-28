use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::ops::Range;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{StreamExt, stream};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;

use ctm_core::layout::PAGE_MAX;
use ctm_core::{
    ChunkList, ChunkPage, ChunkRef, Commit, Content, DirEntry, Id, Kind, Object, ObjectType, Tree,
};
use ctm_repo::refspec::RefTarget;
use ctm_repo::{BranchName, BranchRef, RefSpec, Repo};
use ctm_store::ETag;

use crate::db::{self, WorkDb};
use crate::fetch::{FetchError, Fetcher, READAHEAD_SPAN};
use crate::inode::{Inodes, Node, ROOT};
use crate::{Errno, Error, FsResult, Result};

/// Readahead window, in bytes past the chunk being read: where it starts once a handle reads
/// sequentially, and its cap. It doubles each time the reader moves on to a new chunk.
const READAHEAD_START: u64 = 2 << 20;
const READAHEAD_MAX: u64 = 128 << 20;
/// A read this close to where the handle's reads have got to still counts as sequential: the
/// kernel sends a file's readahead as several reads at once, and they can arrive out of order.
const SEQUENTIAL_SLACK: u64 = 1 << 20;
/// Trees fetched at once by the background metadata walk.
const WALK_CONCURRENCY: usize = 32;

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

impl From<Kind> for FileKind {
    fn from(k: Kind) -> FileKind {
        match k {
            Kind::File => FileKind::File,
            Kind::Dir => FileKind::Dir,
            Kind::Symlink => FileKind::Symlink,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Attr {
    pub ino: u64,
    pub kind: FileKind,
    pub size: u64,
    pub mode: u16,
    pub mtime_ns: i64,
    /// Always 1, including directories: counting subdirectories would mean fetching every
    /// directory's tree on `stat`, and 1 tells `find` not to rely on link counts.
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

/// `statfs` of the disk holding the cache and working state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatFs {
    pub block_size: u64,
    pub blocks: u64,
    pub blocks_free: u64,
    pub blocks_avail: u64,
}

/// An open file handle; readahead state is tracked per handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fh(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Committed locally, and queued to be pushed.
    Committed {
        commit: Id,
    },
    NothingToCommit,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    /// The queued commits, up to `commit`, are on the branch.
    Pushed {
        commit: Id,
    },
    /// The branch moved on another machine; the commits went to `branch`, and the mount
    /// follows it.
    AutoForked {
        from: BranchName,
        branch: BranchName,
        commit: Id,
    },
    NothingToPush,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    /// `None` for a snapshot or commit mount.
    pub branch: Option<BranchName>,
    pub base_commit: Id,
    /// Files and symlinks with uncommitted changes, plus deleted entries.
    pub dirty_files: u64,
    /// Commits made here and not yet pushed, and when the oldest of them was made.
    pub unpushed: u64,
    pub oldest_unpushed_ns: Option<i64>,
    /// The remote ref's ETag differs from this mount's base.
    pub behind: bool,
    /// Set after an auto-fork: the branch the mount was on before.
    pub auto_forked_from: Option<BranchName>,
    pub read_only: bool,
}

/// Receives invalidations (the FUSE session's notifier).
pub type Notifier = Box<dyn Fn(Invalidate) + Send + Sync>;

/// A kernel cache entry to drop after the mount's contents change underneath it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Invalidate {
    Inode(u64),
    Entry { parent: u64, name: Vec<u8> },
}

/// Where a file's chunks are: one page per 4096 chunks, each loaded on first use.
pub(crate) struct Layout {
    pages: Vec<PageSpan>,
}

/// A page's chunks with their offsets in the file, sorted.
type PageChunks = Arc<Vec<(u64, ChunkRef)>>;

struct PageSpan {
    start: u64,
    source: PageSource,
    chunks: OnceCell<PageChunks>,
}

enum PageSource {
    Single(ChunkRef),
    Page(Id),
}

/// A chunk's position: page index and index within the page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Pos {
    page: usize,
    index: usize,
}

impl Pos {
    fn global(self) -> u64 {
        (self.page * PAGE_MAX + self.index) as u64
    }
}

#[derive(Default)]
struct Handle {
    ino: u64,
    next_offset: u64,
    sequential: u32,
    /// Readahead window in bytes; 0 until the handle reads sequentially.
    window: u64,
    last_chunk: Option<u64>,
    /// Chunks up to this global index have been handed to readahead.
    prefetched_to: Option<u64>,
}

/// What the mount is based on.
#[derive(Clone, Debug)]
pub(crate) struct Base {
    pub branch: Option<BranchName>,
    pub commit: Id,
    pub root: Id,
    pub branch_ref: Option<BranchRef>,
    pub etag: Option<ETag>,
    pub auto_forked_from: Option<BranchName>,
}

/// When the working state last changed, and since when it has had uncommitted changes.
#[derive(Default)]
pub(crate) struct Activity {
    pub last_write: Option<std::time::Instant>,
    pub dirty_since: Option<std::time::Instant>,
}

/// Everything that changes, behind one lock.
pub(crate) struct Inner {
    pub inodes: Inodes,
    pub whiteouts: HashSet<(u64, Vec<u8>)>,
    pub extents: HashMap<u64, Vec<Range<u64>>>,
    /// `None` for read-only mounts, which keep no working state.
    pub db: Option<WorkDb>,
    pub base: Base,
}

impl Inner {
    pub fn db(&mut self) -> &mut WorkDb {
        self.db
            .as_mut()
            .expect("read-write mounts have a working state")
    }

    /// Marks `ino` and its ancestors changed, and saves them and `ino`'s extents.
    pub fn touch(&mut self, ino: u64) -> Result<()> {
        let mut save = vec![ino];
        self.inodes
            .get_mut(ino)
            .expect("touching a live node")
            .changed = true;
        let mut cur = ino;
        while cur != ROOT {
            let parent = self.inodes.get(cur).expect("live").parent;
            let p = self.inodes.get_mut(parent).expect("parents are live");
            if p.changed {
                break;
            }
            p.changed = true;
            save.push(parent);
            cur = parent;
        }
        let Inner {
            inodes,
            extents,
            db,
            ..
        } = self;
        let no_extents = Vec::new();
        db.as_mut().expect("read-write").tx(|t| {
            for i in &save {
                db::save_node(t, *i, inodes.get(*i).expect("live"))?;
            }
            db::set_extents(t, ino, extents.get(&ino).unwrap_or(&no_extents))
        })
    }
}

/// A file's bytes as of one moment: base content, dirty extents, and its staging file.
pub(crate) struct FileView {
    pub entry: DirEntry,
    pub size: u64,
    /// Length of the base content in `entry`.
    pub base_len: u64,
    pub base_visible: u64,
    pub dirty: bool,
    pub extents: Vec<Range<u64>>,
    pub staging: Option<Arc<File>>,
}

/// All filesystem logic for one mounted ref.
pub struct MountState {
    pub(crate) repo: Arc<Repo>,
    pub(crate) fetcher: Arc<Fetcher>,
    pub(crate) inner: Mutex<Inner>,
    /// Write operations hold it shared; commit holds it exclusively. Reads never take it.
    pub(crate) gate: tokio::sync::RwLock<()>,
    /// One push at a time.
    pub(crate) pushing: tokio::sync::Mutex<()>,
    /// Signalled by each local commit, for the pusher.
    pub(crate) pushable: tokio::sync::Notify,
    /// Signalled after each successful push, for `wait_pushed`.
    pub(crate) pushed_notify: tokio::sync::Notify,
    /// When writes happened since the last commit, for auto-commits.
    pub(crate) activity: Mutex<Activity>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
    layouts: Mutex<HashMap<Id, Arc<Layout>>>,
    prefetched_dirs: Mutex<HashSet<Id>>,
    staging: Mutex<HashMap<u64, Arc<File>>>,
    pub(crate) read_only: bool,
    cache_dir: PathBuf,
    pub(crate) state_dir: PathBuf,
    pub(crate) empty_tree: Id,
    notify: Mutex<Option<Notifier>>,
    walker: JoinHandle<()>,
}

impl Drop for MountState {
    fn drop(&mut self) {
        self.walker.abort();
    }
}

pub(crate) fn eio(e: FetchError) -> Errno {
    tracing::error!("{e}");
    Errno::EIO
}

pub(crate) fn fail(e: Error) -> Errno {
    tracing::error!("{e}");
    Errno::EIO
}

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

fn attr(ino: u64, n: &Node) -> Attr {
    Attr {
        ino,
        kind: n.kind().into(),
        size: n.entry.size,
        mode: n.entry.mode,
        mtime_ns: n.entry.mtime_ns,
        nlink: 1,
    }
}

/// A stable-looking number for `readdir` entries the kernel hasn't looked up yet. It never
/// collides with assigned inodes, which count up from 1.
fn synthetic_ino(parent: u64, name: &[u8]) -> u64 {
    let h = blake3::Hasher::new()
        .update(&parent.to_le_bytes())
        .update(name)
        .finalize();
    (1 << 63) | u64::from_le_bytes(h.as_bytes()[..8].try_into().expect("8 bytes"))
}

fn check_name(name: &[u8]) -> FsResult<()> {
    if name.len() > 255 {
        return Err(Errno::ENAMETOOLONG);
    }
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
        return Err(Errno::EINVAL);
    }
    Ok(())
}

/// Adds `r` to sorted, non-overlapping extents, merging where they touch.
fn merge_extent(extents: &mut Vec<Range<u64>>, r: Range<u64>) {
    let mut merged = r;
    extents.retain(|e| {
        if e.end < merged.start || e.start > merged.end {
            true
        } else {
            merged = merged.start.min(e.start)..merged.end.max(e.end);
            false
        }
    });
    let at = extents.partition_point(|e| e.start < merged.start);
    extents.insert(at, merged);
}

fn io_errno(e: Errno) -> Error {
    Error::Io(std::io::Error::from_raw_os_error(e.0))
}

/// A merged directory listing entry: a node in memory, or a base entry not looked up yet.
enum Listed {
    Node(u64),
    Base(DirEntry),
}

impl MountState {
    /// Opens (or recovers) the working state in `state_dir` for `spec`, finishing an
    /// interrupted commit first. Snapshots and commit IDs always mount read-only.
    /// `cache_dir` is the repo's shared cache directory.
    pub async fn open(
        repo: Arc<Repo>,
        cache_dir: &Path,
        state_dir: &Path,
        spec: &RefSpec,
        opts: MountOptions,
    ) -> Result<MountState> {
        if spec.path.is_some() {
            return Err(Error::PathInRef);
        }
        let fetcher = Arc::new(Fetcher::new(
            repo.clone(),
            cache_dir,
            opts.chunk_cache_max,
            opts.max_concurrency,
        )?);
        let empty_tree = Id::compute(repo.key(), ObjectType::Tree, &Tree::default().encode());
        fetcher.spawn_flusher();
        let branch = match &spec.target {
            RefTarget::Branch(b) => Some(b.clone()),
            _ => None,
        };
        let read_only = opts.read_only || branch.is_none();
        let mut rows = Vec::new();
        let mut whiteouts = HashSet::new();
        let mut extents = HashMap::new();
        let (db, base) = if read_only {
            let r = repo.resolve(spec).await?;
            let base = Base {
                branch,
                commit: r.commit,
                root: r.root_tree,
                branch_ref: r.branch.as_ref().map(|(_, b, _)| b.clone()),
                etag: r.branch.map(|(_, _, e)| e),
                auto_forked_from: None,
            };
            (None, base)
        } else {
            let branch = branch.expect("read-write mounts are of branches");
            let mut db = WorkDb::open(state_dir)?;
            let base = match crate::commit::load_base(&db)? {
                Some(base) => {
                    if base.branch.as_ref() != Some(&branch) {
                        return Err(Error::StateForOtherBranch(
                            state_dir.display().to_string(),
                            base.branch.map(|b| b.to_string()).unwrap_or_default(),
                        ));
                    }
                    crate::commit::recover_at_open(&repo, &mut db, state_dir, base).await?
                }
                None => {
                    let r = repo.resolve(spec).await?;
                    let (_, branch_ref, etag) = r.branch.expect("branch refs resolve to a branch");
                    let base = Base {
                        branch: Some(branch),
                        commit: r.commit,
                        root: r.root_tree,
                        branch_ref: Some(branch_ref),
                        etag: Some(etag),
                        auto_forked_from: None,
                    };
                    db.tx(|t| crate::commit::save_base(t, &base))?;
                    base
                }
            };
            let (r, w, e) = db.load()?;
            rows = r.into_iter().map(|row| (row.ino, row.node)).collect();
            whiteouts = w.into_iter().collect();
            extents = e;
            (Some(db), base)
        };
        let commit = fetcher
            .meta::<Commit>(&base.commit, false)
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let root = DirEntry {
            name: Vec::new(),
            mode: 0o755,
            mtime_ns: commit.time_ns,
            size: 0,
            content: Content::Dir(base.root),
            btime_ns: None,
            xattrs: None,
            file_id: None,
        };
        let mut inodes = Inodes::new(root);
        let live: HashSet<u64> = rows.iter().map(|(i, _)| *i).collect();
        inodes.load(rows);
        if !read_only {
            // Staging files with no row belong to files unlinked before a crash.
            for f in std::fs::read_dir(state_dir.join("staging"))?.flatten() {
                let ino = f.file_name().to_string_lossy().parse::<u64>().ok();
                if ino.is_none_or(|i| !live.contains(&i)) {
                    let _ = std::fs::remove_file(f.path());
                }
            }
        }
        let walker = tokio::spawn(walk_metadata(fetcher.clone(), base.root, empty_tree));
        Ok(MountState {
            read_only,
            repo,
            fetcher,
            inner: Mutex::new(Inner {
                inodes,
                whiteouts,
                extents,
                db,
                base,
            }),
            gate: tokio::sync::RwLock::new(()),
            pushing: tokio::sync::Mutex::new(()),
            pushable: tokio::sync::Notify::new(),
            pushed_notify: tokio::sync::Notify::new(),
            activity: Mutex::new(Activity::default()),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            layouts: Mutex::new(HashMap::new()),
            prefetched_dirs: Mutex::new(HashSet::new()),
            staging: Mutex::new(HashMap::new()),
            cache_dir: cache_dir.to_path_buf(),
            state_dir: state_dir.to_path_buf(),
            empty_tree,
            notify: Mutex::new(None),
            walker,
        })
    }

    pub fn repo(&self) -> &Arc<Repo> {
        &self.repo
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// The commit this mount is based on.
    pub fn base_commit(&self) -> Id {
        self.lock().base.commit
    }

    /// The commit this mount needs kept in the bucket: its last pushed head (read-write), or
    /// the commit it shows (read-only). Mount records name it for GC.
    pub fn pushed_commit(&self) -> Id {
        let inner = self.lock();
        inner
            .base
            .branch_ref
            .as_ref()
            .filter(|_| !self.read_only)
            .map_or(inner.base.commit, |r| r.head)
    }

    /// The branch this mount writes to (it changes after an auto-fork).
    pub fn branch(&self) -> Option<BranchName> {
        self.lock().base.branch.clone()
    }

    pub fn cache_stats(&self) -> ctm_store::Result<ctm_store::cache::CacheStats> {
        self.fetcher.cache_stats()
    }

    /// Where invalidations go (the FUSE session's notifier).
    pub fn set_notifier(&self, f: Notifier) {
        *self.notify.lock().unwrap() = Some(f);
    }

    fn invalidate(&self, i: Invalidate) {
        if let Some(f) = self.notify.lock().unwrap().as_ref() {
            f(i);
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap()
    }

    /// Whether a file is unchanged, so the kernel may keep its cached pages.
    pub fn keep_cache(&self, ino: u64) -> bool {
        self.lock().inodes.get(ino).is_some_and(|n| !n.dirty)
    }

    /// Fails unless this mount can take writes right now; otherwise records the write, for
    /// auto-commits.
    fn writable(&self) -> FsResult<()> {
        if self.read_only {
            return Err(Errno::EROFS);
        }
        let now = std::time::Instant::now();
        let mut a = self.activity.lock().unwrap();
        a.last_write = Some(now);
        a.dirty_since.get_or_insert(now);
        Ok(())
    }

    /// Whether an auto-commit is due: the mount has been quiet for `quiet` since its last write,
    /// or has had uncommitted changes for `max_dirty`.
    pub fn commit_due(&self, quiet: std::time::Duration, max_dirty: std::time::Duration) -> bool {
        let a = self.activity.lock().unwrap();
        match (a.last_write, a.dirty_since) {
            (Some(last), Some(since)) => last.elapsed() >= quiet || since.elapsed() >= max_dirty,
            _ => false,
        }
    }

    // Trees and directory listings

    pub(crate) async fn tree(&self, id: Id) -> FsResult<Arc<Tree>> {
        if id == self.empty_tree {
            return Ok(Arc::new(Tree::default()));
        }
        self.fetcher.tree(&id, false).await.map_err(eio)
    }

    /// The base tree of a directory inode, and a background prefetch of its subdirectories.
    async fn base_tree(&self, ino: u64) -> FsResult<Arc<Tree>> {
        let id = {
            let inner = self.lock();
            let node = inner.inodes.get(ino).ok_or(Errno::ENOENT)?;
            match node.entry.content {
                Content::Dir(id) => id,
                _ => return Err(Errno::ENOTDIR),
            }
        };
        let tree = self.tree(id).await?;
        if id != self.empty_tree && self.prefetched_dirs.lock().unwrap().insert(id) {
            let fetcher = self.fetcher.clone();
            let children: Vec<Id> = tree
                .entries
                .iter()
                .filter_map(|e| match e.content {
                    Content::Dir(t) => Some(t),
                    _ => None,
                })
                .collect();
            tokio::spawn(async move {
                stream::iter(children)
                    .for_each_concurrent(WALK_CONCURRENCY, |t| {
                        let fetcher = fetcher.clone();
                        async move {
                            let _ = fetcher.tree(&t, true).await;
                        }
                    })
                    .await;
            });
        }
        Ok(tree)
    }

    /// A directory's entries: base entries minus whiteouts, overridden by nodes in memory.
    async fn listing(&self, dir: u64) -> FsResult<BTreeMap<Vec<u8>, Listed>> {
        let tree = self.base_tree(dir).await?;
        let inner = self.lock();
        let mut out = BTreeMap::new();
        for e in &tree.entries {
            if !inner.whiteouts.contains(&(dir, e.name.clone())) {
                out.insert(e.name.clone(), Listed::Base(e.clone()));
            }
        }
        for (name, ino) in inner.inodes.children(dir) {
            out.insert(name, Listed::Node(ino));
        }
        Ok(out)
    }

    /// The node for `name` in `parent`, bringing a base entry into memory (without counting
    /// a kernel lookup) if needed.
    async fn child(&self, parent: u64, name: &[u8]) -> FsResult<Option<u64>> {
        {
            let inner = self.lock();
            let p = inner.inodes.get(parent).ok_or(Errno::ENOENT)?;
            if p.kind() != Kind::Dir {
                return Err(Errno::ENOTDIR);
            }
            if let Some(ino) = inner.inodes.child(parent, name) {
                return Ok(Some(ino));
            }
            if inner.whiteouts.contains(&(parent, name.to_vec())) {
                return Ok(None);
            }
        }
        let tree = self.base_tree(parent).await?;
        let Ok(i) = tree
            .entries
            .binary_search_by(|e| e.name.as_slice().cmp(name))
        else {
            return Ok(None);
        };
        let mut inner = self.lock();
        if let Some(ino) = inner.inodes.child(parent, name) {
            return Ok(Some(ino));
        }
        Ok(Some(
            inner
                .inodes
                .insert(Node::new(parent, tree.entries[i].clone())),
        ))
    }

    /// Whether `parent`'s base tree has `name` (so deleting it needs a whiteout).
    async fn in_base(&self, parent: u64, name: &[u8]) -> FsResult<bool> {
        let tree = self.base_tree(parent).await?;
        Ok(tree
            .entries
            .binary_search_by(|e| e.name.as_slice().cmp(name))
            .is_ok())
    }

    pub async fn lookup(&self, parent: u64, name: &[u8]) -> FsResult<Attr> {
        {
            let mut inner = self.lock();
            let p = inner.inodes.get(parent).ok_or(Errno::ENOENT)?;
            if p.kind() != Kind::Dir {
                return Err(Errno::ENOTDIR);
            }
            if let Some(ino) = inner.inodes.child(parent, name) {
                let node = inner.inodes.get_mut(ino).expect("indexed");
                node.lookups += 1;
                return Ok(attr(ino, node));
            }
            if inner.whiteouts.contains(&(parent, name.to_vec())) {
                return Err(Errno::ENOENT);
            }
        }
        let tree = self.base_tree(parent).await?;
        let entry = tree
            .entries
            .binary_search_by(|e| e.name.as_slice().cmp(name))
            .map(|i| tree.entries[i].clone())
            .map_err(|_| Errno::ENOENT)?;
        let mut inner = self.lock();
        let ino = inner.inodes.lookup(parent, entry);
        Ok(attr(ino, inner.inodes.get(ino).expect("just looked up")))
    }

    pub fn forget(&self, ino: u64, nlookup: u64) {
        self.lock().inodes.forget(ino, nlookup);
    }

    pub async fn getattr(&self, ino: u64) -> FsResult<Attr> {
        let inner = self.lock();
        inner
            .inodes
            .get(ino)
            .map(|n| attr(ino, n))
            .ok_or(Errno::ENOENT)
    }

    pub async fn readdir(&self, ino: u64) -> FsResult<Vec<DirItem>> {
        let listing = self.listing(ino).await?;
        let inner = self.lock();
        Ok(listing
            .into_iter()
            .filter_map(|(name, l)| match l {
                Listed::Node(i) => inner.inodes.get(i).map(|n| DirItem {
                    ino: i,
                    kind: n.kind().into(),
                    name,
                }),
                Listed::Base(e) => Some(DirItem {
                    // What a lookup will return, when the entry has a file ID.
                    ino: e
                        .file_id
                        .filter(|id| inner.inodes.get(*id).is_none())
                        .unwrap_or_else(|| synthetic_ino(ino, &name)),
                    kind: e.content.kind().into(),
                    name,
                }),
            })
            .collect())
    }

    /// The parent of a directory inode (for `..`).
    pub fn parent(&self, ino: u64) -> u64 {
        self.lock().inodes.get(ino).map_or(ROOT, |n| n.parent)
    }

    // Reads

    pub async fn open_file(&self, ino: u64, write: bool) -> FsResult<Fh> {
        if write && self.read_only {
            return Err(Errno::EROFS);
        }
        {
            let mut inner = self.lock();
            let node = inner.inodes.get_mut(ino).ok_or(Errno::ENOENT)?;
            if node.kind() == Kind::Dir {
                return Err(Errno::EISDIR);
            }
            node.open += 1;
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(
            fh,
            Handle {
                ino,
                ..Handle::default()
            },
        );
        Ok(Fh(fh))
    }

    pub async fn release(&self, fh: Fh) -> FsResult<()> {
        let Some(h) = self.handles.lock().unwrap().remove(&fh.0) else {
            return Ok(());
        };
        let drop_staging = {
            let mut inner = self.lock();
            let Some(node) = inner.inodes.get_mut(h.ino) else {
                return Ok(());
            };
            node.open = node.open.saturating_sub(1);
            if node.open == 0 && node.unlinked {
                inner.inodes.remove(h.ino);
                inner.extents.remove(&h.ino);
                true
            } else {
                // A clean entry the kernel already forgot goes once it's closed.
                inner.inodes.forget(h.ino, 0);
                false
            }
        };
        if drop_staging {
            self.drop_staging(h.ino);
        }
        Ok(())
    }

    pub(crate) fn staging_file(&self, ino: u64, create: bool) -> FsResult<Option<Arc<File>>> {
        let mut files = self.staging.lock().unwrap();
        if let Some(f) = files.get(&ino) {
            return Ok(Some(f.clone()));
        }
        let path = db::staging_path(&self.state_dir, ino);
        let f = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .truncate(false)
            .open(&path)
        {
            Ok(f) => Arc::new(f),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                tracing::error!("staging file {}: {e}", path.display());
                return Err(Errno::EIO);
            }
        };
        files.insert(ino, f.clone());
        Ok(Some(f))
    }

    pub(crate) fn drop_staging(&self, ino: u64) {
        self.staging.lock().unwrap().remove(&ino);
        let path = db::staging_path(&self.state_dir, ino);
        if let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!("removing {}: {e}", path.display());
        }
    }

    pub(crate) fn view(&self, ino: u64) -> FsResult<FileView> {
        let (entry, size, base_len, base_visible, dirty, extents) = {
            let inner = self.lock();
            let n = inner.inodes.get(ino).ok_or(Errno::ENOENT)?;
            if n.kind() == Kind::Dir {
                return Err(Errno::EISDIR);
            }
            (
                n.entry.clone(),
                n.entry.size,
                n.base_len,
                n.base_visible,
                n.dirty,
                inner.extents.get(&ino).cloned().unwrap_or_default(),
            )
        };
        let staging = if extents.is_empty() {
            None
        } else {
            self.staging_file(ino, false)?
        };
        Ok(FileView {
            entry,
            size,
            base_len,
            base_visible,
            dirty,
            extents,
            staging,
        })
    }

    pub async fn read(&self, fh: Fh, ino: u64, offset: u64, len: u32) -> FsResult<Vec<u8>> {
        let view = self.view(ino)?;
        if view.dirty {
            return self.read_view(&view, offset, u64::from(len)).await;
        }
        let end = (offset + u64::from(len)).min(view.size);
        if offset >= end {
            return Ok(Vec::new());
        }
        if let Content::Inline(b) = &view.entry.content {
            return Ok(b[offset as usize..end as usize].to_vec());
        }
        let sequential = self.access(fh, offset, end);
        let (bytes, layout, last) = self
            .read_chunks(&view.entry.content, view.base_len, offset, end, sequential)
            .await?;
        if sequential {
            self.readahead(fh, &layout, last);
        }
        Ok(bytes)
    }

    /// Bytes `[offset, offset + len)` of a file as of `view`: dirty extents from staging,
    /// base bytes below `base_visible`, zeros elsewhere.
    pub(crate) async fn read_view(
        &self,
        view: &FileView,
        offset: u64,
        len: u64,
    ) -> FsResult<Vec<u8>> {
        let end = (offset + len).min(view.size);
        if offset >= end {
            return Ok(Vec::new());
        }
        let mut out = vec![0; (end - offset) as usize];
        // Base bytes, only where no extent covers them.
        let base_end = end.min(view.base_visible);
        let mut at = offset;
        for e in view
            .extents
            .iter()
            .chain(std::iter::once(&(u64::MAX..u64::MAX)))
        {
            if at >= base_end {
                break;
            }
            let gap_end = e.start.min(base_end);
            if at < gap_end {
                let bytes = self
                    .read_base(&view.entry.content, view.base_len, at, gap_end)
                    .await?;
                out[(at - offset) as usize..(gap_end - offset) as usize].copy_from_slice(&bytes);
            }
            at = at.max(e.end);
        }
        for e in &view.extents {
            let (s, t) = (e.start.max(offset), e.end.min(end));
            if s < t {
                let f = view.staging.as_ref().ok_or(Errno::EIO)?;
                f.read_exact_at(&mut out[(s - offset) as usize..(t - offset) as usize], s)
                    .map_err(|err| {
                        tracing::error!("reading staging: {err}");
                        Errno::EIO
                    })?;
            }
        }
        Ok(out)
    }

    /// Bytes `[offset, end)` of base content `base_len` bytes long.
    async fn read_base(
        &self,
        content: &Content,
        base_len: u64,
        offset: u64,
        end: u64,
    ) -> FsResult<Vec<u8>> {
        match content {
            Content::Inline(b) => Ok(b[offset as usize..end as usize].to_vec()),
            Content::Chunk(_) | Content::ChunkList(_) => Ok(self
                .read_chunks(content, base_len, offset, end, true)
                .await?
                .0),
            _ => Err(Errno::EISDIR),
        }
    }

    /// The chunk layout of chunked base content `base_len` bytes long.
    pub(crate) async fn layout(&self, content: &Content, base_len: u64) -> FsResult<Arc<Layout>> {
        let key = match *content {
            Content::Chunk(id) | Content::ChunkList(id) => id,
            _ => return Err(Errno::EINVAL),
        };
        if let Some(l) = self.layouts.lock().unwrap().get(&key) {
            return Ok(l.clone());
        }
        let layout = match *content {
            Content::Chunk(id) => Layout {
                pages: vec![PageSpan {
                    start: 0,
                    source: PageSource::Single(ChunkRef {
                        id,
                        // A single-chunk file's chunk is the whole base file.
                        len: base_len as u32,
                    }),
                    chunks: OnceCell::new(),
                }],
            },
            _ => {
                let list = self
                    .fetcher
                    .meta::<ChunkList>(&key, false)
                    .await
                    .map_err(eio)?;
                let mut start = 0;
                let pages = list
                    .pages
                    .iter()
                    .map(|p| {
                        let span = PageSpan {
                            start,
                            source: PageSource::Page(p.id),
                            chunks: OnceCell::new(),
                        };
                        start += p.total_len;
                        span
                    })
                    .collect();
                Layout { pages }
            }
        };
        let layout = Arc::new(layout);
        self.layouts.lock().unwrap().insert(key, layout.clone());
        Ok(layout)
    }

    async fn page(&self, layout: &Layout, page: usize) -> FsResult<PageChunks> {
        let span = &layout.pages[page];
        span.chunks
            .get_or_try_init(|| async {
                let chunks = match span.source {
                    PageSource::Single(c) => vec![c],
                    PageSource::Page(id) => {
                        self.fetcher
                            .meta::<ChunkPage>(&id, false)
                            .await
                            .map_err(eio)?
                            .chunks
                    }
                };
                let mut offset = span.start;
                Ok(Arc::new(
                    chunks
                        .into_iter()
                        .map(|c| {
                            let at = offset;
                            offset += u64::from(c.len);
                            (at, c)
                        })
                        .collect(),
                ))
            })
            .await
            .cloned()
    }

    /// Every chunk of chunked base content, in order.
    pub(crate) async fn all_chunks(
        &self,
        content: &Content,
        base_len: u64,
    ) -> FsResult<Vec<ChunkRef>> {
        let layout = self.layout(content, base_len).await?;
        let mut out = Vec::new();
        for p in 0..layout.pages.len() {
            out.extend(self.page(&layout, p).await?.iter().map(|(_, c)| *c));
        }
        Ok(out)
    }

    /// The position of the chunk holding byte `offset` (which must be inside the file).
    async fn locate(&self, layout: &Layout, offset: u64) -> FsResult<Pos> {
        let page = layout.pages.partition_point(|p| p.start <= offset) - 1;
        let chunks = self.page(layout, page).await?;
        let index = chunks.partition_point(|(at, _)| *at <= offset) - 1;
        Ok(Pos { page, index })
    }

    /// The chunk after `pos`, or `None` at the end of the file.
    async fn next(&self, layout: &Layout, pos: Pos) -> FsResult<Option<Pos>> {
        let len = self.page(layout, pos.page).await?.len();
        Ok(if pos.index + 1 < len {
            Some(Pos {
                page: pos.page,
                index: pos.index + 1,
            })
        } else if pos.page + 1 < layout.pages.len() {
            Some(Pos {
                page: pos.page + 1,
                index: 0,
            })
        } else {
            None
        })
    }

    /// Bytes `[offset, end)` of a chunked file, and the position of the last chunk read. `whole`
    /// fetches whole chunks on a miss, rather than just the 64 KiB blocks read.
    async fn read_chunks(
        &self,
        content: &Content,
        base_len: u64,
        offset: u64,
        end: u64,
        whole: bool,
    ) -> FsResult<(Vec<u8>, Arc<Layout>, Pos)> {
        let layout = self.layout(content, base_len).await?;
        let mut out = vec![0; (end - offset) as usize];
        let mut pos = self.locate(&layout, offset).await?;
        let mut at = offset;
        let last = loop {
            let (start, chunk) = self.page(&layout, pos.page).await?[pos.index];
            let take_end = end.min(start + u64::from(chunk.len));
            let range = (at - start) as u32..(take_end - start) as u32;
            let bytes = self.fetcher.read(chunk, range, whole).await.map_err(eio)?;
            out[(at - offset) as usize..(take_end - offset) as usize].copy_from_slice(&bytes);
            at = take_end;
            if at >= end {
                break pos;
            }
            pos = self.next(&layout, pos).await?.ok_or(Errno::EIO)?;
        };
        Ok((out, layout, last))
    }

    /// Starts downloading the base chunk holding byte `offset`, if it isn't cached.
    async fn prefetch_base(&self, content: &Content, base_len: u64, offset: u64) {
        let Ok(layout) = self.layout(content, base_len).await else {
            return;
        };
        if let Ok(pos) = self.locate(&layout, offset).await
            && let Ok(chunks) = self.page(&layout, pos.page).await
        {
            self.fetcher.prefetch(vec![chunks[pos.index].1]);
        }
    }

    /// Records a read on a handle, and says whether the handle is reading sequentially: two
    /// reads after the first that each start near where the handle's reads have got to. A read
    /// anywhere else starts over.
    fn access(&self, fh: Fh, offset: u64, end: u64) -> bool {
        let mut handles = self.handles.lock().unwrap();
        let Some(h) = handles.get_mut(&fh.0) else {
            return false;
        };
        if offset.abs_diff(h.next_offset) <= SEQUENTIAL_SLACK {
            h.sequential += 1;
            h.next_offset = h.next_offset.max(end);
        } else {
            *h = Handle {
                ino: h.ino,
                next_offset: end,
                ..Handle::default()
            };
        }
        h.sequential >= 2
    }

    /// Grows a sequential handle's readahead window and starts downloading the chunks in it
    /// that aren't coming yet, in batches of at most one pack span each.
    ///
    /// The window starts at 2 MiB past the chunk being read and doubles each time the reader
    /// moves on to a new chunk, up to 128 MiB. Only chunks of pages already loaded are fetched;
    /// the next page is loaded by the read that reaches it.
    fn readahead(&self, fh: Fh, layout: &Layout, last: Pos) {
        let batches = {
            let mut handles = self.handles.lock().unwrap();
            let Some(h) = handles.get_mut(&fh.0) else {
                return;
            };
            let chunk = last.global();
            if h.window == 0 {
                h.window = READAHEAD_START;
            } else if h.last_chunk.is_some_and(|c| chunk > c) {
                h.window = (h.window * 2).min(READAHEAD_MAX);
            }
            h.last_chunk = Some(h.last_chunk.map_or(chunk, |c| c.max(chunk)));
            let from = h.prefetched_to.map_or(chunk, |p| p.max(chunk)) + 1;
            // Chunks in the window not handed out yet, and whether the window reaches past
            // the chunks known (end of file, or a page not loaded yet).
            let mut todo: Vec<(u64, ChunkRef)> = Vec::new();
            let (mut ahead, mut g, mut ended) = (0u64, chunk + 1, false);
            while ahead < h.window {
                let (page, index) = (g as usize / PAGE_MAX, g as usize % PAGE_MAX);
                let Some(c) = layout
                    .pages
                    .get(page)
                    .and_then(|p| p.chunks.get())
                    .and_then(|chunks| chunks.get(index))
                    .map(|(_, c)| *c)
                else {
                    ended = true;
                    break;
                };
                ahead += u64::from(c.len);
                if g >= from {
                    todo.push((g, c));
                }
                g += 1;
            }
            // Whole spans only, so each GET covers as much of a pack as it can. A partial span
            // waits for the window to grow, unless nothing is on its way yet (the start of a
            // stream) or it's all there is.
            let idle = h.prefetched_to.is_none_or(|p| p <= chunk);
            let mut batches: Vec<Vec<ChunkRef>> = Vec::new();
            let mut batch_bytes = 0u64;
            for (_, c) in &todo {
                if batches.is_empty() || batch_bytes + u64::from(c.len) > READAHEAD_SPAN {
                    batches.push(Vec::new());
                    batch_bytes = 0;
                }
                batches.last_mut().expect("just pushed").push(*c);
                batch_bytes += u64::from(c.len);
            }
            if !(idle || ended) && batch_bytes < READAHEAD_SPAN {
                batches.pop();
            }
            let handed: usize = batches.iter().map(Vec::len).sum();
            if handed > 0 {
                h.prefetched_to = Some(todo[handed - 1].0);
            }
            batches
        };
        for batch in batches {
            self.fetcher.prefetch(batch);
        }
    }

    pub async fn readlink(&self, ino: u64) -> FsResult<Vec<u8>> {
        let inner = self.lock();
        match &inner.inodes.get(ino).ok_or(Errno::ENOENT)?.entry.content {
            Content::Symlink(t) => Ok(t.clone()),
            _ => Err(Errno::EINVAL),
        }
    }

    pub fn statfs(&self) -> FsResult<StatFs> {
        let s = rustix::fs::statvfs(&self.cache_dir).map_err(|e| Errno(e.raw_os_error()))?;
        Ok(StatFs {
            block_size: s.f_frsize,
            blocks: s.f_blocks,
            blocks_free: s.f_bfree,
            blocks_avail: s.f_bavail,
        })
    }

    // Writes

    pub async fn write(&self, fh: Fh, ino: u64, offset: u64, data: &[u8]) -> FsResult<u32> {
        let _ = fh;
        let _gate = self.gate.read().await;
        self.writable()?;
        if self.lock().inodes.get(ino).ok_or(Errno::ENOENT)?.kind() != Kind::File {
            return Err(Errno::EISDIR);
        }
        let staging = self.staging_file(ino, true)?.expect("created");
        staging.write_all_at(data, offset).map_err(|e| {
            tracing::error!("writing staging: {e}");
            Errno::EIO
        })?;
        let end = offset + data.len() as u64;
        let (content, base_len, base_visible) = {
            let mut inner = self.lock();
            merge_extent(inner.extents.entry(ino).or_default(), offset..end);
            let node = inner.inodes.get_mut(ino).ok_or(Errno::ENOENT)?;
            node.dirty = true;
            node.entry.size = node.entry.size.max(end);
            node.entry.mtime_ns = now_ns();
            let snapshot = (node.entry.content.clone(), node.base_len, node.base_visible);
            inner.touch(ino).map_err(fail)?;
            snapshot
        };
        // Commit re-chunks from the base chunk holding the byte before the write and resyncs
        // at the chunk holding its end; fetch those now so commit rarely waits.
        if matches!(content, Content::Chunk(_) | Content::ChunkList(_)) {
            if offset > 0 && offset - 1 < base_visible {
                self.prefetch_base(&content, base_len, offset - 1).await;
            }
            if end < base_visible {
                self.prefetch_base(&content, base_len, end).await;
            }
        }
        Ok(data.len() as u32)
    }

    pub async fn setattr(&self, ino: u64, set: SetAttr) -> FsResult<Attr> {
        let _gate = self.gate.read().await;
        self.writable()?;
        if let Some(n) = set.size {
            let shrinks = {
                let inner = self.lock();
                let node = inner.inodes.get(ino).ok_or(Errno::ENOENT)?;
                match node.kind() {
                    Kind::Dir => return Err(Errno::EISDIR),
                    Kind::Symlink => return Err(Errno::EINVAL),
                    Kind::File => n < node.entry.size,
                }
            };
            if shrinks && let Some(f) = self.staging_file(ino, false)? {
                f.set_len(n).map_err(|e| {
                    tracing::error!("truncating staging: {e}");
                    Errno::EIO
                })?;
            }
        }
        let mut inner = self.lock();
        let node = inner.inodes.get_mut(ino).ok_or(Errno::ENOENT)?;
        if let Some(n) = set.size {
            node.base_visible = node.base_visible.min(n);
            node.entry.size = n;
            node.dirty = true;
            node.entry.mtime_ns = now_ns();
        }
        if let Some(mode) = set.mode
            && node.kind() != Kind::Symlink
        {
            node.entry.mode = mode & 0o7777;
        }
        if let Some(t) = set.mtime_ns {
            node.entry.mtime_ns = t;
        }
        if let Some(n) = set.size
            && let Some(ext) = inner.extents.get_mut(&ino)
        {
            ext.retain_mut(|e| {
                e.end = e.end.min(n);
                e.start < e.end
            });
        }
        inner.touch(ino).map_err(fail)?;
        Ok(attr(ino, inner.inodes.get(ino).expect("live")))
    }

    /// Adds a new entry to `parent` and returns its inode (with one kernel lookup counted).
    async fn add(&self, parent: u64, entry: DirEntry, dirty: bool) -> FsResult<u64> {
        check_name(&entry.name)?;
        if self.child(parent, &entry.name).await?.is_some() {
            return Err(Errno::EEXIST);
        }
        let mut inner = self.lock();
        let mut node = Node::new(parent, entry);
        node.lookups = 1;
        if node.kind() == Kind::File {
            node.base_len = 0;
            node.base_visible = 0;
        }
        node.dirty = dirty;
        let ino = inner.inodes.insert(node);
        inner.inodes.get_mut(parent).expect("parent").entry.mtime_ns = now_ns();
        inner.touch(ino).map_err(fail)?;
        inner.touch(parent).map_err(fail)?;
        Ok(ino)
    }

    fn new_entry(name: &[u8], mode: u16, size: u64, content: Content) -> DirEntry {
        DirEntry {
            name: name.to_vec(),
            mode,
            mtime_ns: now_ns(),
            size,
            content,
            btime_ns: None,
            xattrs: None,
            file_id: Some(ctm_repo::new_file_id()),
        }
    }

    pub async fn create(&self, parent: u64, name: &[u8], mode: u16) -> FsResult<(Attr, Fh)> {
        let _gate = self.gate.read().await;
        self.writable()?;
        let entry = Self::new_entry(name, mode & 0o7777, 0, Content::Inline(Vec::new()));
        let ino = self.add(parent, entry, true).await?;
        self.staging_file(ino, true)?;
        let fh = self.open_file(ino, true).await?;
        Ok((self.getattr(ino).await?, fh))
    }

    pub async fn mkdir(&self, parent: u64, name: &[u8], mode: u16) -> FsResult<Attr> {
        let _gate = self.gate.read().await;
        self.writable()?;
        let entry = Self::new_entry(name, mode & 0o7777, 0, Content::Dir(self.empty_tree));
        let ino = self.add(parent, entry, false).await?;
        self.getattr(ino).await
    }

    pub async fn symlink(&self, parent: u64, name: &[u8], target: &[u8]) -> FsResult<Attr> {
        let _gate = self.gate.read().await;
        self.writable()?;
        if target.is_empty() || target.len() > 4095 || target.contains(&0) {
            return Err(Errno::EINVAL);
        }
        let entry = Self::new_entry(
            name,
            0o777,
            target.len() as u64,
            Content::Symlink(target.to_vec()),
        );
        let ino = self.add(parent, entry, false).await?;
        self.getattr(ino).await
    }

    /// Removes `ino` (named `name` in `parent`) from the tree. Open files live on until
    /// their last release.
    fn remove_entry(
        &self,
        inner: &mut Inner,
        parent: u64,
        name: &[u8],
        ino: u64,
        in_base: bool,
    ) -> FsResult<()> {
        inner.inodes.detach(ino);
        if in_base {
            inner.whiteouts.insert((parent, name.to_vec()));
        }
        let open = inner.inodes.get(ino).is_some_and(|n| n.open > 0);
        if inner.inodes.get(ino).is_some_and(|n| n.kind() == Kind::Dir) {
            inner.whiteouts.retain(|(p, _)| *p != ino);
        }
        if open {
            let node = inner.inodes.get_mut(ino).expect("live");
            node.unlinked = true;
            node.changed = false;
        } else {
            inner.inodes.remove(ino);
            inner.extents.remove(&ino);
        }
        inner.inodes.get_mut(parent).expect("parent").entry.mtime_ns = now_ns();
        inner
            .db()
            .tx(|t| {
                db::delete_node(t, ino)?;
                if in_base {
                    db::add_whiteout(t, parent, name)?;
                }
                Ok(())
            })
            .map_err(fail)?;
        inner.touch(parent).map_err(fail)?;
        if !open {
            self.drop_staging(ino);
        }
        Ok(())
    }

    pub async fn unlink(&self, parent: u64, name: &[u8]) -> FsResult<()> {
        let _gate = self.gate.read().await;
        self.writable()?;
        let ino = self.child(parent, name).await?.ok_or(Errno::ENOENT)?;
        let in_base = self.in_base(parent, name).await?;
        let mut inner = self.lock();
        if inner.inodes.get(ino).ok_or(Errno::ENOENT)?.kind() == Kind::Dir {
            return Err(Errno::EISDIR);
        }
        self.remove_entry(&mut inner, parent, name, ino, in_base)
    }

    pub async fn rmdir(&self, parent: u64, name: &[u8]) -> FsResult<()> {
        let _gate = self.gate.read().await;
        self.writable()?;
        let ino = self.child(parent, name).await?.ok_or(Errno::ENOENT)?;
        if self.lock().inodes.get(ino).ok_or(Errno::ENOENT)?.kind() != Kind::Dir {
            return Err(Errno::ENOTDIR);
        }
        if !self.listing(ino).await?.is_empty() {
            return Err(Errno::ENOTEMPTY);
        }
        let in_base = self.in_base(parent, name).await?;
        let mut inner = self.lock();
        self.remove_entry(&mut inner, parent, name, ino, in_base)
    }

    /// `no_replace` is `RENAME_NOREPLACE`.
    pub async fn rename(
        &self,
        parent: u64,
        name: &[u8],
        new_parent: u64,
        new_name: &[u8],
        no_replace: bool,
    ) -> FsResult<()> {
        let _gate = self.gate.read().await;
        self.writable()?;
        check_name(new_name)?;
        let src = self.child(parent, name).await?.ok_or(Errno::ENOENT)?;
        let dst = self.child(new_parent, new_name).await?;
        if dst == Some(src) {
            return Ok(());
        }
        let kind = |ino: u64| -> FsResult<Kind> {
            Ok(self.lock().inodes.get(ino).ok_or(Errno::ENOENT)?.kind())
        };
        let src_dir = kind(src)? == Kind::Dir;
        if let Some(dst) = dst {
            if no_replace {
                return Err(Errno::EEXIST);
            }
            match (src_dir, kind(dst)? == Kind::Dir) {
                (true, false) => return Err(Errno::ENOTDIR),
                (false, true) => return Err(Errno::EISDIR),
                (true, true) if !self.listing(dst).await?.is_empty() => {
                    return Err(Errno::ENOTEMPTY);
                }
                _ => {}
            }
        }
        let src_in_base = self.in_base(parent, name).await?;
        let dst_in_base = self.in_base(new_parent, new_name).await?;
        let mut inner = self.lock();
        if let Some(dst) = dst {
            self.remove_entry(&mut inner, new_parent, new_name, dst, dst_in_base)?;
        }
        inner.inodes.detach(src);
        if src_in_base {
            inner.whiteouts.insert((parent, name.to_vec()));
            inner
                .db()
                .tx(|t| db::add_whiteout(t, parent, name))
                .map_err(fail)?;
        }
        inner.inodes.attach(src, new_parent, new_name.to_vec());
        let now = now_ns();
        inner.inodes.get_mut(parent).expect("parent").entry.mtime_ns = now;
        inner
            .inodes
            .get_mut(new_parent)
            .expect("parent")
            .entry
            .mtime_ns = now;
        inner.touch(src).map_err(fail)?;
        inner.touch(parent).map_err(fail)?;
        inner.touch(new_parent).map_err(fail)?;
        Ok(())
    }

    /// Makes the file's written bytes and extent map durable on this machine.
    pub async fn fsync(&self, ino: u64) -> FsResult<()> {
        if self.read_only {
            return Ok(());
        }
        if let Some(f) = self.staging_file(ino, false)? {
            f.sync_data().map_err(|e| {
                tracing::error!("syncing staging: {e}");
                Errno::EIO
            })?;
        }
        self.lock().db().sync().map_err(fail)
    }

    pub async fn status(&self) -> Result<Status> {
        let (base, dirty) = {
            let inner = self.lock();
            let changed = inner
                .inodes
                .iter()
                .filter(|(_, n)| n.changed && !n.unlinked && n.kind() != Kind::Dir)
                .count();
            (inner.base.clone(), (changed + inner.whiteouts.len()) as u64)
        };
        let (unpushed, oldest_unpushed_ns) = self.unpushed()?;
        let behind = match (&base.branch, &base.etag) {
            (Some(b), Some(etag)) => {
                self.repo.backend().head(&b.branch_key()).await?.as_ref() != Some(etag)
            }
            _ => false,
        };
        Ok(Status {
            branch: base.branch,
            base_commit: base.commit,
            dirty_files: dirty,
            unpushed: unpushed as u64,
            oldest_unpushed_ns,
            behind,
            auto_forked_from: base.auto_forked_from,
            read_only: self.read_only,
        })
    }

    /// Replaces the entry at `path` with the one in `from`, and invalidates the kernel's
    /// cache of it.
    pub async fn restore(&self, path: &[u8], from: &RefSpec) -> Result<()> {
        let _gate = self.gate.read().await;
        self.writable().map_err(io_errno)?;
        let resolved = self.repo.resolve(from).await?;
        let mut entry = self
            .repo
            .entry_at(resolved.root_tree, Some(path))
            .await?
            .ok_or_else(|| ctm_repo::Error::PathNotFound(String::from_utf8_lossy(path).into()))?;
        let (dir, name) = match path.iter().rposition(|&b| b == b'/') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (&b""[..], path),
        };
        let mut parent = ROOT;
        for part in dir.split(|&b| b == b'/').filter(|p| !p.is_empty()) {
            parent = self
                .child(parent, part)
                .await
                .map_err(io_errno)?
                .ok_or_else(|| {
                    ctm_repo::Error::PathNotFound(String::from_utf8_lossy(dir).into())
                })?;
        }
        let existing = self.child(parent, name).await.map_err(io_errno)?;
        entry.name = name.to_vec();
        let (ino, dropped) = {
            let mut inner = self.lock();
            let mut dropped = Vec::new();
            let ino = match existing {
                Some(ino) => {
                    // Everything below a restored directory comes from the ref now.
                    let mut stack = vec![ino];
                    while let Some(d) = stack.pop() {
                        for (_, c) in inner.inodes.children(d) {
                            stack.push(c);
                            dropped.push(c);
                        }
                        inner.whiteouts.retain(|(p, _)| *p != d);
                    }
                    for c in &dropped {
                        if inner.inodes.get(*c).is_some_and(|n| n.open > 0) {
                            inner.inodes.detach(*c);
                            let n = inner.inodes.get_mut(*c).expect("live");
                            n.unlinked = true;
                            n.changed = false;
                        } else {
                            inner.inodes.remove(*c);
                            inner.extents.remove(c);
                        }
                    }
                    inner.extents.remove(&ino);
                    dropped.push(ino);
                    let node = inner.inodes.get_mut(ino).expect("live");
                    node.base_len = entry.size;
                    node.base_visible = entry.size;
                    node.dirty = false;
                    node.entry = entry;
                    ino
                }
                None => inner.inodes.insert(Node::new(parent, entry)),
            };
            inner.db().tx(|t| {
                for d in &dropped {
                    db::delete_node(t, *d)?;
                }
                Ok(())
            })?;
            inner.touch(ino)?;
            (ino, dropped)
        };
        for d in dropped {
            self.drop_staging(d);
        }
        self.invalidate(Invalidate::Inode(ino));
        self.invalidate(Invalidate::Entry {
            parent,
            name: name.to_vec(),
        });
        Ok(())
    }
}

/// Fetches every tree of the mounted commit in the background, breadth-first, so `find` and
/// `stat` rarely wait on the network.
async fn walk_metadata(fetcher: Arc<Fetcher>, root: Id, empty: Id) {
    let mut level = vec![root];
    while !level.is_empty() {
        let trees: Vec<Arc<Tree>> = stream::iter(level)
            .filter(|id| std::future::ready(*id != empty))
            .map(|id| {
                let fetcher = fetcher.clone();
                async move { fetcher.tree(&id, true).await }
            })
            .buffer_unordered(WALK_CONCURRENCY)
            .filter_map(|t| async move { t.ok() })
            .collect()
            .await;
        level = trees
            .iter()
            .flat_map(|t| t.entries.iter())
            .filter_map(|e| match e.content {
                Content::Dir(id) => Some(id),
                _ => None,
            })
            .collect();
    }
}

#[cfg(test)]
mod tests {
    use super::merge_extent;

    #[test]
    fn extents_merge_where_they_touch() {
        let mut e = Vec::new();
        merge_extent(&mut e, 10..20);
        merge_extent(&mut e, 30..40);
        assert_eq!(e, [10..20, 30..40]);
        merge_extent(&mut e, 20..25);
        assert_eq!(e, [10..25, 30..40]);
        merge_extent(&mut e, 5..35);
        assert_eq!((e.len(), e[0].clone()), (1, 5..40));
        merge_extent(&mut e, 0..1);
        assert_eq!(e, [0..1, 5..40]);
    }
}
