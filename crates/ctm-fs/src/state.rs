use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::{StreamExt, stream};
use tokio::sync::OnceCell;
use tokio::task::JoinHandle;

use ctm_core::layout::PAGE_MAX;
use ctm_core::{ChunkList, ChunkPage, ChunkRef, Commit, Content, DirEntry, Id, Kind, Tree};
use ctm_repo::{BranchName, RefSpec, Repo};

use crate::fetch::{FetchError, Fetcher};
use crate::inode::{Inodes, ROOT};
use crate::{Errno, Error, FsResult, Result};

/// Readahead window, in chunks: where it starts once a handle reads sequentially, and its cap.
const READAHEAD_START: u64 = 4;
const READAHEAD_MAX: u64 = 16;
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

/// A page's chunks with their offsets in the file, sorted.
type PageChunks = Arc<Vec<(u64, ChunkRef)>>;

/// Where a file's chunks are: one page per 4096 chunks, each loaded on first use.
struct Layout {
    pages: Vec<PageSpan>,
}

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
    next_offset: u64,
    sequential: u32,
    /// Readahead window in chunks; 0 until the handle reads sequentially.
    window: u64,
    last_chunk: Option<u64>,
    /// Chunks up to this global index have been handed to readahead.
    prefetched_to: Option<u64>,
}

/// All filesystem logic for one mounted ref.
pub struct MountState {
    repo: Arc<Repo>,
    fetcher: Arc<Fetcher>,
    inodes: Mutex<Inodes>,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
    layouts: Mutex<HashMap<Id, Arc<Layout>>>,
    prefetched_dirs: Mutex<HashSet<Id>>,
    read_only: bool,
    cache_dir: PathBuf,
    commit: Id,
    walker: JoinHandle<()>,
}

impl Drop for MountState {
    fn drop(&mut self) {
        self.walker.abort();
    }
}

fn eio(e: FetchError) -> Errno {
    tracing::error!("{e}");
    Errno::EIO
}

fn attr(ino: u64, e: &DirEntry) -> Attr {
    Attr {
        ino,
        kind: e.content.kind().into(),
        size: e.size,
        mode: e.mode,
        mtime_ns: e.mtime_ns,
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

impl MountState {
    /// Opens the working state in `state_dir` for `spec`. Snapshots and commit IDs always
    /// mount read-only. `cache_dir` is the repo's shared cache directory.
    pub async fn open(
        repo: Arc<Repo>,
        cache_dir: &Path,
        state_dir: &Path,
        spec: &RefSpec,
        opts: MountOptions,
    ) -> Result<MountState> {
        let _ = state_dir; // Working state arrives with writes (M3).
        if spec.path.is_some() {
            return Err(Error::PathInRef);
        }
        let resolved = repo.resolve(spec).await?;
        let fetcher = Arc::new(Fetcher::new(
            repo.clone(),
            cache_dir,
            opts.chunk_cache_max,
            opts.max_concurrency,
        )?);
        let commit = fetcher
            .meta::<Commit>(&resolved.commit, false)
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?;
        let root = DirEntry {
            name: Vec::new(),
            mode: 0o755,
            mtime_ns: commit.time_ns,
            size: 0,
            content: Content::Dir(resolved.root_tree),
            btime_ns: None,
            xattrs: None,
        };
        let walker = tokio::spawn(walk_metadata(fetcher.clone(), resolved.root_tree));
        Ok(MountState {
            read_only: opts.read_only || resolved.branch.is_none(),
            repo,
            fetcher,
            inodes: Mutex::new(Inodes::new(root)),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            layouts: Mutex::new(HashMap::new()),
            prefetched_dirs: Mutex::new(HashSet::new()),
            cache_dir: cache_dir.to_path_buf(),
            commit: resolved.commit,
            walker,
        })
    }

    pub fn repo(&self) -> &Arc<Repo> {
        &self.repo
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// The commit this mount shows.
    pub fn base_commit(&self) -> Id {
        self.commit
    }

    pub fn cache_stats(&self) -> ctm_store::Result<ctm_store::cache::CacheStats> {
        self.fetcher.cache_stats()
    }

    fn entry(&self, ino: u64) -> FsResult<DirEntry> {
        let inodes = self.inodes.lock().unwrap();
        inodes
            .get(ino)
            .map(|n| n.entry.clone())
            .ok_or(Errno::ENOENT)
    }

    /// The tree of a directory inode, and a background prefetch of its subdirectories.
    async fn dir_tree(&self, ino: u64) -> FsResult<Arc<Tree>> {
        let Content::Dir(id) = self.entry(ino)?.content else {
            return Err(Errno::ENOTDIR);
        };
        let tree = self.fetcher.tree(&id, false).await.map_err(eio)?;
        if self.prefetched_dirs.lock().unwrap().insert(id) {
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

    pub async fn lookup(&self, parent: u64, name: &[u8]) -> FsResult<Attr> {
        let tree = self.dir_tree(parent).await?;
        let entry = tree
            .entries
            .binary_search_by(|e| e.name.as_slice().cmp(name))
            .map(|i| tree.entries[i].clone())
            .map_err(|_| Errno::ENOENT)?;
        let ino = self.inodes.lock().unwrap().lookup(parent, entry.clone());
        Ok(attr(ino, &entry))
    }

    pub fn forget(&self, ino: u64, nlookup: u64) {
        self.inodes.lock().unwrap().forget(ino, nlookup);
    }

    pub async fn getattr(&self, ino: u64) -> FsResult<Attr> {
        Ok(attr(ino, &self.entry(ino)?))
    }

    pub async fn setattr(&self, ino: u64, set: SetAttr) -> FsResult<Attr> {
        let _ = (ino, set);
        todo!("M3: setattr")
    }

    pub async fn readdir(&self, ino: u64) -> FsResult<Vec<DirItem>> {
        let tree = self.dir_tree(ino).await?;
        let inodes = self.inodes.lock().unwrap();
        Ok(tree
            .entries
            .iter()
            .map(|e| DirItem {
                ino: inodes
                    .peek(ino, &e.name)
                    .unwrap_or_else(|| synthetic_ino(ino, &e.name)),
                name: e.name.clone(),
                kind: e.content.kind().into(),
            })
            .collect())
    }

    /// The parent of a directory inode (for `..`).
    pub fn parent(&self, ino: u64) -> u64 {
        let inodes = self.inodes.lock().unwrap();
        inodes.get(ino).map_or(ROOT, |n| n.parent)
    }

    pub async fn open_file(&self, ino: u64, write: bool) -> FsResult<Fh> {
        let entry = self.entry(ino)?;
        if entry.content.kind() == Kind::Dir {
            return Err(Errno::EISDIR);
        }
        if write && self.read_only {
            return Err(Errno::EROFS);
        }
        if write {
            todo!("M3: open for writing");
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.handles.lock().unwrap().insert(fh, Handle::default());
        Ok(Fh(fh))
    }

    async fn layout(&self, e: &DirEntry) -> FsResult<Arc<Layout>> {
        let key = match e.content {
            Content::Chunk(id) | Content::ChunkList(id) => id,
            _ => return Err(Errno::EINVAL),
        };
        if let Some(l) = self.layouts.lock().unwrap().get(&key) {
            return Ok(l.clone());
        }
        let layout = match e.content {
            Content::Chunk(id) => Layout {
                pages: vec![PageSpan {
                    start: 0,
                    source: PageSource::Single(ChunkRef {
                        id,
                        len: e.size as u32,
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

    pub async fn read(&self, fh: Fh, ino: u64, offset: u64, len: u32) -> FsResult<Vec<u8>> {
        let e = self.entry(ino)?;
        let end = (offset + u64::from(len)).min(e.size);
        if offset >= end {
            return Ok(Vec::new());
        }
        match &e.content {
            Content::Inline(b) => return Ok(b[offset as usize..end as usize].to_vec()),
            Content::Chunk(_) | Content::ChunkList(_) => {}
            _ => return Err(Errno::EISDIR),
        }
        let layout = self.layout(&e).await?;
        let mut out = vec![0; (end - offset) as usize];
        let mut pos = self.locate(&layout, offset).await?;
        let mut at = offset;
        let last = loop {
            let (start, chunk) = self.page(&layout, pos.page).await?[pos.index];
            let take_end = end.min(start + u64::from(chunk.len));
            let bytes = self.fetcher.chunk(chunk, false).await.map_err(eio)?;
            bytes
                .read_at(
                    &mut out[(at - offset) as usize..(take_end - offset) as usize],
                    at - start,
                )
                .map_err(|e| {
                    tracing::error!("reading cached chunk {}: {e}", chunk.id);
                    Errno::EIO
                })?;
            at = take_end;
            if at >= end {
                break pos;
            }
            pos = self.next(&layout, pos).await?.ok_or(Errno::EIO)?;
        };
        self.readahead(fh, layout, offset, end, last);
        Ok(out)
    }

    /// Updates the handle's sequential-read state and starts downloading the chunks ahead.
    ///
    /// Two reads that each continue where the last ended mark the handle sequential. The
    /// window starts at 4 chunks and doubles each time the reader moves on to a new chunk,
    /// up to 16. A read anywhere else resets it.
    fn readahead(&self, fh: Fh, layout: Arc<Layout>, offset: u64, end: u64, last: Pos) {
        let (from, to) = {
            let mut handles = self.handles.lock().unwrap();
            let Some(h) = handles.get_mut(&fh.0) else {
                return;
            };
            if offset == h.next_offset {
                h.sequential += 1;
            } else {
                *h = Handle::default();
            }
            h.next_offset = end;
            let chunk = last.global();
            if h.sequential >= 2 {
                if h.window == 0 {
                    h.window = READAHEAD_START;
                } else if h.last_chunk.is_some_and(|c| chunk > c) {
                    h.window = (h.window * 2).min(READAHEAD_MAX);
                }
            }
            h.last_chunk = Some(chunk);
            if h.window == 0 {
                return;
            }
            let to = chunk + h.window;
            let from = h.prefetched_to.map_or(chunk, |p| p.max(chunk)) + 1;
            if from > to {
                return;
            }
            h.prefetched_to = Some(to);
            (from, to)
        };
        let fetcher = self.fetcher.clone();
        let this_page = move |g: u64| Pos {
            page: g as usize / PAGE_MAX,
            index: g as usize % PAGE_MAX,
        };
        // Resolving later pages may itself need a download, so it all runs in the background.
        let pages: Vec<(usize, PageChunks)> = layout
            .pages
            .iter()
            .enumerate()
            .filter_map(|(i, p)| p.chunks.get().map(|c| (i, c.clone())))
            .collect();
        tokio::spawn(async move {
            for g in from..=to {
                let pos = this_page(g);
                let Some((_, chunks)) = pages.iter().find(|(i, _)| *i == pos.page) else {
                    break; // The next page isn't loaded yet; the next read loads it.
                };
                let Some((_, c)) = chunks.get(pos.index) else {
                    break;
                };
                fetcher.prefetch_chunk(*c);
            }
        });
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
        match self.entry(ino)?.content {
            Content::Symlink(t) => Ok(t),
            _ => Err(Errno::EINVAL),
        }
    }

    /// Makes the file's written bytes and extent map durable on this machine.
    pub async fn fsync(&self, ino: u64) -> FsResult<()> {
        let _ = ino;
        todo!("M3: fsync")
    }

    pub async fn release(&self, fh: Fh) -> FsResult<()> {
        self.handles.lock().unwrap().remove(&fh.0);
        Ok(())
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

/// Fetches every tree of the mounted commit in the background, breadth-first, so `find` and
/// `stat` rarely wait on the network.
async fn walk_metadata(fetcher: Arc<Fetcher>, root: Id) {
    let mut level = vec![root];
    while !level.is_empty() {
        let trees: Vec<Arc<Tree>> = stream::iter(level)
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
