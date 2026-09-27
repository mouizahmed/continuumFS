//! Adapts [`MountState`] to FUSE through `fuser`.
//!
//! Callbacks are synchronous: each one moves its `Reply` into a tokio task and replies from
//! there, so the callback thread never blocks on I/O. Never touch the mountpoint from inside
//! the mount process.
//!
//! Kernel settings: entry and attr TTL 1 s, no writeback cache, `FOPEN_KEEP_CACHE` (a
//! mount's view only changes through its own writes), `max_readahead` and `max_write` 1 MiB.
//! No `auto_unmount`: it needs `allow_other`, and so `user_allow_other` in /etc/fuse.conf.
//! A mount left behind by a dead process is cleared by `ctm mount` and `ctm unmount`.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    BackgroundSession, BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem,
    FopenFlags, Generation, INodeNo, KernelConfig, LockOwner, MountOption, OpenAccMode, OpenFlags,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow, WriteFlags,
};

use crate::{Attr, Fh, FileKind, Invalidate, MountState, SetAttr};

const TTL: Duration = Duration::from_secs(1);

/// Everything appears owned by the mounting user.
#[derive(Clone, Copy)]
struct Owner {
    uid: u32,
    gid: u32,
}

impl Owner {
    fn attr(self, a: &Attr) -> FileAttr {
        let mtime = time_from_ns(a.mtime_ns);
        FileAttr {
            ino: INodeNo(a.ino),
            size: a.size,
            blocks: a.size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind: file_type(a.kind),
            perm: a.mode,
            nlink: a.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 128 * 1024,
            flags: 0,
        }
    }
}

pub struct FuseAdapter {
    state: Arc<MountState>,
    rt: tokio::runtime::Handle,
    owner: Owner,
}

impl FuseAdapter {
    pub fn new(state: Arc<MountState>, rt: tokio::runtime::Handle) -> FuseAdapter {
        FuseAdapter {
            state,
            rt,
            owner: Owner {
                uid: rustix::process::getuid().as_raw(),
                gid: rustix::process::getgid().as_raw(),
            },
        }
    }
}

fn file_type(k: FileKind) -> FileType {
    match k {
        FileKind::File => FileType::RegularFile,
        FileKind::Dir => FileType::Directory,
        FileKind::Symlink => FileType::Symlink,
    }
}

fn errno(e: crate::Errno) -> Errno {
    Errno::from_i32(e.0)
}

fn ns(t: SystemTime) -> i64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

/// Replies to an operation that yields an entry.
fn reply_entry(reply: ReplyEntry, owner: Owner, r: crate::FsResult<Attr>) {
    match r {
        Ok(a) => reply.entry(&TTL, &owner.attr(&a), Generation(0)),
        Err(e) => reply.error(errno(e)),
    }
}

fn reply_empty(reply: ReplyEmpty, r: crate::FsResult<()>) {
    match r {
        Ok(()) => reply.ok(),
        Err(e) => reply.error(errno(e)),
    }
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

impl Filesystem for FuseAdapter {
    fn init(&mut self, _req: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // Together these also negotiate max_pages = 256.
        let _ = config.set_max_readahead(1 << 20);
        let _ = config.set_max_write(1 << 20);
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let (state, owner, name) = (self.state.clone(), self.owner, name.as_bytes().to_vec());
        self.rt.spawn(async move {
            match state.lookup(parent.0, &name).await {
                Ok(a) => reply.entry(&TTL, &owner.attr(&a), Generation(0)),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn forget(&self, _req: &Request, ino: INodeNo, nlookup: u64) {
        self.state.forget(ino.0, nlookup);
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let (state, owner) = (self.state.clone(), self.owner);
        self.rt.spawn(async move {
            match state.getattr(ino.0).await {
                Ok(a) => reply.attr(&TTL, &owner.attr(&a)),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let state = self.state.clone();
        self.rt.spawn(async move {
            match state.readlink(ino.0).await {
                Ok(t) => reply.data(&t),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let state = self.state.clone();
        let write = flags.acc_mode() != OpenAccMode::O_RDONLY;
        self.rt.spawn(async move {
            match state.open_file(ino.0, write).await {
                Ok(fh) => {
                    // Clean files keep the kernel's cached pages; the view can't change.
                    let flags = if state.keep_cache(ino.0) {
                        FopenFlags::FOPEN_KEEP_CACHE
                    } else {
                        FopenFlags::empty()
                    };
                    reply.opened(FileHandle(fh.0), flags)
                }
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let (state, owner) = (self.state.clone(), self.owner);
        // Everything is owned by the mounting user: chown to them is a no-op.
        if uid.is_some_and(|u| u != owner.uid) || gid.is_some_and(|g| g != owner.gid) {
            return reply.error(Errno::EPERM);
        }
        let set = SetAttr {
            size,
            mode: mode.map(|m| (m & 0o7777) as u16),
            mtime_ns: mtime.map(|t| match t {
                TimeOrNow::SpecificTime(t) => ns(t),
                TimeOrNow::Now => ns(SystemTime::now()),
            }),
        };
        self.rt.spawn(async move {
            let r = if set == SetAttr::default() {
                state.getattr(ino.0).await
            } else {
                state.setattr(ino.0, set).await
            };
            match r {
                Ok(a) => reply.attr(&TTL, &owner.attr(&a)),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        // Regular files only; FIFOs, sockets, and device nodes aren't supported.
        if mode & libc::S_IFMT != libc::S_IFREG {
            return reply.error(Errno::ENOTSUP);
        }
        let (state, owner, name) = (self.state.clone(), self.owner, name.as_bytes().to_vec());
        let mode = (mode & !umask & 0o7777) as u16;
        self.rt.spawn(async move {
            let r = match state.create(parent.0, &name, mode).await {
                Ok((a, fh)) => state.release(fh).await.map(|()| a),
                Err(e) => Err(e),
            };
            reply_entry(reply, owner, r);
        });
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let (state, owner, name) = (self.state.clone(), self.owner, name.as_bytes().to_vec());
        let mode = (mode & !umask & 0o7777) as u16;
        self.rt.spawn(async move {
            reply_entry(reply, owner, state.mkdir(parent.0, &name, mode).await);
        });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (state, name) = (self.state.clone(), name.as_bytes().to_vec());
        self.rt.spawn(async move {
            reply_empty(reply, state.unlink(parent.0, &name).await);
        });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let (state, name) = (self.state.clone(), name.as_bytes().to_vec());
        self.rt.spawn(async move {
            reply_empty(reply, state.rmdir(parent.0, &name).await);
        });
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let (state, owner) = (self.state.clone(), self.owner);
        let name = link_name.as_bytes().to_vec();
        let target = target.as_os_str().as_bytes().to_vec();
        self.rt.spawn(async move {
            reply_entry(reply, owner, state.symlink(parent.0, &name, &target).await);
        });
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        if flags.contains(RenameFlags::RENAME_EXCHANGE) {
            return reply.error(Errno::EINVAL);
        }
        let no_replace = flags.contains(RenameFlags::RENAME_NOREPLACE);
        let state = self.state.clone();
        let (name, newname) = (name.as_bytes().to_vec(), newname.as_bytes().to_vec());
        self.rt.spawn(async move {
            let r = state
                .rename(parent.0, &name, newparent.0, &newname, no_replace)
                .await;
            reply_empty(reply, r);
        });
    }

    fn link(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _newparent: INodeNo,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(Errno::EPERM);
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let (state, data) = (self.state.clone(), data.to_vec());
        self.rt.spawn(async move {
            match state.write(Fh(fh.0), ino.0, offset, &data).await {
                Ok(n) => reply.written(n),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let (state, owner, name) = (self.state.clone(), self.owner, name.as_bytes().to_vec());
        let mode = (mode & !umask & 0o7777) as u16;
        self.rt.spawn(async move {
            match state.create(parent.0, &name, mode).await {
                Ok((a, fh)) => reply.created(
                    &TTL,
                    &owner.attr(&a),
                    Generation(0),
                    FileHandle(fh.0),
                    FopenFlags::empty(),
                ),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let state = self.state.clone();
        self.rt.spawn(async move {
            reply_empty(reply, state.fsync(ino.0).await);
        });
    }

    fn fsyncdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOTSUP);
    }

    fn getxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Errno::ENOTSUP);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::ENOTSUP);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::ENOTSUP);
    }

    fn fallocate(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOTSUP);
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let state = self.state.clone();
        self.rt.spawn(async move {
            match state.read(Fh(fh.0), ino.0, offset, size).await {
                Ok(data) => reply.data(&data),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let state = self.state.clone();
        self.rt.spawn(async move {
            match state.release(Fh(fh.0)).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(e)),
            }
        });
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let state = self.state.clone();
        self.rt.spawn(async move {
            let items = match state.readdir(ino.0).await {
                Ok(items) => items,
                Err(e) => return reply.error(errno(e)),
            };
            let dots = [
                (ino.0, &b"."[..], FileKind::Dir),
                (state.parent(ino.0), &b".."[..], FileKind::Dir),
            ];
            let all = dots
                .into_iter()
                .chain(items.iter().map(|i| (i.ino, i.name.as_slice(), i.kind)));
            for (i, (ino, name, kind)) in all.enumerate().skip(offset as usize) {
                if reply.add(
                    INodeNo(ino),
                    i as u64 + 1,
                    file_type(kind),
                    OsStr::from_bytes(name),
                ) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        match self.state.statfs() {
            Ok(s) => reply.statfs(
                s.blocks,
                s.blocks_free,
                s.blocks_avail,
                0,
                0,
                s.block_size as u32,
                255,
                s.block_size as u32,
            ),
            Err(e) => reply.error(errno(e)),
        }
    }
}

/// Sends the mount's invalidations (from `restore`) to the kernel through `session`.
pub fn connect_notifier(session: &BackgroundSession, state: &MountState) {
    let notifier = session.notifier();
    state.set_notifier(Box::new(move |i| {
        let r = match &i {
            Invalidate::Inode(ino) => notifier.inval_inode(INodeNo(*ino), 0, 0),
            Invalidate::Entry { parent, name } => {
                notifier.inval_entry(INodeNo(*parent), OsStr::from_bytes(name))
            }
        };
        // ENOENT just means the kernel had nothing cached.
        if let Err(e) = r
            && e.raw_os_error() != Some(libc::ENOENT)
        {
            tracing::warn!("invalidating {i:?}: {e}");
        }
    }));
}

/// Mounts `adapter` at `mountpoint` and returns the running session. Dropping the session,
/// or calling `umount_and_join`, unmounts.
pub fn spawn(
    adapter: FuseAdapter,
    mountpoint: &Path,
    read_only: bool,
) -> std::io::Result<BackgroundSession> {
    let mut options = vec![
        MountOption::FSName("continuum".into()),
        MountOption::Subtype("ctm".into()),
        MountOption::DefaultPermissions,
        MountOption::NoAtime,
    ];
    if read_only {
        options.push(MountOption::RO);
    }
    let mut config = Config::default();
    config.mount_options = options;
    fuser::spawn_mount(adapter, mountpoint, &config)
}
