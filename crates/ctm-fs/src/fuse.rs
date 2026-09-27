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
    BackgroundSession, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, KernelConfig, LockOwner, MountOption, OpenAccMode, OpenFlags, ReplyAttr,
    ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};

use crate::{Attr, Fh, FileKind, MountState};

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
                Ok(fh) => reply.opened(FileHandle(fh.0), FopenFlags::FOPEN_KEEP_CACHE),
                Err(e) => reply.error(errno(e)),
            }
        });
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
