//! Mounting: one background process per mount, driven through a control socket.

use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;

use ctm_fs::{CommitOutcome, MountOptions, MountState, fuse};
use ctm_repo::{RefSpec, Repo, refspec::RefTarget};

use crate::Result;
use crate::commands::{identity, load_config};
use crate::config::RepoEntry;
use crate::control::{self, Request, Response};
use crate::paths;

/// `mount.json` in a mount's working-state directory.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountRecord {
    pub mount_id: String,
    pub repo: RepoEntry,
    pub repo_id: String,
    /// The ref as given to `ctm mount`.
    pub spec: String,
    pub mountpoint: PathBuf,
    pub read_only: bool,
    /// The mount process, once it has started.
    pub pid: Option<u32>,
}

impl MountRecord {
    fn state_dir(&self) -> PathBuf {
        paths::mounts_dir().join(&self.mount_id)
    }

    fn save(&self) -> io::Result<()> {
        let dir = self.state_dir();
        fs::create_dir_all(&dir)?;
        let tmp = dir.join("mount.json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        fs::rename(tmp, dir.join("mount.json"))
    }

    fn load(state_dir: &Path) -> io::Result<MountRecord> {
        Ok(serde_json::from_slice(&fs::read(
            state_dir.join("mount.json"),
        )?)?)
    }

    fn socket(&self) -> PathBuf {
        paths::control_socket(&self.mount_id)
    }
}

/// Every mount record on this machine.
fn records() -> Vec<MountRecord> {
    let Ok(dirs) = fs::read_dir(paths::mounts_dir()) else {
        return Vec::new();
    };
    dirs.filter_map(|d| MountRecord::load(&d.ok()?.path()).ok())
        .collect()
}

fn find_record(mountpoint: &Path) -> Option<MountRecord> {
    records().into_iter().find(|r| r.mountpoint == mountpoint)
}

/// Whether `path` is a mount point, from /proc/self/mountinfo.
fn is_mounted(path: &Path) -> bool {
    let Ok(info) = fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    let want = path.to_string_lossy();
    info.lines().any(|line| {
        line.split(' ')
            .nth(4)
            .is_some_and(|p| unescape_mountinfo(p) == want)
    })
}

/// Undoes mountinfo's octal escapes (`\040` for a space).
fn unescape_mountinfo(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\'
            && i + 4 <= b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 4], 8)
        {
            out.push(v);
            i += 4;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `fusermount3 -u` (with `-z`, a lazy detach that succeeds even while files are open).
fn fusermount(mountpoint: &Path, lazy: bool) -> std::result::Result<(), String> {
    let mut cmd = Command::new("fusermount3");
    cmd.arg("-u");
    if lazy {
        cmd.arg("-z");
    }
    let out = cmd.arg(mountpoint).output().map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Detaches a mount whose process is gone (`Transport endpoint is not connected`).
fn clear_stale(mountpoint: &Path) -> Result<()> {
    if !is_mounted(mountpoint) {
        return Ok(());
    }
    let status = Command::new("fusermount3")
        .args(["-u", "-z"])
        .arg(mountpoint)
        .status()?;
    if !status.success() {
        return Err(format!(
            "could not clear the stale mount at {}",
            mountpoint.display()
        )
        .into());
    }
    Ok(())
}

async fn alive(record: &MountRecord) -> bool {
    control::call(&record.socket(), &Request::Status)
        .await
        .is_ok()
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().map_err(|_| format!("bad size {s:?}"))?;
    let mult: u64 = match unit.trim() {
        "" | "B" => 1,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        _ => return Err(format!("bad size unit in {s:?} (use B, KiB, MiB, GiB, or TiB)").into()),
    };
    Ok(n * mult)
}

/// Whether a mount's working state should outlive it: read-write, with uncommitted changes.
fn keeps_state(record: &MountRecord) -> bool {
    !record.read_only && ctm_fs::has_local_changes(&record.state_dir()).unwrap_or(true)
}

pub async fn mount(spec: &str, dir: &Path, read_only: bool, foreground: bool) -> Result<()> {
    let config = load_config()?;
    let (_, entry) = config.default_repo()?;
    let parsed: RefSpec = spec.parse()?;
    let is_branch = matches!(parsed.target, RefTarget::Branch(_));
    let read_only = read_only || !is_branch;
    let mountpoint = absolute(dir)?;
    let backend = ctm_store::open(&entry.url, entry.endpoint.as_deref())?;
    let repo = Repo::open(backend, identity(&config)?).await?;
    let repo_id = repo.config().repo_id.clone();

    // A mount of this directory left behind by a crash, `--no-commit`, or a failed commit.
    let mut reuse = None;
    if let Some(old) = find_record(&mountpoint) {
        if alive(&old).await {
            return Err(format!("{} is already mounted", mountpoint.display()).into());
        }
        clear_stale(&mountpoint)?;
        if !keeps_state(&old) {
            fs::remove_dir_all(old.state_dir())?;
        } else if old.repo_id == repo_id && old.spec == spec && !read_only {
            reuse = Some(old);
        } else {
            return Err(format!(
                "{} has uncommitted changes for {}; mount that branch there to commit them, \
                 or delete {} to discard them",
                mountpoint.display(),
                old.spec,
                old.state_dir().display()
            )
            .into());
        }
    } else {
        clear_stale(&mountpoint)?;
    }
    // Only now: statting a dead mount fails with "Transport endpoint is not connected".
    if !mountpoint.is_dir() {
        return Err(format!("{} is not a directory", mountpoint.display()).into());
    }
    let record = match reuse {
        Some(old) => old,
        None => {
            repo.resolve(&parsed).await?; // Fail here, not in the background, on an unknown ref.
            MountRecord {
                mount_id: uuid::Uuid::new_v4().simple().to_string(),
                repo: entry.clone(),
                repo_id,
                spec: spec.to_string(),
                mountpoint: mountpoint.clone(),
                read_only,
                pid: None,
            }
        }
    };
    record.save()?;
    if foreground {
        return run(record.state_dir()).await;
    }
    let log = fs::File::create(record.state_dir().join("mount.log"))?;
    let mut child = Command::new(std::env::current_exe()?)
        .arg("mount-process")
        .arg(record.state_dir())
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .process_group(0)
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if alive(&record).await {
            println!("Mounted {spec} at {}", mountpoint.display());
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            let log = fs::read_to_string(record.state_dir().join("mount.log")).unwrap_or_default();
            if !keeps_state(&record) {
                let _ = fs::remove_dir_all(record.state_dir());
            }
            return Err(format!("the mount process exited ({status}):\n{log}").into());
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err("timed out waiting for the mount".into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The mount process: serves FUSE and the control socket until asked to unmount.
pub async fn run(state_dir: PathBuf) -> Result<()> {
    let mut record = MountRecord::load(&state_dir)?;
    record.pid = Some(std::process::id());
    record.save()?;
    let config = load_config()?;
    let backend = ctm_store::open(&record.repo.url, record.repo.endpoint.as_deref())?;
    let repo = Arc::new(Repo::open(backend, identity(&config)?).await?);
    let opts = MountOptions {
        read_only: record.read_only,
        chunk_cache_max: parse_size(&config.cache.chunks_max)?,
        ..MountOptions::default()
    };
    let state = Arc::new(
        MountState::open(
            repo,
            &paths::cache_dir(&record.repo_id),
            &state_dir,
            &record.spec.parse()?,
            opts,
        )
        .await?,
    );
    // Recovery may have moved the mount to its auto-fork.
    if let Some(branch) = state.branch()
        && !state.read_only()
        && branch.as_str() != record.spec
    {
        record.spec = branch.to_string();
        record.save()?;
    }
    let adapter = fuse::FuseAdapter::new(state.clone(), tokio::runtime::Handle::current());
    let session = fuse::spawn(adapter, &record.mountpoint, state.read_only())?;
    fuse::connect_notifier(&session, &state);

    let socket = record.socket();
    fs::create_dir_all(socket.parent().expect("socket has a parent"))?;
    let _ = fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    // Set once `ctm unmount` has detached the mount; a signal detaches it lazily instead.
    let mut unmounted = false;
    loop {
        tokio::select! {
            conn = listener.accept() => {
                let (stream, _) = conn?;
                let (read, mut write) = stream.into_split();
                let mut line = String::new();
                if BufReader::new(read).read_line(&mut line).await? == 0 {
                    continue;
                }
                let request: Request = match serde_json::from_str(&line) {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("bad control request: {e}");
                        continue;
                    }
                };
                let (response, stop) = handle(&state, &mut record, &request).await;
                let mut out = serde_json::to_vec(&response)?;
                out.push(b'\n');
                let _ = write.write_all(&out).await;
                if stop {
                    unmounted = true;
                    break;
                }
            }
            _ = term.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    drop(listener);
    let _ = fs::remove_file(&socket);
    if !unmounted {
        // Exiting on a signal: detach even if files are open, so no dead mount stays behind.
        let _ = fusermount(&record.mountpoint, true);
    }
    // The kernel has let go of the mount, so the session ends; wait for it.
    tokio::task::spawn_blocking(move || session.join()).await??;
    drop(state);
    if !keeps_state(&record) {
        fs::remove_dir_all(&state_dir)?;
    }
    Ok(())
}

fn ok(message: String) -> Response {
    Response::Ok { message }
}

fn error(message: impl std::fmt::Display) -> Response {
    Response::Error {
        message: message.to_string(),
    }
}

/// Commits, and follows an auto-fork in `mount.json`.
async fn commit(state: &MountState, record: &mut MountRecord, message: &str) -> Response {
    match state.commit(message).await {
        Ok(CommitOutcome::Pushed { commit }) => ok(format!(
            "Committed {} to {}",
            &commit.to_hex()[..12],
            record.spec
        )),
        Ok(CommitOutcome::AutoForked { from, branch, .. }) => {
            record.spec = branch.to_string();
            if let Err(e) = record.save() {
                tracing::warn!("updating mount.json: {e}");
            }
            ok(format!(
                "{from} moved on another machine. Your changes are safe on {branch}."
            ))
        }
        Ok(CommitOutcome::NothingToCommit) => ok("Nothing to commit".into()),
        Err(e) => error(format!("commit failed: {e}")),
    }
}

async fn handle(
    state: &MountState,
    record: &mut MountRecord,
    request: &Request,
) -> (Response, bool) {
    match request {
        Request::Status => match state.status().await {
            Ok(s) => {
                let mut lines = Vec::new();
                match (&s.branch, &s.auto_forked_from) {
                    (Some(b), Some(from)) => {
                        lines.push(format!("Branch: {b} (auto-forked from {from})"))
                    }
                    (Some(b), None) => lines.push(format!("Branch: {b}")),
                    (None, _) => lines.push(format!("Ref: {}", record.spec)),
                }
                lines.push(format!("Base commit: {}", &s.base_commit.to_hex()[..12]));
                if s.read_only {
                    lines.push("Read-only".into());
                } else {
                    lines.push(format!("Changes: {}", s.dirty_files));
                }
                if s.behind
                    && let Some(b) = &s.branch
                {
                    lines.push(format!(
                        "Behind: {b} moved on another machine since this mount's base"
                    ));
                }
                (ok(lines.join("\n")), false)
            }
            Err(e) => (error(e), false),
        },
        Request::Commit { message } => (commit(state, record, message).await, false),
        Request::Restore { path, at } => {
            let spec = match at.parse() {
                Ok(s) => s,
                Err(e) => return (error(e), false),
            };
            match state.restore(path.as_bytes(), &spec).await {
                Ok(()) => (ok(format!("Restored {path} from {at}")), false),
                Err(e) => (error(e), false),
            }
        }
        Request::Unmount {
            commit: wants_commit,
        } => {
            if *wants_commit
                && !state.read_only()
                && let Response::Error { message } = commit(state, record, "").await
            {
                return (
                    error(format!(
                        "{message}; still mounted (retry, or pass --no-commit)"
                    )),
                    false,
                );
            }
            match fusermount(&record.mountpoint, false) {
                Ok(()) => (
                    ok(format!("Unmounted {}", record.mountpoint.display())),
                    true,
                ),
                Err(e) => (
                    error(format!(
                        "{} is busy; close the files open in it and retry ({e})",
                        record.mountpoint.display()
                    )),
                    false,
                ),
            }
        }
    }
}

/// The absolute path of `dir`, resolving symlinks in its parent but never statting `dir`
/// itself, which may be a dead mount.
fn absolute(dir: &Path) -> Result<PathBuf> {
    let dir = std::path::absolute(dir)?;
    match (dir.parent(), dir.file_name()) {
        (Some(parent), Some(name)) => Ok(fs::canonicalize(parent)
            .map_err(|e| format!("{}: {e}", parent.display()))?
            .join(name)),
        _ => Ok(dir),
    }
}

/// The mount record for `dir`, or an error saying it isn't a mount.
fn mount_at(dir: &Path) -> Result<MountRecord> {
    let mountpoint = absolute(dir)?;
    find_record(&mountpoint)
        .ok_or_else(|| format!("{} is not a ctm mount", mountpoint.display()).into())
}

async fn call(record: &MountRecord, request: &Request) -> Result<String> {
    match control::call(&record.socket(), request).await {
        Ok(Response::Ok { message }) => Ok(message),
        Ok(Response::Error { message }) => Err(message.into()),
        Err(e) => Err(format!(
            "the mount at {} isn't running ({e}); run `ctm mount` again",
            record.mountpoint.display()
        )
        .into()),
    }
}

pub async fn commit_cmd(dir: &Path, message: &str) -> Result<()> {
    let record = mount_at(dir)?;
    println!(
        "{}",
        call(
            &record,
            &Request::Commit {
                message: message.to_string()
            }
        )
        .await?
    );
    Ok(())
}

pub async fn status(dir: &Path) -> Result<()> {
    println!("{}", call(&mount_at(dir)?, &Request::Status).await?);
    Ok(())
}

pub async fn restore(path: &Path, at: &str) -> Result<()> {
    let path = absolute(path)?;
    let record = records()
        .into_iter()
        .filter(|r| path.starts_with(&r.mountpoint) && path != r.mountpoint)
        .max_by_key(|r| r.mountpoint.as_os_str().len())
        .ok_or_else(|| format!("{} is not inside a ctm mount", path.display()))?;
    let rel = path
        .strip_prefix(&record.mountpoint)
        .expect("filtered above")
        .to_str()
        .ok_or("the path isn't valid UTF-8")?
        .to_string();
    let request = Request::Restore {
        path: rel,
        at: at.to_string(),
    };
    println!("{}", call(&record, &request).await?);
    Ok(())
}

pub async fn unmount(dir: &Path, no_commit: bool) -> Result<()> {
    let mountpoint = absolute(dir)?;
    let Some(record) = find_record(&mountpoint) else {
        if is_mounted(&mountpoint) {
            clear_stale(&mountpoint)?;
            println!("Cleared a stale mount at {}", mountpoint.display());
            return Ok(());
        }
        return Err(format!("{} is not a ctm mount", mountpoint.display()).into());
    };
    match control::call(&record.socket(), &Request::Unmount { commit: !no_commit }).await {
        Ok(Response::Ok { message }) => {
            // Wait for the mount process to finish unmounting and exit.
            let pid = MountRecord::load(&record.state_dir())
                .ok()
                .and_then(|r| r.pid);
            let deadline = Instant::now() + Duration::from_secs(60);
            while is_mounted(&mountpoint)
                || pid.is_some_and(|p| Path::new(&format!("/proc/{p}")).exists())
            {
                if Instant::now() > deadline {
                    return Err("timed out waiting for the unmount".into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            println!("{message}");
            if keeps_state(&record) && record.state_dir().exists() {
                println!(
                    "Uncommitted changes are kept for the next `ctm mount {}` here",
                    record.spec
                );
            }
        }
        Ok(Response::Error { message }) => return Err(message.into()),
        Err(_) => {
            // The mount process is gone.
            clear_stale(&mountpoint)?;
            if keeps_state(&record) {
                println!(
                    "Cleared a stale mount at {}; its uncommitted changes are kept for the next `ctm mount {}` there",
                    mountpoint.display(),
                    record.spec
                );
            } else {
                fs::remove_dir_all(record.state_dir())?;
                println!("Cleared a stale mount at {}", mountpoint.display());
            }
        }
    }
    Ok(())
}

pub async fn cache_stats(json: bool) -> Result<()> {
    let config = load_config()?;
    let (_, entry) = config.default_repo()?;
    let repo = Repo::open(
        ctm_store::open(&entry.url, entry.endpoint.as_deref())?,
        identity(&config)?,
    )
    .await?;
    let cache = ctm_store::cache::ChunkCache::open(
        paths::cache_dir(&repo.config().repo_id),
        parse_size(&config.cache.chunks_max)?,
    )?;
    let s = cache.stats()?;
    if json {
        println!(
            "{}",
            serde_json::json!({
                "bytes": s.bytes,
                "objects": s.objects,
                "hits": s.hits,
                "misses": s.misses,
                "fetched_bytes": s.fetched_bytes,
            })
        );
    } else {
        println!("Cached: {} ({} chunks)", human(s.bytes), s.objects);
        println!(
            "Hits: {}  Misses: {}  Downloaded: {}",
            s.hits,
            s.misses,
            human(s.fetched_bytes)
        );
    }
    Ok(())
}

fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit + 1 < UNITS.len() {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}
