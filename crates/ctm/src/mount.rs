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

use ctm_fs::{MountOptions, MountState, fuse};
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

pub async fn mount(spec: &str, dir: &Path, read_only: bool, foreground: bool) -> Result<()> {
    let config = load_config()?;
    let (_, entry) = config.default_repo()?;
    let parsed: RefSpec = spec.parse()?;
    let is_branch = matches!(parsed.target, RefTarget::Branch(_));
    if is_branch && !read_only {
        return Err("read-write mounts arrive with writes; pass --read-only for now".into());
    }
    let mountpoint = absolute(dir)?;
    if let Some(old) = find_record(&mountpoint) {
        if alive(&old).await {
            return Err(format!("{} is already mounted", mountpoint.display()).into());
        }
        clear_stale(&mountpoint)?;
        fs::remove_dir_all(old.state_dir())?;
    } else {
        clear_stale(&mountpoint)?;
    }
    // Only now: statting a dead mount fails with "Transport endpoint is not connected".
    if !mountpoint.is_dir() {
        return Err(format!("{} is not a directory", mountpoint.display()).into());
    }
    let backend = ctm_store::open(&entry.url, entry.endpoint.as_deref())?;
    let repo = Repo::open(backend, identity(&config)?).await?;
    repo.resolve(&parsed).await?; // Fail here, not in the background, on an unknown ref.
    let record = MountRecord {
        mount_id: uuid::Uuid::new_v4().simple().to_string(),
        repo: entry.clone(),
        repo_id: repo.config().repo_id.clone(),
        spec: spec.to_string(),
        mountpoint: mountpoint.clone(),
        read_only: read_only || !is_branch,
        pid: None,
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
            let _ = fs::remove_dir_all(record.state_dir());
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
    let adapter = fuse::FuseAdapter::new(state.clone(), tokio::runtime::Handle::current());
    let session = fuse::spawn(adapter, &record.mountpoint, state.read_only())?;

    let socket = record.socket();
    fs::create_dir_all(socket.parent().expect("socket has a parent"))?;
    let _ = fs::remove_file(&socket);
    let listener = UnixListener::bind(&socket)?;
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
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
                let (response, stop) = handle(&state, &record, &request);
                let mut out = serde_json::to_vec(&response)?;
                out.push(b'\n');
                let _ = write.write_all(&out).await;
                if stop {
                    break;
                }
            }
            _ = term.recv() => break,
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    drop(listener);
    let _ = fs::remove_file(&socket);
    tokio::task::spawn_blocking(move || session.umount_and_join()).await??;
    fs::remove_dir_all(&state_dir)?;
    Ok(())
}

fn handle(state: &MountState, record: &MountRecord, request: &Request) -> (Response, bool) {
    match request {
        Request::Status => (
            Response::Ok {
                message: format!(
                    "{} at commit {} ({})",
                    record.spec,
                    &state.base_commit().to_hex()[..12],
                    if record.read_only {
                        "read-only"
                    } else {
                        "read-write"
                    }
                ),
            },
            false,
        ),
        Request::Unmount { .. } => (
            Response::Ok {
                message: format!("Unmounted {}", record.mountpoint.display()),
            },
            true,
        ),
        Request::Commit { .. } | Request::Restore { .. } => (
            Response::Error {
                message: "this mount is read-only".into(),
            },
            false,
        ),
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
            let deadline = Instant::now() + Duration::from_secs(60);
            while is_mounted(&mountpoint) || record.state_dir().exists() {
                if Instant::now() > deadline {
                    return Err("timed out waiting for the unmount".into());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            println!("{message}");
        }
        Ok(Response::Error { message }) => return Err(message.into()),
        Err(_) => {
            // The mount process is gone.
            clear_stale(&mountpoint)?;
            fs::remove_dir_all(record.state_dir())?;
            println!("Cleared a stale mount at {}", mountpoint.display());
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
