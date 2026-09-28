//! The commands that run directly against the bucket.

use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

use ctm_core::diff::Change;
use ctm_core::{Content, DirEntry, Kind};
use ctm_repo::{BranchName, Identity, InitOutcome, RefSpec, Repo};

use crate::Result;
use crate::config::LocalConfig;
use crate::paths;

pub(crate) fn load_config() -> Result<LocalConfig> {
    Ok(LocalConfig::load_or_create(&paths::config_file())?)
}

pub(crate) fn identity(config: &LocalConfig) -> Result<Identity> {
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
        .or_else(|_| std::fs::read_to_string("/etc/hostname"))
        .map(|h| h.trim().to_string())
        .unwrap_or_else(|_| "localhost".to_string());
    Ok(Identity {
        user: std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()),
        hostname,
        machine_id: config.machine_id()?,
    })
}

/// Opens the default repo.
pub async fn open_repo() -> Result<Repo> {
    let config = load_config()?;
    let (_, entry) = config.default_repo()?;
    let backend = ctm_store::open(&entry.url, entry.endpoint.as_deref())?;
    open_at(backend, identity(&config)?).await
}

/// Opens a repo with its index mirror in the repo's cache directory.
pub async fn open_at(backend: Arc<dyn ctm_store::Backend>, identity: Identity) -> Result<Repo> {
    let repo = Repo::open(backend, identity).await?;
    let dir = paths::cache_dir(&repo.config().repo_id);
    std::fs::create_dir_all(&dir)?;
    Ok(repo.with_index_at(&dir.join("index.db"))?)
}

pub async fn init(url: &str, endpoint: Option<&str>) -> Result<()> {
    let mut config = load_config()?;
    let backend: Arc<dyn ctm_store::Backend> = ctm_store::open(url, endpoint)?;
    let (_, outcome) = Repo::init(backend, identity(&config)?).await?;
    let name = config.connect(url, endpoint);
    config.save(&paths::config_file())?;
    match outcome {
        InitOutcome::Created => println!("Created repo {name} at {url}"),
        InitOutcome::Connected => println!("Connected to repo {name} at {url}"),
    }
    Ok(())
}

fn short(id: &ctm_core::Id) -> String {
    id.to_hex()[..12].to_string()
}

pub async fn import(dir: &Path, branch: &str, message: &str) -> Result<()> {
    let repo = open_repo().await?;
    let branch = BranchName::new(branch)?;
    let imported = repo.import(dir, &branch, message).await?;
    if imported.skipped > 0 {
        eprintln!(
            "Skipped {} special files (FIFOs, sockets, devices)",
            imported.skipped
        );
    }
    println!(
        "Imported {} → {branch} (commit {}; uploaded {} objects, {})",
        dir.display(),
        short(&imported.commit),
        imported.uploaded.objects,
        crate::mount::human(imported.uploaded.bytes)
    );
    Ok(())
}

pub async fn export(spec: &str, dir: &Path) -> Result<()> {
    let repo = open_repo().await?;
    repo.export(&spec.parse()?, dir).await?;
    Ok(())
}

/// `-rw-r--r--`-style mode string.
fn mode_string(e: &DirEntry) -> String {
    let kind = match e.content.kind() {
        Kind::File => '-',
        Kind::Dir => 'd',
        Kind::Symlink => 'l',
    };
    let mut s = String::from(kind);
    for shift in [6, 3, 0] {
        let bits = (e.mode >> shift) & 7;
        s.push(if bits & 4 != 0 { 'r' } else { '-' });
        s.push(if bits & 2 != 0 { 'w' } else { '-' });
        s.push(if bits & 1 != 0 { 'x' } else { '-' });
    }
    s
}

pub async fn ls(spec: &str) -> Result<()> {
    let repo = open_repo().await?;
    let mut out = io::stdout().lock();
    for e in repo.ls(&spec.parse()?).await? {
        let name = String::from_utf8_lossy(&e.name);
        match &e.content {
            Content::Symlink(t) => writeln!(
                out,
                "{} {:>12} {name} -> {}",
                mode_string(&e),
                e.size,
                String::from_utf8_lossy(t)
            )?,
            Content::Dir(_) => writeln!(out, "{} {:>12} {name}/", mode_string(&e), e.size)?,
            _ => writeln!(out, "{} {:>12} {name}", mode_string(&e), e.size)?,
        }
    }
    Ok(())
}

pub async fn cat(spec: &str) -> Result<()> {
    let repo = open_repo().await?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    repo.cat(&spec.parse()?, &mut out).await?;
    out.flush()?;
    Ok(())
}

pub async fn fork(from: &str, new: &str) -> Result<()> {
    crate::mount::sync_branch(from).await?;
    let repo = open_repo().await?;
    let new = BranchName::new(new)?;
    let r = repo.fork(&from.parse()?, &new).await?;
    println!("Forked {from} → {new} (commit {})", short(&r.head));
    Ok(())
}

pub async fn branch_list() -> Result<()> {
    for name in open_repo().await?.list_branches().await? {
        println!("{name}");
    }
    Ok(())
}

pub async fn snapshot_create(name: &str, from: Option<&str>) -> Result<()> {
    let repo = open_repo().await?;
    let name = BranchName::new(name)?;
    let from_str = from.unwrap_or("main");
    crate::mount::sync_branch(from_str).await?;
    let from: RefSpec = from_str.parse()?;
    let snap = repo.snapshot(&name, &from).await?;
    println!("Created snapshot {name} (commit {})", short(&snap.commit));
    Ok(())
}

pub async fn snapshot_list() -> Result<()> {
    for name in open_repo().await?.list_snapshots().await? {
        println!("{name}");
    }
    Ok(())
}

pub async fn log(spec: &str, path: Option<&str>) -> Result<()> {
    let repo = open_repo().await?;
    let entries = repo.log(&spec.parse()?, path.map(str::as_bytes)).await?;
    let mut out = io::stdout().lock();
    for e in entries {
        let kind = format!("{:?}", e.kind).to_lowercase();
        writeln!(
            out,
            "{} {} {kind:<7} {}",
            short(&e.commit),
            ctm_repo::rfc3339(e.time_ns),
            e.message
        )?;
    }
    Ok(())
}

pub async fn diff(a: &str, b: &str, stat: bool) -> Result<()> {
    let repo = open_repo().await?;
    let changes = repo.diff(&a.parse()?, &b.parse()?).await?;
    let mut out = io::stdout().lock();
    let (mut added, mut modified, mut removed) = (0, 0, 0);
    for c in &changes {
        let what = match c.change {
            Change::Added(_) => {
                added += 1;
                "added"
            }
            Change::Modified { .. } => {
                modified += 1;
                "modified"
            }
            Change::Removed(_) => {
                removed += 1;
                "removed"
            }
        };
        writeln!(out, "{what}: {}", String::from_utf8_lossy(&c.path))?;
    }
    if stat {
        writeln!(out, "{added} added, {modified} modified, {removed} removed")?;
    }
    Ok(())
}

pub async fn gc(dry_run: bool, grace_secs: Option<u64>) -> Result<()> {
    let config = load_config()?;
    let repo = open_repo().await?;
    let mut opts = ctm_repo::GcOptions {
        auto_retention: std::time::Duration::from_secs(config.retention.auto_days * 86_400),
        dry_run,
        ..ctm_repo::GcOptions::default()
    };
    if let Some(s) = grace_secs {
        opts.grace = std::time::Duration::from_secs(s);
    }
    let r = repo.gc(&opts).await?;
    let verb = if dry_run { "Would delete" } else { "Deleted" };
    println!(
        "Retention: {} auto-commit{} dropped from branch logs",
        r.dropped_commits,
        if r.dropped_commits == 1 { "" } else { "s" }
    );
    println!(
        "Marked: {} objects reachable; {} unreachable and {} unindexed packs listed for a later run",
        r.live_objects, r.newly_dead, r.new_orphans
    );
    println!(
        "{verb}: {} objects in {} packs and {} loose objects, {} orphan packs; {} packs repacked; \
         {} freed",
        r.deleted_objects,
        r.deleted_packs,
        r.deleted_loose,
        r.deleted_orphans,
        r.repacked_packs,
        crate::mount::human(r.freed_bytes)
    );
    if r.compacted_segments > 1 {
        println!(
            "Index: {} segments compacted into one",
            r.compacted_segments
        );
    }
    Ok(())
}

pub async fn fsck(spec: Option<&str>) -> Result<()> {
    let repo = open_repo().await?;
    let problems = match spec {
        Some(s) => ctm_repo::check_ref(&repo, &s.parse()?).await?,
        None => ctm_repo::check(&repo).await?,
    };
    if problems.is_empty() {
        println!("No problems found");
        return Ok(());
    }
    for p in &problems {
        println!("{p}");
    }
    Err(format!("{} problems found", problems.len()).into())
}
