use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// One allocator on every target: musl's (in the static release binaries) is much slower with
/// many threads allocating chunk-sized buffers.
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod commands;
mod config;
mod control;
mod mount;
mod paths;

#[derive(Parser)]
#[command(
    name = "ctm",
    version,
    about = "Git branches for whole workspaces, backed by a bucket"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a repo at s3://bucket/prefix or file:///path, or connect to the one already there
    Init {
        url: String,
        /// S3-compatible endpoint (R2, MinIO); omit for AWS S3
        #[arg(long)]
        endpoint: Option<String>,
    },
    /// Commit a local directory to a branch
    Import {
        dir: PathBuf,
        #[arg(long)]
        branch: String,
        #[arg(short, long, default_value = "")]
        message: String,
    },
    /// Write a ref (or a path inside it) to a local directory
    Export { spec: String, dir: PathBuf },
    /// List a directory inside a ref
    Ls { spec: String },
    /// Print a file inside a ref
    Cat { spec: String },
    /// Mount a ref at a directory
    Mount {
        spec: String,
        dir: PathBuf,
        #[arg(long)]
        read_only: bool,
        #[arg(long)]
        foreground: bool,
    },
    /// Commit, wait for the push, then unmount
    Unmount {
        dir: PathBuf,
        /// Don't wait for the push; unpushed commits are pushed by the next mount here
        #[arg(long)]
        no_wait: bool,
    },
    /// Commit a mount's changes (locally; they're pushed in the background)
    Commit {
        dir: PathBuf,
        #[arg(short, long, default_value = "")]
        message: String,
    },
    /// Show a mount's branch, base commit, changes, unpushed commits, and whether it is behind
    Status { dir: PathBuf },
    /// Wait until a mount (or every read-write mount) has pushed every commit made so far
    Sync { dir: Option<PathBuf> },
    /// Create a new branch from any ref
    Fork { from: String, new: String },
    /// Merge one branch into another, entry by entry (never inside a file)
    Merge {
        src: String,
        /// The branch to merge into
        #[arg(long)]
        into: String,
        /// On a conflict, keep this branch's version
        #[arg(long, group = "resolve")]
        ours: bool,
        /// On a conflict, take the merged branch's version
        #[arg(long, group = "resolve")]
        theirs: bool,
        /// On a conflict, keep both: ours at the path, theirs next to it as <stem>.<src>.<ext>
        #[arg(long, group = "resolve")]
        keep_both: bool,
        /// Report what would happen, change nothing
        #[arg(long)]
        dry_run: bool,
    },
    #[command(subcommand)]
    Branch(BranchCommand),
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
    /// Show a ref's history, optionally only where a path changed
    Log {
        spec: String,
        #[arg(last = true)]
        path: Option<String>,
    },
    /// Compare two refs
    Diff {
        a: String,
        b: String,
        #[arg(long)]
        stat: bool,
    },
    /// Point a branch back at another ref's commit (the old head stays in its log)
    Reset {
        branch: String,
        #[arg(long)]
        to: String,
    },
    /// Replace a path inside a mount with its version in a ref
    Restore {
        path: PathBuf,
        #[arg(long)]
        at: String,
    },
    #[command(subcommand)]
    Cache(CacheCommand),
    /// Collect garbage: drop old auto-commits, list what's unreachable, and delete what an
    /// earlier run (at least a day ago) listed and is still unreachable
    Gc {
        /// Report what would happen, change nothing
        #[arg(long)]
        dry_run: bool,
        /// How old a dead list must be before it's swept (tests and benchmarks)
        #[arg(long, hide = true)]
        grace_secs: Option<u64>,
    },
    /// Check that everything reachable (or reachable from one ref) exists and hash-verifies;
    /// downloads it all
    Fsck { spec: Option<String> },
    /// Run a mount's FUSE session (started by `ctm mount`)
    #[command(hide = true)]
    MountProcess { state_dir: PathBuf },
}

#[derive(Subcommand)]
enum BranchCommand {
    List {
        /// List branches deleted in the last 7 days instead (they can be undeleted)
        #[arg(long)]
        deleted: bool,
    },
    /// Delete a branch; `ctm branch undelete` brings it back within 7 days
    Delete { name: String },
    /// Restore a branch deleted in the last 7 days
    Undelete { name: String },
    /// Rename a branch; merges and forks still recognize it
    Rename { old: String, new: String },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Create {
        name: String,
        #[arg(long)]
        from: Option<String>,
    },
    List {
        /// List snapshots deleted in the last 7 days instead (they can be undeleted)
        #[arg(long)]
        deleted: bool,
    },
    /// Delete a snapshot; `ctm snapshot undelete` brings it back within 7 days
    Delete { name: String },
    /// Restore a snapshot deleted in the last 7 days
    Undelete { name: String },
}

#[derive(Subcommand)]
enum CacheCommand {
    Stats {
        #[arg(long)]
        json: bool,
    },
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .without_time()
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ctm: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(cli.command)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ctm: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(command: Command) -> Result<()> {
    match command {
        Command::Init { url, endpoint } => commands::init(&url, endpoint.as_deref()).await,
        Command::Import {
            dir,
            branch,
            message,
        } => commands::import(&dir, &branch, &message).await,
        Command::Export { spec, dir } => commands::export(&spec, &dir).await,
        Command::Ls { spec } => commands::ls(&spec).await,
        Command::Cat { spec } => commands::cat(&spec).await,
        Command::Fork { from, new } => commands::fork(&from, &new).await,
        Command::Branch(BranchCommand::List { deleted }) => commands::branch_list(deleted).await,
        Command::Branch(BranchCommand::Delete { name }) => commands::branch_delete(&name).await,
        Command::Branch(BranchCommand::Undelete { name }) => commands::branch_undelete(&name).await,
        Command::Branch(BranchCommand::Rename { old, new }) => {
            commands::branch_rename(&old, &new).await
        }
        Command::Reset { branch, to } => commands::reset(&branch, &to).await,
        Command::Snapshot(SnapshotCommand::Create { name, from }) => {
            commands::snapshot_create(&name, from.as_deref()).await
        }
        Command::Snapshot(SnapshotCommand::List { deleted }) => {
            commands::snapshot_list(deleted).await
        }
        Command::Snapshot(SnapshotCommand::Delete { name }) => {
            commands::snapshot_delete(&name).await
        }
        Command::Snapshot(SnapshotCommand::Undelete { name }) => {
            commands::snapshot_undelete(&name).await
        }
        Command::Log { spec, path } => commands::log(&spec, path.as_deref()).await,
        Command::Diff { a, b, stat } => commands::diff(&a, &b, stat).await,
        Command::Mount {
            spec,
            dir,
            read_only,
            foreground,
        } => mount::mount(&spec, &dir, read_only, foreground).await,
        Command::MountProcess { state_dir } => mount::run(state_dir).await,
        Command::Cache(CacheCommand::Stats { json }) => mount::cache_stats(json).await,
        Command::Gc {
            dry_run,
            grace_secs,
        } => commands::gc(dry_run, grace_secs).await,
        Command::Fsck { spec } => commands::fsck(spec.as_deref()).await,
        Command::Merge {
            src,
            into,
            ours,
            theirs,
            keep_both,
            dry_run,
        } => {
            let resolve = match (ours, theirs, keep_both) {
                (true, _, _) => Some(ctm_repo::Resolve::Ours),
                (_, true, _) => Some(ctm_repo::Resolve::Theirs),
                (_, _, true) => Some(ctm_repo::Resolve::KeepBoth),
                _ => None,
            };
            commands::merge(&src, &into, resolve, dry_run).await
        }
        Command::Unmount { dir, no_wait } => mount::unmount(&dir, no_wait).await,
        Command::Sync { dir } => mount::sync_cmd(dir.as_deref()).await,
        Command::Commit { dir, message } => mount::commit_cmd(&dir, &message).await,
        Command::Status { dir } => mount::status(&dir).await,
        Command::Restore { path, at } => mount::restore(&path, &at).await,
    }
}
