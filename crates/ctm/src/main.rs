use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

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
    /// Commit, then unmount
    Unmount {
        dir: PathBuf,
        /// Unmount without committing; the working state is kept for the next mount
        #[arg(long)]
        no_commit: bool,
    },
    /// Commit a mount's changes and push them
    Commit {
        dir: PathBuf,
        #[arg(short, long, default_value = "")]
        message: String,
    },
    /// Show a mount's branch, base commit, dirty files, and whether it is behind
    Status { dir: PathBuf },
    /// Create a new branch from any ref
    Fork { from: String, new: String },
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
    /// Replace a path inside a mount with its version in a ref
    Restore {
        path: PathBuf,
        #[arg(long)]
        at: String,
    },
    #[command(subcommand)]
    Cache(CacheCommand),
    /// Run a mount's FUSE session (started by `ctm mount`)
    #[command(hide = true)]
    MountProcess { state_dir: PathBuf },
}

#[derive(Subcommand)]
enum BranchCommand {
    List,
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Create {
        name: String,
        #[arg(long)]
        from: Option<String>,
    },
    List,
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
        Command::Branch(BranchCommand::List) => commands::branch_list().await,
        Command::Snapshot(SnapshotCommand::Create { name, from }) => {
            commands::snapshot_create(&name, from.as_deref()).await
        }
        Command::Snapshot(SnapshotCommand::List) => commands::snapshot_list().await,
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
        Command::Unmount { dir, no_commit } => mount::unmount(&dir, no_commit).await,
        Command::Commit { dir, message } => mount::commit_cmd(&dir, &message).await,
        Command::Status { dir } => mount::status(&dir).await,
        Command::Restore { path, at } => mount::restore(&path, &at).await,
    }
}
