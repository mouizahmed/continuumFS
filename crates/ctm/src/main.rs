use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

// Used by the commands as they land (M1–M3).
#[allow(dead_code)]
mod config;
#[allow(dead_code)]
mod control;
#[allow(dead_code)]
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
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("RUST_LOG"))
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
        Command::Init { .. } => todo!("M1: ctm init"),
        Command::Import { .. } => todo!("M1: ctm import"),
        Command::Export { .. } => todo!("M1: ctm export"),
        Command::Ls { .. } => todo!("M1: ctm ls"),
        Command::Cat { .. } => todo!("M1: ctm cat"),
        Command::Fork { .. } => todo!("M1: ctm fork"),
        Command::Branch(BranchCommand::List) => todo!("M1: ctm branch list"),
        Command::Snapshot(_) => todo!("M1: ctm snapshot"),
        Command::Log { .. } => todo!("M1: ctm log"),
        Command::Diff { .. } => todo!("M1: ctm diff"),
        Command::Mount { .. } => todo!("M2: ctm mount"),
        Command::MountProcess { .. } => todo!("M2: mount process"),
        Command::Cache(CacheCommand::Stats { .. }) => todo!("M2: ctm cache stats"),
        Command::Unmount { .. } => todo!("M3: ctm unmount"),
        Command::Commit { .. } => todo!("M3: ctm commit"),
        Command::Status { .. } => todo!("M3: ctm status"),
        Command::Restore { .. } => todo!("M3: ctm restore"),
    }
}
