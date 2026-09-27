//! The `ctm` binary end to end, with its own HOME and XDG directories.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    repo_url: String,
    work: PathBuf,
}

impl Env {
    fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let work = tmp.path().join("work");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&work).unwrap();
        let repo_url = format!("file://{}", tmp.path().join("bucket/ws").display());
        Env {
            home,
            repo_url,
            work,
            _tmp: tmp,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_ctm"))
            .args(args)
            .current_dir(&self.work)
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", &self.home)
            .env("USER", "tester")
            .env("XDG_CONFIG_HOME", self.home.join(".config"))
            .env("XDG_CACHE_HOME", self.home.join(".cache"))
            .env("XDG_DATA_HOME", self.home.join(".local/share"))
            .env("XDG_RUNTIME_DIR", self.home.join("run"))
            .output()
            .unwrap()
    }

    /// Runs a command that must succeed and returns its stdout.
    fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "ctm {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs a command that must fail and returns its stderr.
    fn fails(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(!out.status.success(), "ctm {args:?} succeeded");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(!err.contains("panicked"), "ctm {args:?} panicked:\n{err}");
        err
    }

    fn path(&self, rel: &str) -> String {
        self.work.join(rel).display().to_string()
    }
}

fn write(root: &Path, rel: &str, data: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, data).unwrap();
}

#[test]
fn init_creates_then_connects_and_records_the_default_repo() {
    let env = Env::new();
    let out = env.ok(&["init", &env.repo_url]);
    assert!(out.contains("Created"), "{out}");
    let out = env.ok(&["init", &env.repo_url]);
    assert!(out.contains("Connected"), "{out}");
    let config = fs::read_to_string(env.home.join(".config/continuum/config.toml")).unwrap();
    assert!(config.contains("default_repo = \"ws\""), "{config}");
    assert!(config.contains("machine_id"), "{config}");
    assert!(config.contains(&env.repo_url), "{config}");
}

#[test]
fn commands_need_a_repo_and_valid_urls() {
    let env = Env::new();
    let err = env.fails(&["branch", "list"]);
    assert!(err.contains("ctm init"), "{err}");
    let err = env.fails(&["init", "r2://bucket/ws"]);
    assert!(err.contains("s3://"), "{err}");
}

#[test]
fn bucket_commands_work_end_to_end() {
    let env = Env::new();
    env.ok(&["init", &env.repo_url]);
    let src = env.work.join("src");
    write(&src, "project/src/main.rs", "fn main() {}\n");
    write(&src, "notes.txt", "hello\n");

    let out = env.ok(&[
        "import",
        &env.path("src"),
        "--branch",
        "main",
        "-m",
        "first",
    ]);
    assert!(out.contains("main"), "{out}");

    let ls = env.ok(&["ls", "main"]);
    assert!(ls.contains("notes.txt") && ls.contains("project"), "{ls}");
    assert_eq!(env.ok(&["cat", "main:notes.txt"]), "hello\n");

    env.ok(&["export", "main", &env.path("out")]);
    assert_eq!(
        fs::read_to_string(env.work.join("out/project/src/main.rs")).unwrap(),
        "fn main() {}\n"
    );

    let out = env.ok(&["fork", "main", "agent-a"]);
    assert!(out.contains("Forked main → agent-a"), "{out}");
    let branches = env.ok(&["branch", "list"]);
    assert_eq!(branches.lines().collect::<Vec<_>>(), ["agent-a", "main"]);
    env.fails(&["fork", "main", "agent-a"]);

    write(&src, "notes.txt", "changed\n");
    env.ok(&[
        "import",
        &env.path("src"),
        "--branch",
        "agent-a",
        "-m",
        "edit",
    ]);
    let diff = env.ok(&["diff", "main", "agent-a"]);
    assert_eq!(diff.trim(), "modified: notes.txt");

    env.ok(&["snapshot", "create", "v1", "--from", "agent-a"]);
    assert_eq!(env.ok(&["snapshot", "list"]).trim(), "v1");
    assert_eq!(env.ok(&["cat", "snap/v1:notes.txt"]), "changed\n");

    let log = env.ok(&["log", "agent-a"]);
    let lines: Vec<_> = log.lines().collect();
    assert_eq!(lines.len(), 2, "{log}");
    assert!(
        lines[0].contains("edit") && lines[1].contains("first"),
        "{log}"
    );
    let commit = lines[1].split_whitespace().next().unwrap();
    assert_eq!(env.ok(&["cat", &format!("{commit}:notes.txt")]), "hello\n");
    let only_main_rs = env.ok(&["log", "agent-a", "--", "project/src/main.rs"]);
    assert_eq!(only_main_rs.lines().count(), 1, "{only_main_rs}");
}
