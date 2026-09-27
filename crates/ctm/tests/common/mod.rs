//! A `ctm` binary with its own HOME and XDG directories.

#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

pub struct Env {
    pub _tmp: tempfile::TempDir,
    pub home: PathBuf,
    pub repo_url: String,
    /// S3-compatible endpoint, for repos on the S3 test server.
    pub endpoint: Option<String>,
    pub work: PathBuf,
}

impl Env {
    pub fn new() -> Env {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let work = tmp.path().join("work");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&work).unwrap();
        let repo_url = format!("file://{}", tmp.path().join("bucket/ws").display());
        Env {
            home,
            repo_url,
            endpoint: None,
            work,
            _tmp: tmp,
        }
    }

    /// An environment whose repo is on the S3 test server, if `CTM_TEST_S3_URL` is set.
    pub fn s3() -> Option<Env> {
        let Ok(url) = std::env::var("CTM_TEST_S3_URL") else {
            eprintln!("skipped: CTM_TEST_S3_URL is not set");
            return None;
        };
        let mut env = Env::new();
        let unique = env.work.to_string_lossy().replace('/', "-");
        env.repo_url = format!("{url}/cli{unique}");
        env.endpoint = std::env::var("CTM_TEST_S3_ENDPOINT").ok();
        Some(env)
    }

    /// `ctm init` for this environment's repo.
    pub fn init(&self) -> String {
        match &self.endpoint {
            Some(e) => self.ok(&["init", &self.repo_url, "--endpoint", e]),
            None => self.ok(&["init", &self.repo_url]),
        }
    }

    pub fn run(&self, args: &[&str]) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_ctm"));
        cmd.env_clear();
        // S3 credentials pass through; everything else is this environment's own.
        for (k, v) in std::env::vars_os() {
            if k.to_string_lossy().starts_with("AWS_") {
                cmd.env(k, v);
            }
        }
        cmd.args(args)
            .current_dir(&self.work)
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
    pub fn ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "ctm {args:?} failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    }

    /// Runs a command that must fail and returns its stderr.
    pub fn fails(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(!out.status.success(), "ctm {args:?} succeeded");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(!err.contains("panicked"), "ctm {args:?} panicked:\n{err}");
        err
    }

    pub fn path(&self, rel: &str) -> String {
        self.work.join(rel).display().to_string()
    }
}

pub fn write(root: &Path, rel: &str, data: &str) {
    let p = root.join(rel);
    fs::create_dir_all(p.parent().unwrap()).unwrap();
    fs::write(p, data).unwrap();
}
