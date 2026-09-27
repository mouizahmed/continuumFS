//! The `ctm` binary end to end, with its own HOME and XDG directories.

mod common;

use std::fs;

use common::{Env, write};

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
