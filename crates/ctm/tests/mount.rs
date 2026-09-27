//! Read-only FUSE mounts end to end. Needs /dev/fuse and fusermount3; skipped otherwise.

mod common;

use std::fs;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use common::{Env, write};

fn fuse_available() -> bool {
    let ok = Path::new("/dev/fuse").exists()
        && Command::new("fusermount3")
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success());
    if !ok {
        eprintln!("skipped: needs /dev/fuse and fusermount3");
    }
    ok
}

fn is_mounted(path: &Path) -> bool {
    let info = fs::read_to_string("/proc/self/mountinfo").unwrap();
    let want = path.to_str().unwrap();
    info.lines().any(|l| l.split(' ').nth(4) == Some(want))
}

fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

/// A repo with `main` imported from a small tree plus a 24 MiB file.
fn repo_with_main(env: &Env) {
    env.init();
    let src = env.work.join("src");
    write(&src, "project/src/main.rs", "fn main() {}\n");
    write(&src, "project/README.md", "needle in a haystack\n");
    write(&src, "notes.txt", "hello\n");
    fs::write(src.join("big.bin"), random_bytes(7, 24 << 20)).unwrap();
    std::os::unix::fs::symlink("notes.txt", src.join("link")).unwrap();
    env.ok(&["import", &env.path("src"), "--branch", "main"]);
}

fn cache(env: &Env) -> serde_json::Value {
    serde_json::from_str(&env.ok(&["cache", "stats", "--json"])).unwrap()
}

/// `find | sha256sum` over a directory, as the M2 check says.
fn checksums(dir: &Path) -> String {
    let out = Command::new("sh")
        .arg("-c")
        .arg("find . \\( -type f -o -type l \\) | sort | xargs sha256sum")
        .current_dir(dir)
        .output()
        .unwrap();
    assert!(out.status.success());
    String::from_utf8(out.stdout).unwrap()
}

#[test]
fn a_read_only_mount_matches_the_source_and_reads_lazily() {
    if fuse_available() {
        read_only_mount(&Env::new());
    }
}

#[test]
fn a_read_only_mount_on_s3() {
    if fuse_available()
        && let Some(env) = Env::s3()
    {
        read_only_mount(&env);
    }
}

fn read_only_mount(env: &Env) {
    repo_with_main(env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt"), "--read-only"]);
    assert!(is_mounted(&mnt));

    // stat and ls download no file contents.
    assert_eq!(fs::metadata(mnt.join("big.bin")).unwrap().len(), 24 << 20);
    Command::new("ls").arg("-lR").arg(&mnt).output().unwrap();
    assert_eq!(cache(env)["bytes"], 0);

    // The first MiB downloads a few chunks, not the file.
    let mut head = vec![0; 1 << 20];
    fs::File::open(mnt.join("big.bin"))
        .unwrap()
        .read_exact(&mut head)
        .unwrap();
    assert_eq!(head, random_bytes(7, 1 << 20));
    let objects = cache(env)["objects"].as_u64().unwrap();
    assert!((1..=10).contains(&objects), "{objects} chunks for 1 MiB");

    assert_eq!(checksums(&mnt), checksums(&env.work.join("src")));
    let grep = Command::new("grep")
        .args(["-r", "needle"])
        .arg(&mnt)
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&grep.stdout).contains("README.md"));
    assert_eq!(
        fs::read_link(mnt.join("link")).unwrap(),
        Path::new("notes.txt")
    );

    let err = fs::write(mnt.join("new.txt"), "x").unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc_erofs()));

    env.ok(&["unmount", &env.path("mnt")]);
    assert!(!is_mounted(&mnt));
    assert_eq!(fs::read_dir(&mnt).unwrap().count(), 0);
}

fn libc_erofs() -> i32 {
    30 // EROFS on Linux
}

fn mount_pid(env: &Env) -> u32 {
    let dir = env.home.join(".local/share/continuum/mounts");
    let state = fs::read_dir(dir).unwrap().next().unwrap().unwrap().path();
    let record: serde_json::Value =
        serde_json::from_slice(&fs::read(state.join("mount.json")).unwrap()).unwrap();
    record["pid"].as_u64().unwrap() as u32
}

fn kill_9(pid: u32) {
    let status = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    // Wait for the process to go.
    while Path::new(&format!("/proc/{pid}")).exists() {
        std::thread::yield_now();
    }
}

#[test]
fn a_mount_left_by_a_dead_process_is_cleared() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();

    // Mounting over a dead mount replaces it.
    env.ok(&["mount", "main", &env.path("mnt"), "--read-only"]);
    let err = env.fails(&["mount", "main", &env.path("mnt"), "--read-only"]);
    assert!(err.contains("already mounted"), "{err}");
    kill_9(mount_pid(&env));
    env.ok(&["mount", "main", &env.path("mnt"), "--read-only"]);
    assert_eq!(
        fs::read_to_string(mnt.join("notes.txt")).unwrap(),
        "hello\n"
    );

    // Unmounting a dead mount clears it.
    kill_9(mount_pid(&env));
    env.ok(&["unmount", &env.path("mnt")]);
    assert!(!is_mounted(&mnt));
    assert!(
        fs::read_dir(env.home.join(".local/share/continuum/mounts"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn snapshots_mount_read_only_without_the_flag() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    env.ok(&["snapshot", "create", "v1", "--from", "main"]);
    let mnt = env.work.join("snap");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "snap/v1", &env.path("snap")]);
    assert_eq!(
        fs::read_to_string(mnt.join("notes.txt")).unwrap(),
        "hello\n"
    );
    let err = fs::write(mnt.join("notes.txt"), "x").unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc_erofs()));
    env.ok(&["unmount", &env.path("snap")]);
}

fn mounts(env: &Env) -> usize {
    fs::read_dir(env.home.join(".local/share/continuum/mounts"))
        .map(|d| d.count())
        .unwrap_or(0)
}

#[test]
fn a_read_write_mount_commits_and_survives_remounts() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);

    // Edits through the kernel: append, create, nested dirs, rename, delete, symlink.
    let mut notes = fs::OpenOptions::new()
        .append(true)
        .open(mnt.join("notes.txt"))
        .unwrap();
    std::io::Write::write_all(&mut notes, b"more\n").unwrap();
    drop(notes);
    fs::create_dir_all(mnt.join("d/e")).unwrap();
    fs::write(mnt.join("d/e/f.txt"), "deep\n").unwrap();
    fs::rename(
        mnt.join("project/src/main.rs"),
        mnt.join("project/src/lib.rs"),
    )
    .unwrap();
    fs::remove_file(mnt.join("big.bin")).unwrap();
    std::os::unix::fs::symlink("notes.txt", mnt.join("link2")).unwrap();
    let status = env.ok(&["status", &env.path("mnt")]);
    // notes.txt, f.txt, lib.rs, link2, and the deleted main.rs and big.bin.
    assert!(status.contains("Changes: 6"), "{status}");

    let out = env.ok(&["commit", &env.path("mnt"), "-m", "edits"]);
    assert!(out.starts_with("Committed"), "{out}");
    assert_eq!(env.ok(&["cat", "main:notes.txt"]), "hello\nmore\n");
    assert_eq!(env.ok(&["cat", "main:d/e/f.txt"]), "deep\n");
    assert!(env.run(&["cat", "main:big.bin"]).status.code() != Some(0));

    // Unmounting commits whatever is left.
    fs::write(mnt.join("late.txt"), "late\n").unwrap();
    let before = checksums(&mnt);
    env.ok(&["unmount", &env.path("mnt")]);
    assert_eq!(env.ok(&["cat", "main:late.txt"]), "late\n");
    assert_eq!(mounts(&env), 0, "a clean unmount leaves no working state");
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(checksums(&mnt), before);
    env.ok(&["unmount", &env.path("mnt")]);
}

#[test]
fn uncommitted_work_survives_a_killed_mount_and_no_commit_unmounts() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);

    // Not fsynced: the bytes sit in staging and the extent map in state.db.
    fs::write(mnt.join("wip.txt"), "work in progress\n").unwrap();
    kill_9(mount_pid(&env));
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(
        fs::read_to_string(mnt.join("wip.txt")).unwrap(),
        "work in progress\n"
    );

    env.ok(&["unmount", "--no-commit", &env.path("mnt")]);
    assert!(env.run(&["cat", "main:wip.txt"]).status.code() != Some(0));
    // Another branch can't take over the directory while its changes are uncommitted.
    env.ok(&["fork", "main", "other"]);
    let err = env.fails(&["mount", "other", &env.path("mnt")]);
    assert!(err.contains("uncommitted changes"), "{err}");
    env.ok(&["mount", "main", &env.path("mnt")]);
    env.ok(&["unmount", &env.path("mnt")]);
    assert_eq!(env.ok(&["cat", "main:wip.txt"]), "work in progress\n");
}

#[test]
fn two_mounts_of_one_branch_race_and_one_auto_forks() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let (a, b) = (env.work.join("a"), env.work.join("b"));
    fs::create_dir(&a).unwrap();
    fs::create_dir(&b).unwrap();
    env.ok(&["mount", "main", &env.path("a")]);
    env.ok(&["mount", "main", &env.path("b")]);
    fs::write(a.join("notes.txt"), "from a\n").unwrap();
    fs::write(b.join("notes.txt"), "from b\n").unwrap();
    assert!(env.ok(&["commit", &env.path("a")]).starts_with("Committed"));
    let out = env.ok(&["commit", &env.path("b")]);
    assert!(
        out.contains("main moved on another machine. Your changes are safe on main."),
        "{out}"
    );
    let branches = env.ok(&["branch", "list"]);
    assert_eq!(branches.lines().count(), 2, "{branches}");
    let fork = branches.lines().find(|l| *l != "main").unwrap().to_string();
    assert_eq!(env.ok(&["cat", "main:notes.txt"]), "from a\n");
    assert_eq!(env.ok(&["cat", &format!("{fork}:notes.txt")]), "from b\n");
    let status = env.ok(&["status", &env.path("b")]);
    assert!(
        status.contains(&format!("Branch: {fork} (auto-forked from main)")),
        "{status}"
    );
    env.ok(&["unmount", &env.path("a")]);
    env.ok(&["unmount", &env.path("b")]);
}

#[test]
fn restore_replaces_a_path_inside_a_mount() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    // Read it first, so the kernel caches the old contents.
    assert_eq!(
        fs::read_to_string(mnt.join("notes.txt")).unwrap(),
        "hello\n"
    );
    fs::write(mnt.join("notes.txt"), "broken\n").unwrap();
    assert_eq!(
        fs::read_to_string(mnt.join("notes.txt")).unwrap(),
        "broken\n"
    );
    env.ok(&["restore", &env.path("mnt/notes.txt"), "--at", "main"]);
    assert_eq!(
        fs::read_to_string(mnt.join("notes.txt")).unwrap(),
        "hello\n"
    );
    env.ok(&["unmount", &env.path("mnt")]);
}
