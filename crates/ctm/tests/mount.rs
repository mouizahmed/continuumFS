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
    // The commit is local; `ctm sync` waits for the background push.
    let out = env.ok(&["sync", &env.path("mnt")]);
    assert!(out.starts_with("Everything is pushed to main"), "{out}");
    assert!(!env.ok(&["status", &env.path("mnt")]).contains("Unpushed"));
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
fn uncommitted_work_survives_a_killed_mount_and_unpushed_commits_a_no_wait_unmount() {
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
    // Another branch can't take over the directory while its changes are uncommitted.
    env.ok(&["fork", "main", "other"]);
    let err = env.fails(&["mount", "other", &env.path("mnt")]);
    assert!(err.contains("uncommitted changes"), "{err}");
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(
        fs::read_to_string(mnt.join("wip.txt")).unwrap(),
        "work in progress\n"
    );

    // `--no-wait` commits and unmounts without waiting for the push; whatever wasn't pushed
    // yet is pushed by the next mount here.
    fs::write(mnt.join("more.txt"), "more\n").unwrap();
    env.ok(&["unmount", "--no-wait", &env.path("mnt")]);
    env.ok(&["mount", "main", &env.path("mnt")]);
    env.ok(&["unmount", &env.path("mnt")]);
    assert_eq!(env.ok(&["cat", "main:wip.txt"]), "work in progress\n");
    assert_eq!(env.ok(&["cat", "main:more.txt"]), "more\n");
    assert_eq!(
        mounts(&env),
        0,
        "everything is pushed, so no working state is kept"
    );
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
    env.ok(&["sync", &env.path("a")]);
    assert!(env.ok(&["commit", &env.path("b")]).starts_with("Committed"));
    // b's push loses the race and goes to a fork, which the mount follows.
    let out = env.ok(&["sync", &env.path("b")]);
    assert!(out.starts_with("Everything is pushed to main."), "{out}");
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

// M4: hardening.

fn errno_of<T>(r: std::io::Result<T>) -> Option<i32> {
    r.err().and_then(|e| e.raw_os_error())
}

#[test]
fn unsupported_calls_fail_with_the_documented_errors() {
    use rustix::fs::{CWD, FallocateFlags, FileType, Mode, RenameFlags};
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    let (a, b) = (mnt.join("notes.txt"), mnt.join("project/README.md"));

    // chown to yourself is a no-op; to anyone else, EPERM.
    let (uid, gid) = (
        rustix::process::getuid().as_raw(),
        rustix::process::getgid().as_raw(),
    );
    std::os::unix::fs::chown(&a, Some(uid), Some(gid)).unwrap();
    if uid != 0 {
        assert_eq!(
            errno_of(std::os::unix::fs::chown(&a, Some(uid + 1), None)),
            Some(1)
        );
    }
    // RENAME_EXCHANGE is refused; RENAME_NOREPLACE works.
    let exchange = rustix::fs::renameat_with(CWD, &a, CWD, &b, RenameFlags::EXCHANGE);
    assert_eq!(exchange.err().map(|e| e.raw_os_error()), Some(22), "EINVAL");
    let noreplace = rustix::fs::renameat_with(CWD, &a, CWD, &b, RenameFlags::NOREPLACE);
    assert_eq!(
        noreplace.err().map(|e| e.raw_os_error()),
        Some(17),
        "EEXIST"
    );
    rustix::fs::renameat_with(CWD, &a, CWD, mnt.join("moved.txt"), RenameFlags::NOREPLACE).unwrap();
    // fallocate, FIFOs, hard links, and xattrs aren't supported; mknod of a file is.
    let f = fs::OpenOptions::new()
        .write(true)
        .open(mnt.join("moved.txt"))
        .unwrap();
    let r = rustix::fs::fallocate(&f, FallocateFlags::empty(), 0, 4096);
    assert_eq!(r.err().map(|e| e.raw_os_error()), Some(95), "EOPNOTSUPP");
    rustix::fs::mknodat(
        CWD,
        mnt.join("plain"),
        FileType::RegularFile,
        Mode::from(0o644),
        0,
    )
    .unwrap();
    assert_eq!(fs::metadata(mnt.join("plain")).unwrap().len(), 0);
    let fifo = rustix::fs::mknodat(CWD, mnt.join("fifo"), FileType::Fifo, Mode::from(0o644), 0);
    assert_eq!(fifo.err().map(|e| e.raw_os_error()), Some(95), "EOPNOTSUPP");
    assert_eq!(
        errno_of(fs::hard_link(mnt.join("plain"), mnt.join("hard"))),
        Some(1)
    );
    let xattr = rustix::fs::setxattr(
        mnt.join("plain"),
        "user.x",
        b"1",
        rustix::fs::XattrFlags::empty(),
    );
    assert_eq!(xattr.err().map(|e| e.raw_os_error()), Some(95), "ENOTSUP");

    // Unmounting with a file open fails cleanly and leaves the mount working.
    let err = env.fails(&["unmount", &env.path("mnt")]);
    assert!(err.contains("busy"), "{err}");
    assert!(is_mounted(&mnt));
    assert!(fs::metadata(mnt.join("plain")).is_ok());
    drop(f);
    env.ok(&["unmount", &env.path("mnt")]);
}

fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

/// fsx-style: random pread/pwrite/truncate/fsync/reopen through the kernel, every read
/// checked against a model, with commits and remounts along the way. mmap isn't a v0 test
/// target. `CTM_FSX_OPS` sets the number of operations (default 5000).
#[test]
fn fsx_random_operations_match_a_model() {
    use std::os::unix::fs::FileExt;
    if !fuse_available() {
        return;
    }
    let ops: usize = std::env::var("CTM_FSX_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);
    let env = Env::new();
    env.init();
    let src = env.work.join("src");
    fs::create_dir(&src).unwrap();
    let mut model = random_bytes(11, 3 << 20);
    fs::write(src.join("fsx.bin"), &model).unwrap();
    env.ok(&["import", &env.path("src"), "--branch", "main"]);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    let path = mnt.join("fsx.bin");
    let open = || {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap()
    };
    let mut file = open();
    let mut rng = 0x5eed_u64;
    const MAX_LEN: u64 = 8 << 20;
    // About 100 commits and 20 remounts whatever the length: v0 has no GC, so every commit
    // stays in the bucket.
    let commit_every = (ops / 100).max(1000);
    let remount_every = (ops / 20).max(2500);
    for op in 0..ops {
        let r = xorshift(&mut rng);
        let len = model.len() as u64;
        match r % 100 {
            0..40 => {
                let at = xorshift(&mut rng) % MAX_LEN;
                let n = (xorshift(&mut rng) % 65536 + 1).min(MAX_LEN - at) as usize;
                let data = random_bytes(xorshift(&mut rng), n);
                file.write_all_at(&data, at).unwrap();
                if model.len() < at as usize + n {
                    model.resize(at as usize + n, 0);
                }
                model[at as usize..at as usize + n].copy_from_slice(&data);
            }
            40..80 => {
                let at = xorshift(&mut rng) % (len + 1);
                let n = (xorshift(&mut rng) % 262_144) as usize;
                let mut buf = vec![0; n];
                let mut got = 0;
                while got < n {
                    match file.read_at(&mut buf[got..], at + got as u64).unwrap() {
                        0 => break,
                        k => got += k,
                    }
                }
                let end = (at as usize + n).min(model.len());
                assert!(
                    buf[..got] == model[at as usize..end],
                    "op {op}: read({at}, {n}) differs"
                );
            }
            80..88 => {
                let n = xorshift(&mut rng) % MAX_LEN;
                file.set_len(n).unwrap();
                model.resize(n as usize, 0);
            }
            88..92 => file.sync_data().unwrap(),
            92..97 => {
                drop(file);
                file = open();
            }
            _ => {
                assert_eq!(
                    file.metadata().unwrap().len(),
                    model.len() as u64,
                    "op {op}"
                );
            }
        }
        if op % commit_every == commit_every - 1 {
            env.ok(&["commit", &env.path("mnt")]);
        }
        if op % remount_every == remount_every - 1 {
            drop(file);
            let args: &[&str] = if (op / remount_every) % 2 == 1 {
                &["unmount", "--no-wait", &env.path("mnt")]
            } else {
                &["unmount", &env.path("mnt")]
            };
            env.ok(args);
            env.ok(&["mount", "main", &env.path("mnt")]);
            file = open();
        }
    }
    drop(file);
    assert!(fs::read(&path).unwrap() == model, "final contents differ");
    env.ok(&["unmount", &env.path("mnt")]);
    let out = env.run(&["cat", "main:fsx.bin"]);
    assert!(out.stdout == model, "committed contents differ");
}

/// A real workload: clone this repo and build a crate inside a mount, then again after a
/// remount.
#[test]
fn git_clone_and_cargo_build_work_inside_a_mount() {
    if !fuse_available() {
        return;
    }
    let have = |tool: &str| {
        Command::new(tool)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    };
    if !have("git") || !have("cargo") {
        eprintln!("skipped: needs git and cargo");
        return;
    }
    let env = Env::new();
    env.init();
    let src = env.work.join("src");
    fs::create_dir(&src).unwrap();
    env.ok(&["import", &env.path("src"), "--branch", "main"]);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let run = |dir: &Path, cmd: &str, args: &[&str]| {
        let out = Command::new(cmd)
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{cmd} {args:?} failed:\n{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    run(
        &mnt,
        "git",
        &[
            "clone",
            "--quiet",
            "--no-hardlinks",
            workspace.to_str().unwrap(),
            "repo",
        ],
    );
    let repo = mnt.join("repo");
    assert_eq!(run(&repo, "git", &["status", "--porcelain"]), "");
    run(&repo, "git", &["fsck", "--no-progress"]);

    run(&mnt, "cargo", &["new", "--quiet", "--vcs", "none", "hello"]);
    let hello = mnt.join("hello");
    run(&hello, "cargo", &["build", "--quiet", "--offline"]);
    assert_eq!(run(&hello, "./target/debug/hello", &[]), "Hello, world!\n");

    env.ok(&["unmount", &env.path("mnt")]);
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(run(&repo, "git", &["status", "--porcelain"]), "");
    assert_eq!(run(&hello, "./target/debug/hello", &[]), "Hello, world!\n");
    run(&hello, "cargo", &["build", "--quiet", "--offline"]);
    env.ok(&["unmount", &env.path("mnt")]);
}

/// `kill -9` of the mount process in the middle of a commit, at a few different moments:
/// after a remount the branch is at the old commit or the new one, and committing again
/// never forks.
#[test]
fn kill_9_during_a_commit_recovers_without_a_fork() {
    if !fuse_available() {
        return;
    }
    let big = random_bytes(21, 48 << 20);
    for delay_ms in [30, 150, 600] {
        let env = Env::new();
        repo_with_main(&env);
        let mnt = env.work.join("mnt");
        fs::create_dir(&mnt).unwrap();
        env.ok(&["mount", "main", &env.path("mnt")]);
        fs::write(mnt.join("new.bin"), &big).unwrap();
        let pid = mount_pid(&env);
        let mut commit = Command::new(env!("CARGO_BIN_EXE_ctm"));
        commit
            .args(["commit", &env.path("mnt")])
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap())
            .env("HOME", &env.home)
            .env("XDG_CONFIG_HOME", env.home.join(".config"))
            .env("XDG_DATA_HOME", env.home.join(".local/share"))
            .env("XDG_RUNTIME_DIR", env.home.join("run"));
        let mut child = commit.spawn().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        kill_9(pid);
        let _ = child.wait();

        env.ok(&["mount", "main", &env.path("mnt")]);
        let out = env.ok(&["commit", &env.path("mnt")]);
        assert!(
            out.starts_with("Committed") || out.starts_with("Nothing to commit"),
            "after {delay_ms} ms: {out}"
        );
        assert_eq!(
            env.ok(&["branch", "list"]).trim(),
            "main",
            "after {delay_ms} ms"
        );
        assert_eq!(
            fs::metadata(mnt.join("new.bin")).unwrap().len(),
            big.len() as u64
        );
        env.ok(&["unmount", &env.path("mnt")]);
        assert!(
            env.run(&["cat", "main:new.bin"]).stdout == big,
            "after {delay_ms} ms"
        );
    }
}

#[test]
fn appending_to_a_large_file_downloads_at_most_one_chunk() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    env.init();
    let src = env.work.join("src");
    fs::create_dir(&src).unwrap();
    let mut data = random_bytes(31, 64 << 20);
    fs::write(src.join("big.bin"), &data).unwrap();
    env.ok(&["import", &env.path("src"), "--branch", "main"]);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);

    let mut f = fs::OpenOptions::new()
        .append(true)
        .open(mnt.join("big.bin"))
        .unwrap();
    assert_eq!(
        cache(&env)["fetched_bytes"],
        0,
        "opening read-write downloads nothing"
    );
    std::io::Write::write_all(&mut f, b"!").unwrap();
    drop(f);
    env.ok(&["commit", &env.path("mnt")]);
    let fetched = cache(&env)["fetched_bytes"].as_u64().unwrap();
    assert!(
        fetched <= 4 << 20,
        "{fetched} bytes downloaded to append one"
    );
    env.ok(&["unmount", &env.path("mnt")]);
    data.push(b'!');
    assert!(env.run(&["cat", "main:big.bin"]).stdout == data);
}

/// R9: a git index written in one mount stays valid in the next, so `git status` on a fresh
/// mount doesn't re-read the files.
#[test]
fn git_status_on_a_fresh_mount_reads_no_files() {
    if !fuse_available() {
        return;
    }
    if !Command::new("git")
        .arg("--version")
        .output()
        .is_ok_and(|o| o.status.success())
    {
        eprintln!("skipped: needs git");
        return;
    }
    let env = Env::new();
    env.init();
    fs::create_dir(env.work.join("src")).unwrap();
    env.ok(&["import", &env.path("src"), "--branch", "main"]);
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    let repo = mnt.join("repo");
    let git = |args: &[&str]| {
        let out = Command::new("git")
            .args(args)
            .current_dir(&repo)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap()
    };
    fs::create_dir(&repo).unwrap();
    git(&["init", "--quiet"]);
    for i in 0..60 {
        // Over 4 KiB each, so they're chunks: re-reading them would show up as downloads.
        fs::write(repo.join(format!("f{i}.bin")), random_bytes(i, 10_000)).unwrap();
    }
    // Let a second pass so no file is "racily clean" (modified in the index's second).
    std::thread::sleep(std::time::Duration::from_millis(1100));
    git(&["add", "."]);
    git(&["commit", "--quiet", "-m", "files"]);
    assert_eq!(git(&["status", "--porcelain"]), "");
    env.ok(&["unmount", &env.path("mnt")]);

    fs::remove_dir_all(env.home.join(".cache")).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(git(&["status", "--porcelain"]), "", "the tree is clean");
    let fetched = cache(&env)["fetched_bytes"].as_u64().unwrap();
    assert!(
        fetched < 200_000,
        "git status downloaded {fetched} bytes: it re-read the files"
    );
    env.ok(&["unmount", &env.path("mnt")]);
}

/// R2: a quiet mount commits on its own, and `fork` of a mounted branch first commits and
/// pushes what's been written there.
#[test]
fn quiet_mounts_commit_on_their_own_and_fork_includes_mounted_work() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let config = env.home.join(".config/continuum/config.toml");
    let text = fs::read_to_string(&config).unwrap();
    assert!(text.contains("quiet_secs = 5"), "{text}");
    fs::write(&config, text.replace("quiet_secs = 5", "quiet_secs = 1")).unwrap();
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);

    fs::write(mnt.join("auto.txt"), "committed by itself\n").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !env.ok(&["status", &env.path("mnt")]).contains("Changes: 0") {
        assert!(std::time::Instant::now() < deadline, "no auto-commit");
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    env.ok(&["sync", &env.path("mnt")]);
    assert_eq!(env.ok(&["cat", "main:auto.txt"]), "committed by itself\n");
    let log = env.ok(&["log", "main"]);
    assert_eq!(log.lines().count(), 2, "{log}");

    // Written just now, not committed yet: the fork still has it.
    fs::write(mnt.join("fresh.txt"), "just written\n").unwrap();
    env.ok(&["fork", "main", "copy"]);
    assert_eq!(env.ok(&["cat", "copy:fresh.txt"]), "just written\n");
    env.ok(&["unmount", &env.path("mnt")]);
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    for e in fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        total += if p.is_dir() {
            dir_bytes(&p)
        } else {
            fs::metadata(&p).unwrap().len()
        };
    }
    total
}

/// R6: mounts keep a record in the bucket while they run; old auto-commits leave the log, and
/// two `ctm gc` runs delete what only they referenced.
#[test]
fn gc_drops_old_auto_commits_and_deletes_what_only_they_referenced() {
    if !fuse_available() {
        return;
    }
    let env = Env::new();
    repo_with_main(&env);
    let config = env.home.join(".config/continuum/config.toml");
    let text = fs::read_to_string(&config).unwrap();
    let text = text
        .replace("quiet_secs = 5", "quiet_secs = 1")
        .replace("auto_days = 14", "auto_days = 0");
    fs::write(&config, text).unwrap();
    let bucket = Path::new(env.repo_url.strip_prefix("file://").unwrap()).to_path_buf();
    let mnt = env.work.join("mnt");
    fs::create_dir(&mnt).unwrap();
    env.ok(&["mount", "main", &env.path("mnt")]);
    assert_eq!(fs::read_dir(bucket.join("mounts")).unwrap().count(), 1);

    let wait_committed = || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !env.ok(&["status", &env.path("mnt")]).contains("Changes: 0") {
            assert!(std::time::Instant::now() < deadline, "no auto-commit");
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        env.ok(&["sync", &env.path("mnt")]);
    };
    fs::write(mnt.join("v.bin"), random_bytes(41, 3 << 20)).unwrap();
    wait_committed();
    let second = random_bytes(42, 3 << 20);
    fs::write(mnt.join("v.bin"), &second).unwrap();
    wait_committed();
    env.ok(&["unmount", &env.path("mnt")]);
    assert_eq!(fs::read_dir(bucket.join("mounts")).unwrap().count(), 0);

    let before = dir_bytes(&bucket);
    let first = env.ok(&["gc", "--grace-secs", "0"]);
    assert!(
        first.contains("Retention: 1 auto-commit dropped"),
        "{first}"
    );
    let second_run = env.ok(&["gc", "--grace-secs", "0"]);
    assert!(second_run.contains("Deleted:"), "{second_run}");
    let after = dir_bytes(&bucket);
    assert!(
        before - after >= 3 << 20,
        "{before} → {after}\n{second_run}"
    );
    assert!(env.run(&["cat", "main:v.bin"]).stdout == second);
    assert_eq!(env.ok(&["fsck"]).trim(), "No problems found");
    assert_eq!(env.ok(&["fsck", "main"]).trim(), "No problems found");
}
