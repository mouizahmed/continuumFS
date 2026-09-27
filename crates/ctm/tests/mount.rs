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
