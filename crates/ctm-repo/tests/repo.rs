//! Repo operations against MemBackend, file://, and (with CTM_TEST_S3_URL) an S3 server.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use ctm_core::diff::Change;
use ctm_core::{CommitKind, Id};
use ctm_repo::{BranchName, Error, Identity, InitOutcome, RefSpec, Repo};
use ctm_store::faulty::Faults;
use ctm_store::{Backend, ETag, FaultyBackend, FileBackend, MemBackend, PutMode};

fn identity() -> Identity {
    Identity {
        user: "me".into(),
        hostname: "laptop".into(),
        machine_id: [7; 16],
    }
}

fn name(s: &str) -> BranchName {
    BranchName::new(s).unwrap()
}

fn spec(s: &str) -> RefSpec {
    s.parse().unwrap()
}

async fn repo_on(be: Arc<dyn Backend>) -> Repo {
    Repo::init(be, identity()).await.unwrap().0
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

fn set_mtime(path: &Path, secs: u64) {
    let t = SystemTime::UNIX_EPOCH + Duration::new(secs, 123_456_789);
    fs::File::open(path).unwrap().set_modified(t).unwrap();
}

/// A tree with every kind of entry `import` handles, including the size edge cases.
fn sample_tree(root: &Path) {
    fs::create_dir_all(root.join("src/deep/er")).unwrap();
    fs::create_dir_all(root.join("empty")).unwrap();
    fs::write(root.join("empty.txt"), b"").unwrap();
    fs::write(root.join("inline_max.bin"), random_bytes(1, 4096)).unwrap();
    fs::write(root.join("one_chunk.bin"), random_bytes(2, 4097)).unwrap();
    fs::write(root.join("big.bin"), random_bytes(3, 9 << 20)).unwrap();
    fs::write(root.join("src/main.rs"), b"fn main() {}\n").unwrap();
    fs::write(root.join("src/deep/er/x.txt"), b"deep").unwrap();
    fs::write(root.join("run.sh"), b"#!/bin/sh\n").unwrap();
    fs::set_permissions(root.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
    fs::set_permissions(root.join("src/main.rs"), fs::Permissions::from_mode(0o600)).unwrap();
    symlink("src/main.rs", root.join("link")).unwrap();
    symlink("../nowhere", root.join("dangling")).unwrap();
    for f in ["empty.txt", "big.bin", "src/main.rs", "run.sh"] {
        set_mtime(&root.join(f), 1_700_000_000);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Node {
    File {
        mode: u32,
        mtime: (i64, i64),
        data: Vec<u8>,
    },
    Dir {
        mode: u32,
    },
    Link(Vec<u8>),
}

/// Everything import/export must preserve, keyed by relative path.
fn snapshot(root: &Path) -> BTreeMap<String, Node> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in fs::read_dir(&dir).unwrap() {
            let e = e.unwrap();
            let p = e.path();
            let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            let m = fs::symlink_metadata(&p).unwrap();
            let node = if m.file_type().is_symlink() {
                Node::Link(
                    fs::read_link(&p)
                        .unwrap()
                        .into_os_string()
                        .into_encoded_bytes(),
                )
            } else if m.is_dir() {
                stack.push(p.clone());
                Node::Dir {
                    mode: m.mode() & 0o7777,
                }
            } else {
                Node::File {
                    mode: m.mode() & 0o7777,
                    mtime: (m.mtime(), m.mtime_nsec()),
                    data: fs::read(&p).unwrap(),
                }
            };
            out.insert(rel, node);
        }
    }
    out
}

async fn round_trip(be: Arc<dyn Backend>) {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let repo = repo_on(be).await;
    let imported = repo
        .import(src.path(), &name("main"), "first import")
        .await
        .unwrap();
    let commit = imported.commit;
    let dst = tempfile::tempdir().unwrap();
    let out = dst.path().join("out");
    repo.export(&spec("main"), &out).await.unwrap();
    assert_eq!(snapshot(src.path()), snapshot(&out));

    // A path inside a ref, and a commit ID prefix.
    let sub = dst.path().join("sub");
    repo.export(&spec("main:src/deep"), &sub).await.unwrap();
    assert_eq!(fs::read(sub.join("er/x.txt")).unwrap(), b"deep");
    let by_id = dst.path().join("by_id");
    repo.export(&spec(&commit.to_hex()[..12]), &by_id)
        .await
        .unwrap();
    assert_eq!(snapshot(src.path()), snapshot(&by_id));
}

#[tokio::test]
async fn import_export_round_trips_on_mem() {
    round_trip(Arc::new(MemBackend::new())).await;
}

#[tokio::test]
async fn import_export_round_trips_on_file() {
    let dir = tempfile::tempdir().unwrap();
    round_trip(Arc::new(FileBackend::new(dir.path()))).await;
}

#[tokio::test]
async fn import_export_round_trips_on_s3() {
    let Ok(url) = std::env::var("CTM_TEST_S3_URL") else {
        eprintln!("skipped: CTM_TEST_S3_URL is not set");
        return;
    };
    let endpoint = std::env::var("CTM_TEST_S3_ENDPOINT").ok();
    let url = format!("{url}/repo-{}", std::process::id());
    round_trip(ctm_store::open(&url, endpoint.as_deref()).unwrap()).await;
}

#[tokio::test]
async fn init_creates_then_connects() {
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let (a, outcome) = Repo::init(be.clone(), identity()).await.unwrap();
    assert_eq!(outcome, InitOutcome::Created);
    let (b, outcome) = Repo::init(be.clone(), identity()).await.unwrap();
    assert_eq!(outcome, InitOutcome::Connected);
    assert_eq!(a.config(), b.config());
    assert_eq!(
        Repo::open(be, identity()).await.unwrap().config(),
        a.config()
    );
}

#[tokio::test]
async fn refuses_newer_format() {
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let repo = repo_on(be.clone()).await;
    let mut config = serde_json::to_value(repo.config()).unwrap();
    config["format_version"] = 2.into();
    be.put(
        "config",
        serde_json::to_vec(&config).unwrap().into(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    assert!(matches!(
        Repo::open(be, identity()).await,
        Err(Error::UnsupportedFormat(2))
    ));
}

#[tokio::test]
async fn open_without_init_fails() {
    assert!(
        Repo::open(Arc::new(MemBackend::new()), identity())
            .await
            .is_err()
    );
}

/// Counts calls, to check how much work operations do.
#[derive(Default)]
struct Counting {
    inner: MemBackend,
    gets: AtomicUsize,
    heads: AtomicUsize,
    puts: AtomicUsize,
    lists: AtomicUsize,
}

impl Counting {
    fn reset(&self) {
        for c in [&self.gets, &self.heads, &self.puts, &self.lists] {
            c.store(0, Ordering::SeqCst);
        }
    }
    fn counts(&self) -> [usize; 4] {
        [&self.gets, &self.heads, &self.puts, &self.lists].map(|c| c.load(Ordering::SeqCst))
    }
}

#[async_trait]
impl Backend for Counting {
    async fn get(&self, key: &str) -> ctm_store::Result<(Bytes, ETag)> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key).await
    }
    async fn head(&self, key: &str) -> ctm_store::Result<Option<ETag>> {
        self.heads.fetch_add(1, Ordering::SeqCst);
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: Bytes, mode: PutMode) -> ctm_store::Result<ETag> {
        self.puts.fetch_add(1, Ordering::SeqCst);
        self.inner.put(key, body, mode).await
    }
    async fn list(&self, prefix: &str) -> ctm_store::Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> ctm_store::Result<()> {
        self.inner.delete(key).await
    }
}

#[tokio::test]
async fn fork_costs_one_get_and_one_put_whatever_the_size() {
    for files in [3, 300] {
        let src = tempfile::tempdir().unwrap();
        for i in 0..files {
            fs::write(src.path().join(format!("f{i}")), format!("{i}")).unwrap();
        }
        let be = Arc::new(Counting::default());
        let repo = repo_on(be.clone()).await;
        repo.import(src.path(), &name("main"), "").await.unwrap();
        be.reset();
        let forked = repo.fork(&spec("main"), &name("agent-1")).await.unwrap();
        assert_eq!(
            be.counts(),
            [1, 0, 1, 0],
            "gets, heads, puts, lists ({files} files)"
        );
        let (main, _) = repo.read_ref(&name("main")).await.unwrap();
        assert_eq!((forked.head, forked.log), (main.head, main.log));
        let from = forked.forked_from.unwrap();
        assert_eq!((from.from.as_str(), from.commit), ("main", main.head));
    }
}

#[tokio::test]
async fn fork_refuses_an_existing_name() {
    let repo = repo_with_file("a", "1").await;
    repo.fork(&spec("main"), &name("x")).await.unwrap();
    assert!(matches!(
        repo.fork(&spec("main"), &name("x")).await,
        Err(Error::AlreadyExists(_))
    ));
}

async fn repo_with_file(path: &str, data: &str) -> Repo {
    let src = tempfile::tempdir().unwrap();
    fs::write(src.path().join(path), data).unwrap();
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    repo.import(src.path(), &name("main"), "import")
        .await
        .unwrap();
    repo
}

async fn import_files(repo: &Repo, branch: &str, files: &[(&str, &str)], msg: &str) -> Id {
    let src = tempfile::tempdir().unwrap();
    for (p, d) in files {
        let p = src.path().join(p);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, d).unwrap();
    }
    repo.import(src.path(), &name(branch), msg)
        .await
        .unwrap()
        .commit
}

#[tokio::test]
async fn fork_from_a_snapshot_or_commit_starts_a_fresh_log() {
    let repo = repo_with_file("a", "1").await;
    let c2 = import_files(&repo, "main", &[("a", "2")], "second").await;
    repo.snapshot(&name("v1"), &spec("main")).await.unwrap();

    let from_snap = repo.fork(&spec("snap/v1"), &name("s")).await.unwrap();
    assert_eq!(from_snap.head, c2);
    assert_eq!(from_snap.forked_from.unwrap().from, "snap/v1");
    let log = repo.log(&spec("s"), None).await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!((log[0].commit, log[0].message.as_str()), (c2, "second"));

    let prefix = &c2.to_hex()[..10];
    let from_commit = repo.fork(&spec(prefix), &name("c")).await.unwrap();
    assert_eq!(from_commit.forked_from.unwrap().from, c2.to_hex());
    assert_eq!(repo.log(&spec("c"), None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn snapshots_are_create_only() {
    let repo = repo_with_file("a", "1").await;
    repo.snapshot(&name("v1"), &spec("main")).await.unwrap();
    assert!(matches!(
        repo.snapshot(&name("v1"), &spec("main")).await,
        Err(Error::AlreadyExists(_))
    ));
    assert_eq!(repo.list_snapshots().await.unwrap(), [name("v1")]);
}

#[tokio::test]
async fn log_is_newest_first_and_filters_by_path() {
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    let c1 = import_files(&repo, "main", &[("a", "1"), ("b", "1")], "one").await;
    let c2 = import_files(&repo, "main", &[("a", "2"), ("b", "1")], "two").await;
    let c3 = import_files(&repo, "main", &[("a", "2"), ("b", "2")], "three").await;
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(
        log.iter().map(|e| e.commit).collect::<Vec<_>>(),
        [c3, c2, c1]
    );
    assert!(log.iter().all(|e| e.kind == CommitKind::Import));
    let only_a = repo.log(&spec("main"), Some(b"a")).await.unwrap();
    assert_eq!(
        only_a.iter().map(|e| e.commit).collect::<Vec<_>>(),
        [c2, c1]
    );
}

#[tokio::test]
async fn log_spans_segments() {
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    let mut commits = Vec::new();
    for i in 0..1003 {
        let (r, etag) = match repo.read_ref(&name("main")).await {
            Ok(x) => x,
            Err(_) => {
                commits.push(import_files(&repo, "main", &[("a", "0")], "0").await);
                continue;
            }
        };
        // Appending through import is slow for 1000 commits; append a log entry directly.
        let next = repo
            .append_log(&r, r.head, CommitKind::Manual, &format!("{i}"))
            .await
            .unwrap();
        repo.cas_ref(&name("main"), &next, &etag).await.unwrap();
        commits.push(r.head);
    }
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(log.len(), 1003);
    assert_eq!(log.last().unwrap().message, "0");
}

#[tokio::test]
async fn diff_lists_changed_paths_only() {
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    let base: Vec<(&str, &str)> = vec![
        ("keep/a", "1"),
        ("keep/deep/b", "1"),
        ("mod/c", "1"),
        ("gone", "1"),
    ];
    import_files(&repo, "main", &base, "").await;
    repo.fork(&spec("main"), &name("other")).await.unwrap();
    import_files(
        &repo,
        "other",
        &[
            ("keep/a", "1"),
            ("keep/deep/b", "1"),
            ("mod/c", "2"),
            ("new", "1"),
        ],
        "",
    )
    .await;
    let changes = repo.diff(&spec("main"), &spec("other")).await.unwrap();
    let summary: Vec<(String, &str)> = changes
        .iter()
        .map(|c| {
            let kind = match c.change {
                Change::Added(_) => "added",
                Change::Removed(_) => "removed",
                Change::Modified { .. } => "modified",
            };
            (String::from_utf8(c.path.clone()).unwrap(), kind)
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("gone".to_string(), "removed"),
            ("mod/c".to_string(), "modified"),
            ("new".to_string(), "added"),
        ]
    );
}

#[tokio::test]
async fn cas_has_exactly_one_winner() {
    let repo = repo_with_file("a", "1").await;
    let (r, etag) = repo.read_ref(&name("main")).await.unwrap();
    let mut a = r.clone();
    a.updated_by = "a".into();
    let mut b = r.clone();
    b.updated_by = "b".into();
    repo.cas_ref(&name("main"), &a, &etag).await.unwrap();
    assert!(repo.cas_ref(&name("main"), &b, &etag).await.is_err());
}

#[tokio::test]
async fn reimport_uploads_no_new_chunks() {
    let src = tempfile::tempdir().unwrap();
    fs::write(src.path().join("big"), random_bytes(9, 5 << 20)).unwrap();
    let be = Arc::new(Counting::default());
    let repo = repo_on(be.clone()).await;
    repo.import(src.path(), &name("main"), "").await.unwrap();
    let chunks_before = be.inner.list("chunks/").await.unwrap().len();
    be.reset();
    repo.import(src.path(), &name("main"), "").await.unwrap();
    assert_eq!(be.inner.list("chunks/").await.unwrap().len(), chunks_before);
    // Only the commit, the log segment, and the ref are new.
    assert_eq!(be.counts()[2], 3, "puts");
}

#[tokio::test]
async fn names_are_validated() {
    for ok in [
        "main",
        "agent-1",
        "main.desktop",
        "a_b",
        "v1.2",
        "deadbee",
        "x".repeat(100).as_str(),
    ] {
        assert!(BranchName::new(ok).is_ok(), "{ok}");
    }
    for bad in [
        "",
        ".hidden",
        "-x",
        "a..b",
        "a/b",
        "a b",
        "deadbeef",
        "0123456789abcdef",
        "é",
        "x".repeat(101).as_str(),
    ] {
        assert!(BranchName::new(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn ref_syntax() {
    use ctm_repo::refspec::RefTarget;
    let s: RefSpec = "main".parse().unwrap();
    assert_eq!((s.target, s.path), (RefTarget::Branch(name("main")), None));
    let s: RefSpec = "snap/v1:a/b.txt".parse().unwrap();
    assert_eq!(
        (s.target, s.path),
        (RefTarget::Snapshot(name("v1")), Some("a/b.txt".into()))
    );
    let s: RefSpec = "7f3a9c1e20b4".parse().unwrap();
    assert_eq!(s.target, RefTarget::CommitPrefix("7f3a9c1e20b4".into()));
    let s: RefSpec = "main:".parse().unwrap();
    assert_eq!(s.path, None);
    for bad in ["", "7f3A9c1e", "snap/", "main:/abs", "a..b", ":x"] {
        assert!(bad.parse::<RefSpec>().is_err(), "{bad:?}");
    }
}

#[tokio::test]
async fn check_passes_and_catches_missing_objects() {
    let be = Arc::new(MemBackend::new());
    let repo = repo_on(be.clone()).await;
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    repo.import(src.path(), &name("main"), "").await.unwrap();
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
    let victim = be.list("chunks/").await.unwrap().remove(0);
    be.delete(&victim).await.unwrap();
    let problems = ctm_repo::check(&repo).await.unwrap();
    assert_eq!(problems.len(), 1, "{problems:?}");
}

/// A crash at any point during an import leaves a consistent repo: the branch is at the old
/// or the new commit, and every object it references exists.
#[tokio::test]
async fn a_crash_at_any_put_leaves_the_repo_consistent() {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let mut n = 0;
    loop {
        let be = Arc::new(FaultyBackend::new(MemBackend::new(), Faults::default()));
        let repo = repo_on(be.clone()).await;
        let old = import_files(&repo, "main", &[("a", "1")], "old").await;
        let fresh = Arc::new(FaultyBackend::new(
            MemBackend::new(),
            Faults {
                crash_after_puts: Some(n),
                ..Faults::default()
            },
        ));
        // Copy the repo into a backend that will crash after `n` more puts.
        for key in be.list("").await.unwrap() {
            let (body, _) = be.get(&key).await.unwrap();
            fresh
                .inner()
                .put(&key, body, PutMode::Overwrite)
                .await
                .unwrap();
        }
        let repo = Repo::open(fresh.clone(), identity()).await.unwrap();
        let result = repo.import(src.path(), &name("main"), "new").await;
        fresh.heal();
        assert_eq!(
            ctm_repo::check(&repo).await.unwrap(),
            Vec::<String>::new(),
            "crash after {n} puts"
        );
        let (r, _) = repo.read_ref(&name("main")).await.unwrap();
        match result {
            Ok(new) => {
                assert_eq!(r.head, new.commit);
                break;
            }
            Err(_) => assert_eq!(r.head, old, "crash after {n} puts"),
        }
        n += 1;
    }
    assert!(n > 5, "the import should take several puts");
}

#[tokio::test]
async fn import_skips_special_files_and_counts_them() {
    let src = tempfile::tempdir().unwrap();
    fs::write(src.path().join("keep"), "1").unwrap();
    let status = std::process::Command::new("mkfifo")
        .arg(src.path().join("fifo"))
        .status()
        .unwrap();
    assert!(status.success());
    let _socket = std::os::unix::net::UnixListener::bind(src.path().join("sock")).unwrap();
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    let imported = repo.import(src.path(), &name("main"), "").await.unwrap();
    assert_eq!(imported.skipped, 2);
    let names: Vec<_> = repo
        .ls(&spec("main"))
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.name)
        .collect();
    assert_eq!(names, [b"keep".to_vec()]);
}
