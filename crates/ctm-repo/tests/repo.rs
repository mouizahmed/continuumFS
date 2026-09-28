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
use futures::StreamExt;

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
    config["format_version"] = 3.into();
    be.put(
        "config",
        serde_json::to_vec(&config).unwrap().into(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    assert!(matches!(
        Repo::open(be, identity()).await,
        Err(Error::UnsupportedFormat(3))
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
    got: std::sync::Mutex<Vec<String>>,
}

impl Counting {
    fn reset(&self) {
        for c in [&self.gets, &self.heads, &self.puts, &self.lists] {
            c.store(0, Ordering::SeqCst);
        }
        self.got.lock().unwrap().clear();
    }
    /// Keys read (and `LIST <prefix>`es) so far that start with `prefix`.
    fn got(&self, prefix: &str) -> usize {
        let got = self.got.lock().unwrap();
        got.iter().filter(|k| k.starts_with(prefix)).count()
    }
    fn counts(&self) -> [usize; 4] {
        [&self.gets, &self.heads, &self.puts, &self.lists].map(|c| c.load(Ordering::SeqCst))
    }
}

#[async_trait]
impl Backend for Counting {
    async fn get(&self, key: &str) -> ctm_store::Result<(Bytes, ETag)> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.got.lock().unwrap().push(key.to_string());
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
        self.got.lock().unwrap().push(format!("LIST {prefix}"));
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
            .append_log(
                &r,
                vec![ctm_core::LogEntry {
                    time_ns: i as i64,
                    commit: r.head,
                    kind: CommitKind::Manual,
                    message: format!("{i}"),
                }],
            )
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
    let packs_before = be.inner.list("packs/data/").await.unwrap().len();
    be.reset();
    repo.import(src.path(), &name("main"), "").await.unwrap();
    assert_eq!(
        be.inner.list("packs/data/").await.unwrap().len(),
        packs_before
    );
    // Only the commit and log segment are new: one meta pack, its index segment, and the ref.
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
    let victim = be.list("packs/data/").await.unwrap().remove(0);
    be.delete(&victim).await.unwrap();
    let problems = ctm_repo::check(&repo).await.unwrap();
    assert!(!problems.is_empty());
    assert!(
        problems.iter().all(|p| p.starts_with("chunk ")),
        "{problems:?}"
    );
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
    // A data pack, a meta pack, an index segment, and the ref.
    assert_eq!(n, 4, "puts per import");
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

#[tokio::test(flavor = "multi_thread")]
async fn files_sharing_chunks_upload_them_once() {
    // Two identical 6 MiB files are imported concurrently: each chunk is PUT once.
    let src = tempfile::tempdir().unwrap();
    let data = random_bytes(4, 6 << 20);
    fs::write(src.path().join("a.bin"), &data).unwrap();
    fs::write(src.path().join("b.bin"), &data).unwrap();
    // With network-like latency, both files' uploads of a chunk overlap.
    let be = Arc::new(FaultyBackend::new(
        MemBackend::new(),
        Faults {
            latency: std::time::Duration::from_millis(20),
            ..Faults::default()
        },
    ));
    let repo = repo_on(be.clone()).await;
    let imported = repo.import(src.path(), &name("main"), "").await.unwrap();
    let data_packs: u64 = futures_len(be.inner(), "packs/data/").await;
    // Each chunk once (6 MiB plus pack framing), plus the page, list, tree, commit, and log
    // segment.
    assert!(data_packs < (6 << 20) + 4096, "{data_packs}");
    assert!(
        imported.uploaded.bytes < data_packs + 64 * 1024,
        "{:?}",
        imported.uploaded
    );
}

async fn futures_len(be: &MemBackend, prefix: &str) -> u64 {
    let mut total = 0;
    for key in be.list(prefix).await.unwrap() {
        total += be.get(&key).await.unwrap().0.len() as u64;
    }
    total
}

/// Every entry `import` writes has a file ID, in range and distinct.
#[tokio::test]
async fn import_gives_every_entry_a_file_id() {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let repo = repo_on(Arc::new(MemBackend::new())).await;
    let commit = repo
        .import(src.path(), &name("main"), "")
        .await
        .unwrap()
        .commit;
    let root = repo
        .get::<ctm_core::Commit>(&commit)
        .await
        .unwrap()
        .root_tree;
    let mut ids = std::collections::HashSet::new();
    let mut stack = vec![root];
    while let Some(t) = stack.pop() {
        for e in repo.get::<ctm_core::Tree>(&t).await.unwrap().entries {
            let id = e.file_id.expect("imported entries have file IDs");
            assert!((ctm_core::FILE_ID_MIN..ctm_core::FILE_ID_END).contains(&id));
            assert!(ids.insert(id), "duplicate file ID");
            if let ctm_core::Content::Dir(d) = e.content {
                stack.push(d);
            }
        }
    }
    assert!(ids.len() > 10);
}

/// A repo written by format version 1 (v0.1) is still readable, and the first write raises
/// its config to version 2.
#[tokio::test]
async fn a_version_1_repo_is_read_and_upgraded_on_first_write() {
    use ctm_core::encoding::encode_legacy_tree;
    use ctm_core::{Commit, Content, DirEntry, LogEntry, LogSegment, ObjectType, Tree};
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let repo = repo_on(be.clone()).await;
    // Hand-build what v0.1 wrote: a version-1 config, a LegacyTree, a commit, a log, a ref.
    let mut config = serde_json::to_value(repo.config()).unwrap();
    config["format_version"] = 1.into();
    be.put(
        "config",
        serde_json::to_vec(&config).unwrap().into(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    let repo = Repo::open(be.clone(), identity()).await.unwrap();
    assert_eq!(repo.format_version(), 1);
    let tree = Tree {
        entries: vec![DirEntry {
            name: b"old.txt".to_vec(),
            mode: 0o644,
            mtime_ns: 1,
            size: 3,
            content: Content::Inline(b"old".to_vec()),
            btime_ns: None,
            xattrs: None,
            file_id: None,
        }],
    };
    let payload = encode_legacy_tree(&tree);
    let tree_id = Id::compute(repo.key(), ObjectType::LegacyTree, &payload);
    let mut stored = vec![ObjectType::LegacyTree as u8, 0];
    stored.extend_from_slice(&payload);
    be.put(
        &format!("meta/{tree_id}"),
        stored.into(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    let commit = Commit {
        root_tree: tree_id,
        time_ns: 1,
        author: "me@old".into(),
        machine_id: [0; 16],
        kind: CommitKind::Import,
        message: "v0.1".into(),
    };
    let commit_enc = ctm_core::Encoded::new(repo.key(), &commit);
    let log = LogSegment {
        prev: None,
        entries: vec![LogEntry {
            time_ns: 1,
            commit: commit_enc.id,
            kind: CommitKind::Import,
            message: "v0.1".into(),
        }],
    };
    let log_enc = ctm_core::Encoded::new(repo.key(), &log);
    let (head, log_id) = (commit_enc.id, log_enc.id);
    repo.put_objects(vec![commit_enc, log_enc]).await.unwrap();
    repo.create_ref(
        &name("main"),
        &ctm_repo::BranchRef {
            head,
            log: log_id,
            head_hint: None,
            log_hint: None,
            forked_from: None,
            updated_at: "2026-09-27T00:00:00Z".into(),
            updated_by: "me".into(),
        },
    )
    .await
    .unwrap();

    // Readable as is.
    let dst = tempfile::tempdir().unwrap();
    repo.export(&spec("main"), &dst.path().join("v1"))
        .await
        .unwrap();
    assert_eq!(fs::read(dst.path().join("v1/old.txt")).unwrap(), b"old");

    // The first write upgrades the config; old and new data stay readable.
    import_files(&repo, "main", &[("new.txt", "new")], "v0.2").await;
    assert_eq!(repo.format_version(), 2);
    let reopened = Repo::open(be.clone(), identity()).await.unwrap();
    assert_eq!(reopened.config().format_version, 2);
    let log = reopened.log(&spec("main"), None).await.unwrap();
    let first = log.last().unwrap().commit.to_hex();
    reopened
        .export(&spec(&first), &dst.path().join("again"))
        .await
        .unwrap();
    assert_eq!(fs::read(dst.path().join("again/old.txt")).unwrap(), b"old");
    assert_eq!(
        ctm_repo::check(&reopened).await.unwrap(),
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn another_handle_finds_packed_objects_through_the_index() {
    // 40 MiB of chunks fills more than one 32 MiB data pack.
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    fs::write(src.path().join("large.bin"), random_bytes(5, 40 << 20)).unwrap();
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let writer = repo_on(be.clone()).await;
    let imported = writer.import(src.path(), &name("main"), "").await.unwrap();
    assert_eq!(be.list("packs/data/").await.unwrap().len(), 2);
    assert_eq!(be.list("packs/meta/").await.unwrap().len(), 1);
    assert_eq!(be.list("index/").await.unwrap().len(), 1);
    assert!(be.list("chunks/").await.unwrap().is_empty());
    assert!(be.list("meta/").await.unwrap().is_empty());

    // A fresh handle (another machine) syncs the index on its first miss.
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    let out = tempfile::tempdir().unwrap();
    reader
        .export(&spec("main"), &out.path().join("x"))
        .await
        .unwrap();
    assert_eq!(snapshot(&out.path().join("x")), snapshot(src.path()));
    let prefix = &imported.commit.to_hex()[..12];
    assert_eq!(
        reader.resolve(&spec(prefix)).await.unwrap().commit,
        imported.commit
    );
}

#[tokio::test]
async fn the_index_mirror_persists_between_opens() {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let be = Arc::new(Counting::default());
    repo_on(be.clone())
        .await
        .import(src.path(), &name("main"), "")
        .await
        .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let db = cache.path().join("index.db");
    let open = || async {
        Repo::open(be.clone(), identity())
            .await
            .unwrap()
            .with_index_at(&db)
            .unwrap()
    };
    be.reset();
    open().await.sync_index().await.unwrap();
    assert_eq!(be.got("index/"), 1);
    be.reset();
    open().await.sync_index().await.unwrap();
    assert_eq!(be.got("index/"), 0);
}

/// R3: refs and objects carry hints, so another machine reads any branch, fork, or snapshot
/// straight from the packs, without listing or reading the index.
#[tokio::test]
async fn a_fresh_handle_reads_by_hints_without_the_index() {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let be = Arc::new(Counting::default());
    let writer = repo_on(be.clone()).await;
    writer.import(src.path(), &name("main"), "").await.unwrap();
    writer.fork(&spec("main"), &name("feature")).await.unwrap();
    writer.snapshot(&name("v1"), &spec("main")).await.unwrap();
    for target in ["main", "feature", "snap/v1"] {
        be.reset();
        let reader = Repo::open(be.clone(), identity()).await.unwrap();
        let out = tempfile::tempdir().unwrap();
        reader
            .export(&spec(target), &out.path().join("x"))
            .await
            .unwrap();
        assert_eq!(snapshot(&out.path().join("x")), snapshot(src.path()));
        assert_eq!(be.got("index/"), 0, "{target}");
        assert_eq!(be.got("LIST index/"), 0, "{target}");
        assert_eq!(
            be.got("meta/") + be.got("chunks/"),
            0,
            "no loose-key misses"
        );
    }
}

#[tokio::test]
async fn a_stale_hint_falls_back_to_the_index() {
    let src = tempfile::tempdir().unwrap();
    sample_tree(src.path());
    let be = Arc::new(Counting::default());
    repo_on(be.clone())
        .await
        .import(src.path(), &name("main"), "")
        .await
        .unwrap();
    // Point the head's hint at the log segment's entry: the hash won't match.
    let (body, _) = be.inner.get("refs/branches/main").await.unwrap();
    let mut r: serde_json::Value = serde_json::from_slice(&body).unwrap();
    r["head_hint"] = r["log_hint"].clone();
    be.inner
        .put(
            "refs/branches/main",
            serde_json::to_vec(&r).unwrap().into(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    be.reset();
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    let out = tempfile::tempdir().unwrap();
    reader
        .export(&spec("main"), &out.path().join("x"))
        .await
        .unwrap();
    assert_eq!(snapshot(&out.path().join("x")), snapshot(src.path()));
    assert_eq!(be.got("LIST index/"), 1);
}

#[tokio::test]
async fn a_second_import_on_another_handle_skips_stored_chunks() {
    let src = tempfile::tempdir().unwrap();
    fs::write(src.path().join("big"), random_bytes(9, 5 << 20)).unwrap();
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    repo_on(be.clone())
        .await
        .import(src.path(), &name("main"), "")
        .await
        .unwrap();
    let other = Repo::open(be.clone(), identity()).await.unwrap();
    other.import(src.path(), &name("copy"), "").await.unwrap();
    assert_eq!(be.list("packs/data/").await.unwrap().len(), 1);
}

#[tokio::test]
async fn loose_objects_from_before_packs_are_still_read() {
    use ctm_core::{Chunk, Encoded};
    let be: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let repo = repo_on(be.clone()).await;
    let chunk = Chunk(b"written by v0.1".to_vec());
    let enc = Encoded::new(repo.key(), &chunk);
    be.put(
        &format!("chunks/{}", enc.id),
        enc.to_stored().into(),
        PutMode::Overwrite,
    )
    .await
    .unwrap();
    let read: Chunk = repo.get(&enc.id).await.unwrap();
    assert_eq!(read.0, chunk.0);
}

/// The chunks of a file on `main`, in order (reading the list and pages learns their hints).
async fn chunks_of(repo: &Repo, path: &str) -> Vec<ctm_core::ChunkRef> {
    use ctm_core::{ChunkList, ChunkPage, Content};
    let root = repo.resolve(&spec("main")).await.unwrap().root_tree;
    let entry = repo
        .entry_at(root, Some(path.as_bytes()))
        .await
        .unwrap()
        .unwrap();
    let Content::ChunkList(list) = entry.content else {
        panic!("{path} isn't chunked");
    };
    let mut out = Vec::new();
    for p in repo.get::<ChunkList>(&list).await.unwrap().pages {
        out.extend(repo.get::<ChunkPage>(&p.id).await.unwrap().chunks);
    }
    out
}

#[tokio::test]
async fn chunks_stored_side_by_side_come_with_one_get_per_span() {
    let src = tempfile::tempdir().unwrap();
    let data = random_bytes(6, 40 << 20);
    fs::write(src.path().join("big"), &data).unwrap();
    let be = Arc::new(Counting::default());
    repo_on(be.clone())
        .await
        .import(src.path(), &name("main"), "")
        .await
        .unwrap();
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    let chunks = chunks_of(&reader, "big").await;
    assert!(chunks.len() > 20);
    be.reset();
    let ids: Vec<Id> = chunks.iter().map(|c| c.id).collect();
    let mut fetched: std::collections::HashMap<Id, Vec<u8>> = reader
        .fetch_chunks(&ids, 16 << 20)
        .map(|(id, chunk)| (id, chunk.unwrap().0))
        .collect()
        .await;
    let joined: Vec<u8> = ids
        .iter()
        .flat_map(|id| fetched.remove(id).unwrap())
        .collect();
    assert_eq!(joined, data);
    // 40 MiB in spans of at most 16 MiB, plus one more where the data pack filled at 32 MiB.
    let gets = be.counts()[0];
    assert!((3..=5).contains(&gets), "{gets} GETs");
}

#[tokio::test]
async fn part_of_a_chunk_is_one_ranged_get() {
    use ctm_core::{Chunk, Encoded};
    let src = tempfile::tempdir().unwrap();
    let data = random_bytes(7, 3 << 20);
    fs::write(src.path().join("big"), &data).unwrap();
    let be = Arc::new(Counting::default());
    repo_on(be.clone())
        .await
        .import(src.path(), &name("main"), "")
        .await
        .unwrap();
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    let chunks = chunks_of(&reader, "big").await;
    let c = chunks[1];
    let start = u64::from(chunks[0].len) as usize;
    be.reset();
    let got = reader.chunk_range(&c.id, 1000..70_000).await.unwrap();
    assert_eq!(&got[..], &data[start + 1000..start + 70_000]);
    assert_eq!(be.counts()[0], 1);
    // A chunk from before packs: a range of its loose object.
    let loose = Chunk(random_bytes(8, 100_000));
    let enc = Encoded::new(reader.key(), &loose);
    be.inner
        .put(
            &format!("chunks/{}", enc.id),
            enc.to_stored().into(),
            PutMode::Overwrite,
        )
        .await
        .unwrap();
    let got = reader.chunk_range(&enc.id, 5..10).await.unwrap();
    assert_eq!(&got[..], &loose.0[5..10]);
}

#[tokio::test]
async fn outgoing_packs_stay_local_until_flushed_and_survive_a_restart() {
    use ctm_core::{Chunk, Encoded};
    let be = Arc::new(Counting::default());
    repo_on(be.clone()).await;
    let out = tempfile::tempdir().unwrap();
    let local = || async {
        Repo::open(be.clone(), identity())
            .await
            .unwrap()
            .with_outgoing(out.path())
            .unwrap()
    };
    let repo = local().await;
    let chunks: Vec<Chunk> = (0..3u64)
        .map(|i| Chunk(random_bytes(20 + i, 300_000)))
        .collect();
    let encoded: Vec<Encoded> = chunks.iter().map(|c| Encoded::new(repo.key(), c)).collect();
    let ids: Vec<Id> = encoded.iter().map(|e| e.id).collect();
    be.reset();
    repo.put_objects(encoded).await.unwrap();
    repo.seal().await.unwrap();
    assert_eq!(be.counts()[2], 0, "nothing is uploaded before a flush");
    assert!(repo.has_unpushed());
    for (id, c) in ids.iter().zip(&chunks) {
        assert_eq!(&repo.get::<Chunk>(id).await.unwrap(), c);
    }
    // A half-written pack from a crash is dropped; the sealed one is taken over.
    fs::write(out.path().join("0123.tmp"), b"partial").unwrap();
    fs::write(out.path().join("4567.pack"), b"not a pack").unwrap();
    drop(repo);
    let repo = local().await;
    assert_eq!(fs::read_dir(out.path()).unwrap().count(), 1);
    assert_eq!(&repo.get::<Chunk>(&ids[1]).await.unwrap(), &chunks[1]);
    let got = repo.chunk_range(&ids[2], 10..20).await.unwrap();
    assert_eq!(&got[..], &chunks[2].0[10..20]);
    // A flush pushes the pack and its index segment, then the local file goes.
    repo.flush().await.unwrap();
    assert_eq!(be.counts()[2], 2);
    assert!(!repo.has_unpushed());
    assert_eq!(fs::read_dir(out.path()).unwrap().count(), 0);
    let elsewhere = Repo::open(be.clone(), identity()).await.unwrap();
    assert_eq!(&elsewhere.get::<Chunk>(&ids[0]).await.unwrap(), &chunks[0]);
}
