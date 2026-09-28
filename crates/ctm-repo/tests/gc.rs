//! Garbage collection and retention (R6), against MemBackend.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ctm_core::{CommitKind, Id, LogEntry};
use ctm_repo::{BranchName, GcOptions, Identity, RefSpec, Repo};
use ctm_store::{Backend, MemBackend};

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

/// No grace periods: a second run deletes what the first one listed.
fn now_opts() -> GcOptions {
    GcOptions {
        auto_retention: Duration::ZERO,
        grace: Duration::ZERO,
        ..GcOptions::default()
    }
}

/// Imports `files` (name, bytes) into `branch` and returns the commit.
async fn import(repo: &Repo, branch: &str, files: &[(&str, &[u8])]) -> Id {
    let dir = tempfile::tempdir().unwrap();
    for (n, data) in files {
        fs::write(dir.path().join(n), data).unwrap();
    }
    repo.import(dir.path(), &name(branch), "")
        .await
        .unwrap()
        .commit
}

/// Files in a ref: name → bytes.
async fn export(repo: &Repo, r: &str) -> BTreeMap<String, Vec<u8>> {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("x");
    repo.export(&spec(r), &out).await.unwrap();
    read_dir(&out)
}

fn read_dir(d: &Path) -> BTreeMap<String, Vec<u8>> {
    fs::read_dir(d)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            (
                p.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read(&p).unwrap(),
            )
        })
        .collect()
}

/// Replaces `branch`'s log with these commits as ancient auto-commits, then its head.
async fn prepend_old_autos(repo: &Repo, branch: &str, commits: &[Id]) {
    use ctm_core::{Encoded, LogSegment};
    let (r, etag) = repo.read_ref(&name(branch)).await.unwrap();
    let mut entries: Vec<LogEntry> = commits
        .iter()
        .map(|c| LogEntry {
            time_ns: 1,
            commit: *c,
            kind: CommitKind::Auto,
            message: String::new(),
        })
        .collect();
    entries.push(LogEntry {
        time_ns: 2,
        commit: r.head,
        kind: CommitKind::Manual,
        message: "head".into(),
    });
    let seg = Encoded::new(
        repo.key(),
        &LogSegment {
            prev: None,
            entries,
        },
    );
    let log = seg.id;
    repo.put_objects(vec![seg]).await.unwrap();
    let next = ctm_repo::BranchRef { log, ..r };
    repo.cas_ref(&name(branch), &next, &etag).await.unwrap();
}

async fn drop_branch(be: &dyn Backend, branch: &str) {
    be.delete(&format!("refs/branches/{branch}")).await.unwrap();
}

async fn bucket_bytes(be: &dyn Backend) -> u64 {
    let mut total = 0;
    for key in be.list("").await.unwrap() {
        total += be.get(&key).await.unwrap().0.len() as u64;
    }
    total
}

#[tokio::test]
async fn unreachable_data_is_listed_then_deleted_a_run_later() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let (a, b) = (random_bytes(1, 3 << 20), random_bytes(2, 3 << 20));
    let old_a = import(&repo, "scratch-a", &[("a.bin", &a)]).await;
    let old_b = import(&repo, "scratch-b", &[("b.bin", &b)]).await;
    import(&repo, "main", &[("keep.txt", b"keep")]).await;
    prepend_old_autos(&repo, "main", &[old_a, old_b]).await;
    drop_branch(be.as_ref(), "scratch-a").await;
    drop_branch(be.as_ref(), "scratch-b").await;
    let before = bucket_bytes(be.as_ref()).await;

    // The first run drops the old auto-commits and lists what's unreachable; nothing goes.
    let first = repo.gc(&now_opts()).await.unwrap();
    assert_eq!(first.dropped_commits, 2);
    assert!(first.newly_dead > 0);
    assert_eq!(first.deleted_objects, 0);
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(log.len(), 1, "only the head stays");

    // The second deletes it, and everything still reachable reads back.
    let second = repo.gc(&now_opts()).await.unwrap();
    assert!(second.deleted_packs >= 2, "{second:?}");
    assert!(second.freed_bytes >= 6 << 20, "{second:?}");
    let after = bucket_bytes(be.as_ref()).await;
    assert!(before - after >= 6 << 20, "{before} → {after}");
    assert_eq!(export(&repo, "main").await["keep.txt"], b"keep");
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
    assert_eq!(
        be.list("index/")
            .await
            .unwrap()
            .iter()
            .filter(|k| !k.starts_with("index/dead/"))
            .count(),
        1
    );

    // A fresh reader works from the compacted index alone.
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    assert_eq!(export(&reader, "main").await["keep.txt"], b"keep");
    assert_eq!(repo.gc(&now_opts()).await.unwrap().deleted_objects, 0);
}

#[tokio::test]
async fn snapshots_and_recent_mount_records_keep_their_commits() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let snap = import(&repo, "s", &[("s.bin", &random_bytes(3, 1 << 20))]).await;
    repo.snapshot(&name("v1"), &spec("s")).await.unwrap();
    let mounted = import(&repo, "m", &[("m.bin", &random_bytes(4, 1 << 20))]).await;
    repo.save_mount_record("abc", "m", mounted).await.unwrap();
    drop_branch(be.as_ref(), "s").await;
    drop_branch(be.as_ref(), "m").await;
    for _ in 0..2 {
        repo.gc(&now_opts()).await.unwrap();
    }
    assert_eq!(
        repo.resolve(&spec(&snap.to_hex())).await.unwrap().commit,
        snap
    );
    assert!(export(&repo, "snap/v1").await.contains_key("s.bin"));
    let tree = repo
        .resolve(&spec(&mounted.to_hex()))
        .await
        .unwrap()
        .root_tree;
    assert!(repo.entry_at(tree, Some(b"m.bin")).await.unwrap().is_some());
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());

    // Once the record goes, its commit is collected.
    repo.delete_mount_record("abc").await.unwrap();
    for _ in 0..2 {
        repo.gc(&now_opts()).await.unwrap();
    }
    let fresh = Repo::open(be.clone(), identity()).await.unwrap();
    assert!(fresh.resolve(&spec(&mounted.to_hex())).await.is_err());
}

#[tokio::test]
async fn mostly_dead_packs_are_repacked_and_old_readers_follow() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let small = random_bytes(5, 400_000);
    let big = random_bytes(6, 3 << 20);
    // One import: both files' chunks share a data pack. Then big.bin goes, and the commit
    // that had it becomes an old auto-commit, so retention drops it.
    let both = import(&repo, "main", &[("small.bin", &small), ("big.bin", &big)]).await;
    import(&repo, "main", &[("small.bin", &small)]).await;
    prepend_old_autos(&repo, "main", &[both]).await;
    // A reader that synced its index before the GC runs.
    let reader = Repo::open(be.clone(), identity()).await.unwrap();
    reader.sync_index().await.unwrap();

    let first = repo.gc(&now_opts()).await.unwrap();
    assert!(first.dropped_commits >= 1, "{first:?}");
    let second = repo.gc(&now_opts()).await.unwrap();
    // The data pack held both files: it's repacked with small.bin only.
    assert_eq!(second.repacked_packs, 1, "{second:?}");
    assert!(second.freed_bytes >= 3 << 20, "{second:?}");
    assert_eq!(export(&repo, "main").await["small.bin"], small);
    assert_eq!(export(&reader, "main").await["small.bin"], small);
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn writers_never_rely_on_dead_objects() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let data = random_bytes(7, 2 << 20);
    import(&repo, "old", &[("x.bin", &data)]).await;
    drop_branch(be.as_ref(), "old").await;
    repo.gc(&now_opts()).await.unwrap();

    // An import after the dead list: the same bytes are uploaded again, not deduplicated.
    let writer = Repo::open(be.clone(), identity()).await.unwrap();
    let before = writer.uploaded();
    import(&writer, "new", &[("x.bin", &data)]).await;
    assert!((writer.uploaded() - before).bytes >= 2 << 20);
    for _ in 0..2 {
        repo.gc(&now_opts()).await.unwrap();
    }
    assert_eq!(export(&repo, "new").await["x.bin"], data);
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn a_push_referencing_a_dead_object_uploads_it_again() {
    use ctm_core::{Chunk, Commit, Content, DirEntry, Encoded, Tree};
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let data = random_bytes(8, 300_000);
    import(&repo, "old", &[("x.bin", &data)]).await;
    let chunk = Encoded::new(repo.key(), &Chunk(data.clone())).id;
    // A writer that knew the chunk before it died builds a tree on it...
    let writer = Repo::open(be.clone(), identity()).await.unwrap();
    writer.sync_index().await.unwrap();
    drop_branch(be.as_ref(), "old").await;
    repo.gc(&now_opts()).await.unwrap();
    // ...and pushes after seeing the dead list: the chunk goes up again with it.
    writer.sync_index().await.unwrap();
    let tree = Tree {
        entries: vec![DirEntry {
            name: b"x.bin".to_vec(),
            mode: 0o644,
            mtime_ns: 1,
            size: data.len() as u64,
            content: Content::Chunk(chunk),
            btime_ns: None,
            xattrs: None,
            file_id: Some(ctm_repo::new_file_id()),
        }],
    };
    let tree_enc = Encoded::new(writer.key(), &tree);
    let commit = Commit {
        root_tree: tree_enc.id,
        time_ns: 5,
        author: "me@laptop".into(),
        machine_id: [7; 16],
        kind: CommitKind::Manual,
        message: String::new(),
    };
    let commit_enc = Encoded::new(writer.key(), &commit);
    let commit_id = commit_enc.id;
    writer
        .put_objects(vec![tree_enc, commit_enc])
        .await
        .unwrap();
    let before = writer.uploaded();
    writer
        .snapshot(&name("kept"), &spec(&commit_id.to_hex()))
        .await
        .unwrap();
    assert!(
        (writer.uploaded() - before).bytes >= 300_000,
        "the chunk was uploaded again"
    );
    for _ in 0..2 {
        repo.gc(&now_opts()).await.unwrap();
    }
    assert_eq!(export(&repo, "snap/kept").await["x.bin"], data);
}

#[tokio::test]
async fn one_run_at_a_time_and_a_stale_lease_is_taken_over() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    import(&repo, "main", &[("a", b"1")]).await;
    be.put(
        "locks/gc",
        serde_json::to_vec(&serde_json::json!({
            "run_id": "x", "started_at": ctm_repo::rfc3339(ctm_repo::now_ns()), "by": "other"
        }))
        .unwrap()
        .into(),
        ctm_store::PutMode::Overwrite,
    )
    .await
    .unwrap();
    let err = repo.gc(&now_opts()).await.unwrap_err();
    assert!(err.to_string().contains("GC run"), "{err}");
    let stale = GcOptions {
        lock_stale: Duration::ZERO,
        ..now_opts()
    };
    repo.gc(&stale).await.unwrap();
    assert!(be.list("locks/").await.unwrap().is_empty());
}

#[tokio::test]
async fn a_dry_run_changes_nothing() {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    let old = import(&repo, "scratch", &[("a.bin", &random_bytes(9, 1 << 20))]).await;
    import(&repo, "main", &[("b", b"2")]).await;
    prepend_old_autos(&repo, "main", &[old]).await;
    drop_branch(be.as_ref(), "scratch").await;
    repo.gc(&now_opts()).await.unwrap();
    let keys = be.list("").await.unwrap();
    let dry = GcOptions {
        dry_run: true,
        ..now_opts()
    };
    let report = repo.gc(&dry).await.unwrap();
    assert!(report.deleted_objects > 0, "{report:?}");
    assert_eq!(be.list("").await.unwrap(), keys);
}
