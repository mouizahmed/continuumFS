//! Deleting, undeleting, renaming, and resetting refs (v0.6), against MemBackend.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ctm_repo::{BranchName, Error, GcOptions, Identity, MergeOptions, MergeOutcome, RefSpec, Repo};
use ctm_store::MemBackend;

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

/// Commits a tree of `files` (path → contents) to `branch`.
async fn put(repo: &Repo, branch: &str, files: &[(&str, &str)]) {
    let dir = tempfile::tempdir().unwrap();
    for (path, data) in files {
        let p = dir.path().join(path);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, data).unwrap();
    }
    repo.import(dir.path(), &name(branch), "").await.unwrap();
}

async fn files(repo: &Repo, r: &str) -> BTreeMap<String, String> {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("x");
    repo.export(&spec(r), &out).await.unwrap();
    let mut map = BTreeMap::new();
    walk(&out, &out, &mut map);
    map
}

fn walk(root: &Path, d: &Path, out: &mut BTreeMap<String, String>) {
    for e in fs::read_dir(d).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            walk(root, &p, out);
        } else {
            let rel = p.strip_prefix(root).unwrap().to_string_lossy().into_owned();
            out.insert(rel, fs::read_to_string(&p).unwrap());
        }
    }
}

async fn setup() -> Repo {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be, identity()).await.unwrap().0;
    put(&repo, "main", &[("a.txt", "a"), ("b.txt", "b")]).await;
    repo
}

/// GC with no grace period, so a second run deletes what the first listed.
fn gc_now() -> GcOptions {
    GcOptions {
        grace: Duration::ZERO,
        ..GcOptions::default()
    }
}

async fn merge(repo: &Repo, src: &str, dst: &str) -> ctm_repo::Result<MergeOutcome> {
    repo.merge(&name(src), &name(dst), &MergeOptions::default())
        .await
}

#[tokio::test]
async fn a_deleted_branch_keeps_its_data_until_its_tombstone_expires() {
    let repo = setup().await;
    repo.fork(&spec("main"), &name("feature")).await.unwrap();
    put(&repo, "feature", &[("a.txt", "a"), ("only-here.txt", "x")]).await;
    let before = files(&repo, "feature").await;

    repo.delete_branch(&name("feature")).await.unwrap();
    assert!(matches!(
        repo.read_ref(&name("feature")).await,
        Err(Error::UnknownRef(_))
    ));
    assert_eq!(repo.list_branches().await.unwrap(), [name("main")]);
    let deleted = repo.list_deleted().await.unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].name, name("feature"));
    assert!(!deleted[0].snapshot);

    // Two GC runs (the second would delete what the first listed) keep it.
    for _ in 0..2 {
        repo.gc(&gc_now()).await.unwrap();
    }
    repo.undelete_branch(&name("feature")).await.unwrap();
    assert_eq!(files(&repo, "feature").await, before);
    assert!(repo.list_deleted().await.unwrap().is_empty());
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());

    // Once the tombstone has expired, GC drops it and collects the data.
    repo.delete_branch(&name("feature")).await.unwrap();
    let expired = GcOptions {
        tombstone_ttl: Duration::ZERO,
        ..gc_now()
    };
    std::thread::sleep(Duration::from_millis(1100));
    let first = repo.gc(&expired).await.unwrap();
    assert_eq!(first.expired_tombstones, 1);
    let second = repo.gc(&expired).await.unwrap();
    assert!(second.deleted_objects > 0, "{second:?}");
    assert!(repo.undelete_branch(&name("feature")).await.is_err());
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn a_push_that_races_a_delete_fails_instead_of_landing() {
    let repo = setup().await;
    let (r, etag) = repo.read_ref(&name("main")).await.unwrap();
    repo.delete_branch(&name("main")).await.unwrap();
    let err = repo.cas_ref(&name("main"), &r, &etag).await.unwrap_err();
    assert!(
        matches!(err, Error::Store(ctm_store::Error::PreconditionFailed(_))),
        "{err}"
    );
    // The name is free again.
    put(&repo, "main", &[("new.txt", "n")]).await;
    assert_eq!(files(&repo, "main").await.len(), 1);
    assert!(matches!(
        repo.undelete_branch(&name("main")).await,
        Err(Error::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn a_name_used_again_starts_with_no_merge_history() {
    let repo = setup().await;
    repo.fork(&spec("main"), &name("feature")).await.unwrap();
    put(
        &repo,
        "feature",
        &[("a.txt", "a"), ("b.txt", "b"), ("c.txt", "c")],
    )
    .await;
    merge(&repo, "feature", "main").await.unwrap();
    repo.delete_branch(&name("feature")).await.unwrap();

    // An unrelated branch under the old name: main's merge base for "feature" doesn't apply.
    put(&repo, "feature", &[("z.txt", "z")]).await;
    assert!(matches!(
        merge(&repo, "feature", "main").await,
        Err(Error::NoMergeBase { .. })
    ));

    // A fresh fork under the old name merges from its own fork point.
    repo.delete_branch(&name("feature")).await.unwrap();
    repo.fork(&spec("main"), &name("feature")).await.unwrap();
    put(
        &repo,
        "feature",
        &[
            ("a.txt", "a"),
            ("b.txt", "b"),
            ("c.txt", "c"),
            ("d.txt", "d"),
        ],
    )
    .await;
    let MergeOutcome::Merged { taken, .. } = merge(&repo, "feature", "main").await.unwrap() else {
        panic!("expected a merge");
    };
    assert_eq!(taken, ["d.txt"]);
}

#[tokio::test]
async fn a_renamed_branch_keeps_its_merge_history() {
    let repo = setup().await;
    repo.fork(&spec("main"), &name("feature")).await.unwrap();
    put(
        &repo,
        "feature",
        &[("a.txt", "a"), ("b.txt", "b"), ("c.txt", "c")],
    )
    .await;
    merge(&repo, "feature", "main").await.unwrap();

    repo.rename_branch(&name("feature"), &name("feat"))
        .await
        .unwrap();
    assert_eq!(
        repo.list_branches().await.unwrap(),
        [name("feat"), name("main")]
    );
    put(
        &repo,
        "feat",
        &[
            ("a.txt", "a"),
            ("b.txt", "b"),
            ("c.txt", "c"),
            ("d.txt", "d"),
        ],
    )
    .await;
    let MergeOutcome::Merged { taken, .. } = merge(&repo, "feat", "main").await.unwrap() else {
        panic!("expected a merge");
    };
    assert_eq!(taken, ["d.txt"], "only what's new since the last merge");
    assert!(matches!(
        repo.rename_branch(&name("feat"), &name("main")).await,
        Err(Error::AlreadyExists(_))
    ));
}

#[tokio::test]
async fn reset_points_a_branch_back_and_keeps_the_old_head_in_its_log() {
    let repo = setup().await;
    let first = repo.read_ref(&name("main")).await.unwrap().0.head;
    put(&repo, "main", &[("a.txt", "A"), ("c.txt", "c")]).await;
    let second = repo.read_ref(&name("main")).await.unwrap().0.head;

    repo.reset(&name("main"), &spec(&first.to_hex()))
        .await
        .unwrap();
    assert_eq!(files(&repo, "main").await["a.txt"], "a");
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(log[0].kind, ctm_core::CommitKind::Reset);
    assert_eq!(log[0].commit, first);
    assert_eq!(log[1].commit, second);
}

#[tokio::test]
async fn snapshots_can_be_deleted_and_undeleted() {
    let repo = setup().await;
    repo.snapshot(&name("v1"), &spec("main")).await.unwrap();
    repo.delete_snapshot(&name("v1")).await.unwrap();
    assert!(repo.list_snapshots().await.unwrap().is_empty());
    assert!(repo.resolve(&spec("snap/v1")).await.is_err());
    let deleted = repo.list_deleted().await.unwrap();
    assert!(deleted[0].snapshot);
    repo.undelete_snapshot(&name("v1")).await.unwrap();
    assert_eq!(files(&repo, "snap/v1").await.len(), 2);
}

#[tokio::test]
async fn manual_retention_drops_old_non_auto_commits_but_never_the_head() {
    let repo = setup().await;
    put(&repo, "main", &[("a.txt", "2")]).await;
    put(&repo, "main", &[("a.txt", "3")]).await;
    assert_eq!(repo.log(&spec("main"), None).await.unwrap().len(), 3);

    let keep = repo.gc(&gc_now()).await.unwrap();
    assert_eq!(keep.dropped_commits, 0, "kept by default");

    let opts = GcOptions {
        manual_retention: Some(Duration::ZERO),
        ..gc_now()
    };
    let report = repo.gc(&opts).await.unwrap();
    assert_eq!(report.dropped_commits, 2);
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(files(&repo, "main").await["a.txt"], "3");
}
