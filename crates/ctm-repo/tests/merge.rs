//! `ctm merge` (R7), against MemBackend.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ctm_repo::{
    BranchName, Error, GcOptions, Identity, MergeOptions, MergeOutcome, RefSpec, Repo, Resolve,
};
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

/// Commits a tree of `files` (path → contents; `/` makes directories) to `branch`.
async fn put(repo: &Repo, branch: &str, files: &[(&str, &str)]) {
    let dir = tempfile::tempdir().unwrap();
    for (path, data) in files {
        let p = dir.path().join(path);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, data).unwrap();
    }
    repo.import(dir.path(), &name(branch), "").await.unwrap();
}

/// Every file in a ref: path → contents.
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

async fn setup() -> (Arc<MemBackend>, Repo) {
    let be = Arc::new(MemBackend::new());
    let repo = Repo::init(be.clone(), identity()).await.unwrap().0;
    put(
        &repo,
        "main",
        &[
            ("a.txt", "a"),
            ("b.txt", "b"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    repo.fork(&spec("main"), &name("feature")).await.unwrap();
    (be, repo)
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

async fn merge(repo: &Repo, src: &str, dst: &str, resolve: Option<Resolve>) -> MergeOutcome {
    let opts = MergeOptions {
        resolve,
        dry_run: false,
    };
    repo.merge(&name(src), &name(dst), &opts).await.unwrap()
}

#[tokio::test]
async fn changes_on_both_sides_merge_and_the_next_merge_starts_from_there() {
    let (_be, repo) = setup().await;
    put(
        &repo,
        "main",
        &[
            ("a.txt", "A"),
            ("b.txt", "b"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    put(
        &repo,
        "feature",
        &[
            ("a.txt", "a"),
            ("b.txt", "B"),
            ("dir/x", "x"),
            ("dir/y", "y"),
        ],
    )
    .await;

    let MergeOutcome::Merged {
        commit,
        taken,
        resolved,
    } = merge(&repo, "feature", "main", None).await
    else {
        panic!("expected a merge");
    };
    assert!(commit.is_some());
    assert!(resolved.is_empty());
    assert_eq!(taken, ["b.txt", "dir/y", "gone.txt"]);
    assert_eq!(
        files(&repo, "main").await,
        map(&[
            ("a.txt", "A"),
            ("b.txt", "B"),
            ("dir/x", "x"),
            ("dir/y", "y")
        ])
    );
    let (r, _) = repo.read_ref(&name("main")).await.unwrap();
    let (f, _) = repo.read_ref(&name("feature")).await.unwrap();
    assert_eq!(r.merge_bases["feature"], f.head);
    let log = repo.log(&spec("main"), None).await.unwrap();
    assert_eq!(log[0].message, "merge feature into main");

    assert_eq!(
        merge(&repo, "feature", "main", None).await,
        MergeOutcome::UpToDate
    );

    // GC doesn't take the base away, and the next merge brings only what's new since.
    let gc = GcOptions {
        auto_retention: Duration::ZERO,
        grace: Duration::ZERO,
        ..GcOptions::default()
    };
    for _ in 0..2 {
        repo.gc(&gc).await.unwrap();
    }
    put(
        &repo,
        "feature",
        &[
            ("a.txt", "a"),
            ("b.txt", "B"),
            ("dir/x", "x"),
            ("dir/y", "y"),
            ("c.txt", "c"),
        ],
    )
    .await;
    let MergeOutcome::Merged { taken, .. } = merge(&repo, "feature", "main", None).await else {
        panic!("expected a merge");
    };
    assert_eq!(taken, ["c.txt"]);
    assert_eq!(
        files(&repo, "main").await["a.txt"],
        "A",
        "main's own change stays"
    );
    assert_eq!(ctm_repo::check(&repo).await.unwrap(), Vec::<String>::new());
}

#[tokio::test]
async fn conflicts_are_refused_unless_resolved() {
    let (_be, repo) = setup().await;
    put(
        &repo,
        "main",
        &[
            ("a.txt", "main's"),
            ("b.txt", "b"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    put(
        &repo,
        "feature",
        &[
            ("a.txt", "feature's"),
            ("b.txt", "b"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    let (before, _) = repo.read_ref(&name("main")).await.unwrap();
    assert_eq!(
        merge(&repo, "feature", "main", None).await,
        MergeOutcome::Conflicts(vec!["a.txt".into()])
    );
    assert_eq!(
        repo.read_ref(&name("main")).await.unwrap().0.head,
        before.head,
        "nothing written"
    );

    let dry = MergeOptions {
        resolve: Some(Resolve::Theirs),
        dry_run: true,
    };
    let MergeOutcome::Merged {
        commit: None,
        resolved,
        ..
    } = repo
        .merge(&name("feature"), &name("main"), &dry)
        .await
        .unwrap()
    else {
        panic!("expected a dry run");
    };
    assert_eq!(resolved, ["a.txt"]);
    assert_eq!(
        repo.read_ref(&name("main")).await.unwrap().0.head,
        before.head
    );

    repo.fork(&spec("main"), &name("m2")).await.unwrap();
    merge(&repo, "feature", "main", Some(Resolve::KeepBoth)).await;
    let merged = files(&repo, "main").await;
    assert_eq!(merged["a.txt"], "main's");
    assert_eq!(merged["a.feature.txt"], "feature's");

    // m2 was forked from main before the merge: --theirs takes feature's version.
    repo.merge(&name("main"), &name("m2"), &MergeOptions::default())
        .await
        .unwrap();
    put(
        &repo,
        "m2",
        &[
            ("a.txt", "m2's"),
            ("b.txt", "b"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    merge(&repo, "feature", "m2", Some(Resolve::Theirs)).await;
    assert_eq!(files(&repo, "m2").await["a.txt"], "feature's");
}

#[tokio::test]
async fn a_change_beats_a_deletion_and_merges_go_both_ways() {
    let (_be, repo) = setup().await;
    // main deletes b.txt; feature changes it.
    put(
        &repo,
        "main",
        &[("a.txt", "a"), ("gone.txt", "g"), ("dir/x", "x")],
    )
    .await;
    put(
        &repo,
        "feature",
        &[
            ("a.txt", "a"),
            ("b.txt", "changed"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
        ],
    )
    .await;
    merge(&repo, "feature", "main", None).await;
    assert_eq!(files(&repo, "main").await["b.txt"], "changed");

    // Back the other way: main's new state (a merge commit) goes into feature.
    put(
        &repo,
        "main",
        &[
            ("a.txt", "a"),
            ("b.txt", "changed"),
            ("gone.txt", "g"),
            ("dir/x", "x"),
            ("m.txt", "m"),
        ],
    )
    .await;
    let MergeOutcome::Merged { taken, .. } = merge(&repo, "main", "feature", None).await else {
        panic!("expected a merge");
    };
    assert_eq!(taken, ["m.txt"]);
    assert_eq!(files(&repo, "feature").await, files(&repo, "main").await);
    assert_eq!(
        merge(&repo, "feature", "main", None).await,
        MergeOutcome::UpToDate
    );
}

#[tokio::test]
async fn unrelated_branches_have_no_merge_base() {
    let (_be, repo) = setup().await;
    put(&repo, "other", &[("z", "z")]).await;
    let err = repo
        .merge(&name("other"), &name("main"), &MergeOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NoMergeBase { .. }), "{err}");
}
