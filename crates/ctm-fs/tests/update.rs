//! `update` (v0.6): a mount follows its branch, keeping uncommitted changes. Driven through
//! `MountState` (no FUSE).

mod common;

use std::fs;
use std::sync::Arc;

use ctm_fs::{Errno, MountState, PushOutcome, SetAttr, UpdateOutcome};
use ctm_repo::{BranchName, Repo};
use ctm_store::{Backend, MemBackend};

use common::{commit_push, identity, lookup_path, open, read_all, read_fh};

async fn repo_with(files: &[(&str, &str)]) -> Arc<Repo> {
    let src = tempfile::tempdir().unwrap();
    for (path, data) in files {
        let p = src.path().join(path);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, data).unwrap();
    }
    let backend: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let (repo, _) = Repo::init(backend, identity("laptop")).await.unwrap();
    repo.import(src.path(), &BranchName::new("main").unwrap(), "import")
        .await
        .unwrap();
    Arc::new(repo)
}

async fn write_file(state: &MountState, path: &str, data: &[u8]) {
    let (parent, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (lookup_path(state, dir).await.unwrap(), name),
        None => (1, path),
    };
    let ino = match state.lookup(parent, name.as_bytes()).await {
        Ok(a) => {
            let size = SetAttr {
                size: Some(0),
                ..SetAttr::default()
            };
            state.setattr(a.ino, size).await.unwrap();
            a.ino
        }
        Err(_) => {
            let (a, fh) = state.create(parent, name.as_bytes(), 0o644).await.unwrap();
            state.release(fh).await.unwrap();
            a.ino
        }
    };
    let fh = state.open_file(ino, true).await.unwrap();
    state.write(fh, ino, 0, data).await.unwrap();
    state.release(fh).await.unwrap();
}

async fn read(state: &MountState, path: &str) -> Result<String, Errno> {
    let ino = lookup_path(state, path).await?;
    Ok(String::from_utf8(read_all(state, ino).await).unwrap())
}

/// Two mounts of `main`: `here` and `there`, the other machine.
async fn two(
    files: &[(&str, &str)],
) -> (Arc<Repo>, Vec<tempfile::TempDir>, MountState, MountState) {
    let repo = repo_with(files).await;
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let here = open(&repo, d1.path(), "main", false).await;
    let there = open(&repo, d2.path(), "main", false).await;
    (repo, vec![d1, d2], here, there)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_clean_mount_follows_its_branch() {
    let (_repo, _dirs, here, there) =
        two(&[("a.txt", "a"), ("dir/b.txt", "b"), ("dir/gone.txt", "g")]).await;
    // Looked up here first, so stale nodes would show if update missed them.
    assert_eq!(read(&here, "dir/b.txt").await.unwrap(), "b");
    assert_eq!(read(&here, "dir/gone.txt").await.unwrap(), "g");
    let a_ino = lookup_path(&here, "a.txt").await.unwrap();

    write_file(&there, "dir/b.txt", b"B").await;
    write_file(&there, "dir/new.txt", b"n").await;
    let gone_dir = lookup_path(&there, "dir").await.unwrap();
    there.unlink(gone_dir, b"gone.txt").await.unwrap();
    commit_push(&there, "there").await;

    assert!(here.status().await.unwrap().behind);
    let UpdateOutcome::Updated { changed, .. } = here.update().await.unwrap() else {
        panic!("expected an update");
    };
    assert_eq!(changed, 3);
    assert_eq!(read(&here, "dir/b.txt").await.unwrap(), "B");
    assert_eq!(read(&here, "dir/new.txt").await.unwrap(), "n");
    assert_eq!(read(&here, "dir/gone.txt").await, Err(Errno::ENOENT));
    assert_eq!(
        lookup_path(&here, "a.txt").await.unwrap(),
        a_ino,
        "same inode"
    );
    let status = here.status().await.unwrap();
    assert!(!status.behind);
    assert_eq!(status.dirty_files, 0);
    assert_eq!(here.update().await.unwrap(), UpdateOutcome::UpToDate);

    // And it pushes on top of the branch: no auto-fork.
    write_file(&here, "a.txt", b"A").await;
    let pushed = commit_push(&here, "here").await;
    assert!(matches!(pushed, PushOutcome::Pushed { .. }), "{pushed:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn uncommitted_changes_stay_unless_they_overlap() {
    let (repo, dirs, mut here, there) =
        two(&[("a.txt", "a"), ("dir/b.txt", "b"), ("dir/c.txt", "c")]).await;
    write_file(&here, "dir/c.txt", b"mine").await;
    write_file(&here, "local.txt", b"l").await;

    write_file(&there, "dir/b.txt", b"theirs").await;
    commit_push(&there, "there").await;
    assert!(matches!(
        here.update().await.unwrap(),
        UpdateOutcome::Updated { changed: 1, .. }
    ));
    assert_eq!(read(&here, "dir/b.txt").await.unwrap(), "theirs");
    assert_eq!(read(&here, "dir/c.txt").await.unwrap(), "mine");
    assert_eq!(read(&here, "local.txt").await.unwrap(), "l");
    // The updated base and the kept changes survive a remount.
    drop(here);
    here = open(&repo, dirs[0].path(), "main", false).await;
    assert_eq!(read(&here, "dir/b.txt").await.unwrap(), "theirs");
    assert_eq!(read(&here, "dir/c.txt").await.unwrap(), "mine");
    assert!(!here.status().await.unwrap().behind);
    let pushed = commit_push(&here, "here").await;
    assert!(matches!(pushed, PushOutcome::Pushed { .. }), "{pushed:?}");
    let files = [
        ("dir/b.txt", "theirs"),
        ("dir/c.txt", "mine"),
        ("local.txt", "l"),
    ];
    for (path, want) in files {
        let mut out = Vec::new();
        repo.cat(&format!("main:{path}").parse().unwrap(), &mut out)
            .await
            .unwrap();
        assert_eq!(String::from_utf8(out).unwrap(), want, "{path}");
    }

    // Both change the same file: refused, and nothing applied.
    there.update().await.unwrap();
    write_file(&here, "a.txt", b"here").await;
    write_file(&there, "a.txt", b"there").await;
    write_file(&there, "dir/b.txt", b"again").await;
    commit_push(&there, "there").await;
    assert_eq!(
        here.update().await.unwrap(),
        UpdateOutcome::Conflicts(vec!["a.txt".into()])
    );
    assert_eq!(read(&here, "a.txt").await.unwrap(), "here");
    assert_eq!(read(&here, "dir/b.txt").await.unwrap(), "theirs");
    assert!(here.status().await.unwrap().behind);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_file_keeps_what_it_opened() {
    let (_repo, _dirs, here, there) = two(&[("a.txt", "old")]).await;
    let ino = lookup_path(&here, "a.txt").await.unwrap();
    let fh = here.open_file(ino, false).await.unwrap();
    write_file(&there, "a.txt", b"new").await;
    commit_push(&there, "there").await;
    here.update().await.unwrap();
    assert_eq!(read_fh(&here, fh, ino).await, b"old");
    here.release(fh).await.unwrap();
    assert_eq!(read(&here, "a.txt").await.unwrap(), "new");
}

#[tokio::test(flavor = "multi_thread")]
async fn unpushed_commits_go_first_and_read_only_mounts_follow_too() {
    let (repo, dirs, here, there) = two(&[("a.txt", "a")]).await;
    let ro_dir = tempfile::tempdir().unwrap();
    let reader = open(&repo, ro_dir.path(), "main", true).await;
    assert_eq!(read(&reader, "a.txt").await.unwrap(), "a");

    write_file(&there, "a.txt", b"b").await;
    commit_push(&there, "there").await;
    write_file(&here, "x.txt", b"x").await;
    here.commit("unpushed").await.unwrap();
    assert_eq!(here.update().await.unwrap(), UpdateOutcome::Unpushed);

    reader.update().await.unwrap();
    assert_eq!(read(&reader, "a.txt").await.unwrap(), "b");
    drop(dirs);
}
