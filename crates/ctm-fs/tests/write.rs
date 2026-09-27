//! Writes, commit, auto-fork, and crash recovery, driven through `MountState` (no FUSE).

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use ctm_fs::{CommitOutcome, Errno, FileKind, MountState, SetAttr};
use ctm_repo::{BranchName, Repo};
use ctm_store::faulty::Faults;
use ctm_store::{Backend, FaultyBackend, MemBackend};
use proptest::prelude::*;

use common::{Counting, identity, lookup_path, open, random_bytes, read_all, read_fh};

fn main_branch() -> BranchName {
    BranchName::new("main").unwrap()
}

/// A repo whose `main` holds `files`, on `backend`.
async fn repo_with(backend: Arc<dyn Backend>, files: &[(&str, Vec<u8>)]) -> Arc<Repo> {
    let src = tempfile::tempdir().unwrap();
    fs::create_dir_all(src.path().join("empty")).unwrap();
    for (path, data) in files {
        let p = src.path().join(path);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, data).unwrap();
    }
    let (repo, _) = Repo::init(backend, identity("laptop")).await.unwrap();
    repo.import(src.path(), &main_branch(), "import")
        .await
        .unwrap();
    Arc::new(repo)
}

/// Every file in a ref: path → bytes (directories as `path/`).
async fn export(repo: &Repo, spec: &str) -> BTreeMap<String, Vec<u8>> {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    repo.export(&spec.parse().unwrap(), &out).await.unwrap();
    let mut files = BTreeMap::new();
    let mut stack = vec![out.clone()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            let rel = p.strip_prefix(&out).unwrap().to_string_lossy().into_owned();
            let m = fs::symlink_metadata(&p).unwrap();
            if m.file_type().is_symlink() {
                let t = fs::read_link(&p).unwrap();
                files.insert(rel, format!("-> {}", t.display()).into_bytes());
            } else if m.is_dir() {
                files.insert(format!("{rel}/"), Vec::new());
                stack.push(p);
            } else {
                files.insert(rel, fs::read(&p).unwrap());
            }
        }
    }
    files
}

async fn write_file(state: &MountState, path: &str, data: &[u8]) -> u64 {
    let (parent, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (lookup_path(state, dir).await.unwrap(), name),
        None => (1, path),
    };
    let ino = match state.lookup(parent, name.as_bytes()).await {
        Ok(a) => {
            state
                .setattr(
                    a.ino,
                    SetAttr {
                        size: Some(0),
                        ..SetAttr::default()
                    },
                )
                .await
                .unwrap();
            a.ino
        }
        Err(_) => {
            let (a, fh) = state.create(parent, name.as_bytes(), 0o644).await.unwrap();
            state.release(fh).await.unwrap();
            a.ino
        }
    };
    let fh = state.open_file(ino, true).await.unwrap();
    assert_eq!(
        state.write(fh, ino, 0, data).await.unwrap() as usize,
        data.len()
    );
    state.release(fh).await.unwrap();
    ino
}

async fn names(state: &MountState, path: &str) -> Vec<String> {
    let ino = lookup_path(state, path).await.unwrap();
    state
        .readdir(ino)
        .await
        .unwrap()
        .into_iter()
        .map(|i| String::from_utf8(i.name).unwrap())
        .collect()
}

// The model test: one file, random operations, compared with a plain byte vector.

#[derive(Clone, Debug)]
enum Op {
    Write { at: usize, len: usize, seed: u64 },
    Truncate(usize),
    Read { at: usize, len: usize },
    Fsync,
    Reopen,
    Commit,
    Remount,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (0usize..3_500_000, 1usize..300_000, any::<u64>())
            .prop_map(|(at, len, seed)| Op::Write { at, len, seed }),
        2 => (0usize..3_500_000).prop_map(Op::Truncate),
        4 => (0usize..3_500_000, 1usize..2_000_000).prop_map(|(at, len)| Op::Read { at, len }),
        1 => Just(Op::Fsync),
        1 => Just(Op::Reopen),
        2 => Just(Op::Commit),
        2 => Just(Op::Remount),
    ]
}

async fn run_model(base_len: usize, ops: Vec<Op>) {
    let base = random_bytes(99, base_len);
    let repo = repo_with(Arc::new(MemBackend::new()), &[("f", base.clone())]).await;
    let dir = tempfile::tempdir().unwrap();
    let mut model = base;
    let mut state = open(&repo, dir.path(), "main", false).await;
    let mut ino = lookup_path(&state, "f").await.unwrap();
    let mut fh = state.open_file(ino, true).await.unwrap();
    for op in ops {
        match op {
            Op::Write { at, len, seed } => {
                let data = random_bytes(seed, len);
                state.write(fh, ino, at as u64, &data).await.unwrap();
                if model.len() < at + len {
                    model.resize(at + len, 0);
                }
                model[at..at + len].copy_from_slice(&data);
            }
            Op::Truncate(n) => {
                state
                    .setattr(
                        ino,
                        SetAttr {
                            size: Some(n as u64),
                            ..SetAttr::default()
                        },
                    )
                    .await
                    .unwrap();
                model.resize(n, 0);
            }
            Op::Read { at, len } => {
                let got = state.read(fh, ino, at as u64, len as u32).await.unwrap();
                let end = (at + len).min(model.len());
                let want = if at >= model.len() {
                    &[][..]
                } else {
                    &model[at..end]
                };
                assert!(got == want, "read({at}, {len}) differs");
            }
            Op::Fsync => state.fsync(ino).await.unwrap(),
            Op::Reopen => {
                state.release(fh).await.unwrap();
                fh = state.open_file(ino, true).await.unwrap();
            }
            Op::Commit => {
                state.commit("model").await.unwrap();
            }
            Op::Remount => {
                // A crash of the mount process: nothing is committed or released.
                drop(state);
                state = open(&repo, dir.path(), "main", false).await;
                ino = lookup_path(&state, "f").await.unwrap();
                fh = state.open_file(ino, true).await.unwrap();
            }
        }
        assert_eq!(state.getattr(ino).await.unwrap().size, model.len() as u64);
    }
    assert_eq!(read_fh(&state, fh, ino).await, model, "final contents");
    state.commit("final").await.unwrap();
    assert_eq!(
        export(&repo, "main").await["f"],
        model,
        "committed contents"
    );
}

proptest! {
    // Each case commits megabytes, so the default is small; PROPTEST_CASES raises it.
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(24)
    ))]

    #[test]
    fn working_state_matches_a_byte_vector(
        base_len in prop_oneof![Just(0usize), 1usize..5000, 1_000_000usize..3_000_000],
        ops in proptest::collection::vec(arb_op(), 1..14),
    ) {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_model(base_len, ops));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn tree_operations_commit_and_survive_a_remount() {
    let repo = repo_with(
        Arc::new(MemBackend::new()),
        &[
            ("small.txt", b"small".to_vec()),
            ("notes.txt", b"notes".to_vec()),
            ("dir/a.txt", b"a".to_vec()),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;

    let d = state.mkdir(1, b"new", 0o755).await.unwrap().ino;
    assert_eq!(state.getattr(d).await.unwrap().kind, FileKind::Dir);
    write_file(&state, "new/f.txt", b"fresh").await;
    state.symlink(1, b"link", b"new/f.txt").await.unwrap();
    state
        .rename(1, b"small.txt", d, b"moved.txt", false)
        .await
        .unwrap();
    state.unlink(1, b"notes.txt").await.unwrap();
    state.rmdir(1, b"empty").await.unwrap();

    assert_eq!(
        state.create(1, b"link", 0o644).await.err(),
        Some(Errno::EEXIST)
    );
    assert_eq!(state.rmdir(1, b"new").await, Err(Errno::ENOTEMPTY));
    assert_eq!(state.unlink(1, b"dir").await, Err(Errno::EISDIR));
    assert_eq!(state.rmdir(1, b"link").await, Err(Errno::ENOTDIR));
    assert_eq!(lookup_path(&state, "notes.txt").await, Err(Errno::ENOENT));

    let expected_root = vec!["dir", "link", "new"];
    assert_eq!(names(&state, "").await, expected_root);

    // Nothing committed yet: a remount still shows every change.
    drop(state);
    let state = open(&repo, dir.path(), "main", false).await;
    assert_eq!(names(&state, "").await, expected_root);
    assert_eq!(names(&state, "new").await, ["f.txt", "moved.txt"]);
    let moved = lookup_path(&state, "new/moved.txt").await.unwrap();
    assert_eq!(read_all(&state, moved).await, b"small");
    // f.txt, link, moved.txt, plus the deleted notes.txt, empty/, and small.txt (renamed away).
    assert_eq!(state.status().await.unwrap().dirty_files, 6);

    assert!(matches!(
        state.commit("tree ops").await.unwrap(),
        CommitOutcome::Pushed { .. }
    ));
    let files = export(&repo, "main").await;
    let want: BTreeMap<String, Vec<u8>> = [
        ("dir/", &b""[..]),
        ("dir/a.txt", b"a"),
        ("link", b"-> new/f.txt"),
        ("new/", b""),
        ("new/f.txt", b"fresh"),
        ("new/moved.txt", b"small"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_vec()))
    .collect();
    assert_eq!(files, want);
    assert_eq!(state.status().await.unwrap().dirty_files, 0);
    assert!(matches!(
        state.commit("again").await.unwrap(),
        CommitOutcome::NothingToCommit
    ));

    // After the commit the working state is empty, and a remount shows the new base.
    drop(state);
    let state = open(&repo, dir.path(), "main", false).await;
    assert_eq!(names(&state, "").await, expected_root);
}

#[tokio::test(flavor = "multi_thread")]
async fn rename_replaces_files_and_moves_directories_with_their_contents() {
    let repo = repo_with(
        Arc::new(MemBackend::new()),
        &[
            ("a.txt", b"a".to_vec()),
            ("b.txt", b"b".to_vec()),
            ("src/deep/x.txt", b"x".to_vec()),
        ],
    )
    .await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;
    state.rename(1, b"a.txt", 1, b"b.txt", false).await.unwrap();
    let b = lookup_path(&state, "b.txt").await.unwrap();
    assert_eq!(read_all(&state, b).await, b"a");
    assert_eq!(
        state.rename(1, b"b.txt", 1, b"src", false).await,
        Err(Errno::EISDIR)
    );
    let dst = state.mkdir(1, b"dst", 0o755).await.unwrap().ino;
    state.rename(1, b"src", dst, b"src2", false).await.unwrap();
    let x = lookup_path(&state, "dst/src2/deep/x.txt").await.unwrap();
    assert_eq!(read_all(&state, x).await, b"x");
    state.commit("renames").await.unwrap();
    let files = export(&repo, "main").await;
    assert_eq!(files["b.txt"], b"a");
    assert_eq!(files["dst/src2/deep/x.txt"], b"x");
    assert!(!files.contains_key("a.txt") && !files.contains_key("src/"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unlinked_open_file_stays_readable_and_is_not_committed() {
    let repo = repo_with(Arc::new(MemBackend::new()), &[("keep", b"k".to_vec())]).await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;
    let (a, fh) = state.create(1, b"tmp", 0o600).await.unwrap();
    state.write(fh, a.ino, 0, b"scratch").await.unwrap();
    state.unlink(1, b"tmp").await.unwrap();
    assert_eq!(lookup_path(&state, "tmp").await, Err(Errno::ENOENT));
    assert_eq!(read_fh(&state, fh, a.ino).await, b"scratch");
    state.commit("").await.unwrap();
    state.release(fh).await.unwrap();
    assert!(!export(&repo, "main").await.contains_key("tmp"));
}

#[tokio::test(flavor = "multi_thread")]
async fn two_mounts_race_and_exactly_one_auto_forks() {
    let backend: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let repo = repo_with(backend.clone(), &[("notes.txt", b"base\n".to_vec())]).await;
    let desktop_repo = Arc::new(Repo::open(backend, identity("desktop")).await.unwrap());
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let laptop = open(&repo, d1.path(), "main", false).await;
    let desktop = open(&desktop_repo, d2.path(), "main", false).await;
    write_file(&laptop, "notes.txt", b"a\n").await;
    write_file(&desktop, "notes.txt", b"b\n").await;

    let first = laptop.commit("laptop").await.unwrap();
    assert!(matches!(first, CommitOutcome::Pushed { .. }), "{first:?}");
    let second = desktop.commit("desktop").await.unwrap();
    let CommitOutcome::AutoForked { branch, .. } = second else {
        panic!("expected an auto-fork, got {second:?}");
    };
    assert_eq!(branch.as_str(), "main.desktop");
    assert_eq!(export(&repo, "main").await["notes.txt"], b"a\n");
    assert_eq!(export(&repo, "main.desktop").await["notes.txt"], b"b\n");

    let status = desktop.status().await.unwrap();
    assert_eq!(status.branch.unwrap().as_str(), "main.desktop");
    assert_eq!(status.auto_forked_from.unwrap().as_str(), "main");

    // Later commits go to the fork, and the fork's history includes the base.
    write_file(&desktop, "notes.txt", b"c\n").await;
    assert!(matches!(
        desktop.commit("more").await.unwrap(),
        CommitOutcome::Pushed { .. }
    ));
    assert_eq!(export(&repo, "main.desktop").await["notes.txt"], b"c\n");
    let log = repo
        .log(&"main.desktop".parse().unwrap(), None)
        .await
        .unwrap();
    let messages: Vec<_> = log.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(messages, ["more", "desktop", "import"]);
    assert_eq!(repo.list_branches().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn behind_is_reported_when_the_branch_moves() {
    let backend: Arc<dyn Backend> = Arc::new(MemBackend::new());
    let repo = repo_with(backend, &[("a", b"1".to_vec())]).await;
    let (d1, d2) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let one = open(&repo, d1.path(), "main", false).await;
    let two = open(&repo, d2.path(), "main", false).await;
    assert!(!two.status().await.unwrap().behind);
    write_file(&one, "a", b"2").await;
    one.commit("").await.unwrap();
    assert!(two.status().await.unwrap().behind);
}

/// A crash at any point during a commit: after a remount, the branch is at the old or the
/// new commit, the working state agrees, and committing again never forks against itself.
/// `lands` makes the crashing PUT reach the bucket first, as when the process dies after the
/// CAS succeeds but before it records that.
async fn crash_during_commit(n: usize, lands: bool) -> bool {
    let backend = Arc::new(FaultyBackend::new(MemBackend::new(), Faults::default()));
    let repo = repo_with(backend.clone(), &[("f", random_bytes(3, 2_000_000))]).await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;
    write_file(&state, "g", b"new file").await;
    let f = lookup_path(&state, "f").await.unwrap();
    let fh = state.open_file(f, true).await.unwrap();
    state.write(fh, f, 1_000_000, b"edit").await.unwrap();
    state.release(fh).await.unwrap();
    drop(state);

    backend.set_faults(Faults {
        crash_after_puts: Some(n),
        crash_lands_put: lands,
        ..Faults::default()
    });
    let crashing = backend.clone();
    let state = open(&repo, dir.path(), "main", false).await;
    let result = state.commit("crashy").await;
    drop(state);
    crashing.heal();

    let case = format!("crash after {n} puts (lands: {lands})");
    let state = open(&repo, dir.path(), "main", false).await;
    match &result {
        Ok(outcome) => {
            assert!(
                matches!(outcome, CommitOutcome::Pushed { .. }),
                "{case}: {outcome:?}"
            );
            assert_eq!(state.status().await.unwrap().dirty_files, 0, "{case}");
        }
        Err(_) => {
            let again = state.commit("retry").await.unwrap();
            assert!(
                matches!(
                    again,
                    CommitOutcome::Pushed { .. } | CommitOutcome::NothingToCommit
                ),
                "{case}: {again:?}"
            );
        }
    }
    assert_eq!(repo.list_branches().await.unwrap().len(), 1, "{case}");
    let files = export(&repo, "main").await;
    assert_eq!(files["g"], b"new file", "{case}");
    assert_eq!(&files["f"][1_000_000..1_000_004], b"edit", "{case}");
    assert_eq!(
        ctm_repo::check(&repo).await.unwrap(),
        Vec::<String>::new(),
        "{case}"
    );
    result.is_ok()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_at_any_put_during_commit_recovers_without_a_fork() {
    for lands in [false, true] {
        let mut n = 0;
        while !crash_during_commit(n, lands).await {
            n += 1;
        }
        assert!(n > 3, "the commit should take several puts");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn appending_one_byte_downloads_at_most_one_chunk() {
    let counting = Arc::new(Counting::default());
    let big = random_bytes(5, 20 << 20);
    let repo = repo_with(counting.clone(), &[("big.bin", big.clone())]).await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;
    counting.reset();
    let ino = lookup_path(&state, "big.bin").await.unwrap();
    let fh = state.open_file(ino, true).await.unwrap();
    assert_eq!(
        counting.chunk_gets(),
        0,
        "opening read-write downloads nothing"
    );
    state.write(fh, ino, big.len() as u64, b"!").await.unwrap();
    state.release(fh).await.unwrap();
    state.commit("append").await.unwrap();
    assert!(
        counting.chunk_gets() <= 1,
        "{} chunks downloaded",
        counting.chunk_gets()
    );
    let mut want = big;
    want.push(b'!');
    assert_eq!(export(&repo, "main").await["big.bin"], want);
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_brings_back_a_path_from_a_ref() {
    let repo = repo_with(Arc::new(MemBackend::new()), &[("f", b"original".to_vec())]).await;
    let dir = tempfile::tempdir().unwrap();
    let state = open(&repo, dir.path(), "main", false).await;
    write_file(&state, "f", b"changed").await;
    state.unlink(1, b"f").await.unwrap();
    state.restore(b"f", &"main".parse().unwrap()).await.unwrap();
    let f = lookup_path(&state, "f").await.unwrap();
    assert_eq!(read_all(&state, f).await, b"original");
    write_file(&state, "f", b"changed again").await;
    state.commit("").await.unwrap();
    let import = repo.log(&"main".parse().unwrap(), None).await.unwrap();
    let import = import.last().unwrap().commit.to_hex();
    state.restore(b"f", &import.parse().unwrap()).await.unwrap();
    assert_eq!(read_all(&state, f).await, b"original");
    state.commit("restored").await.unwrap();
    assert_eq!(export(&repo, "main").await["f"], b"original");
}
