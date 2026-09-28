//! The read-only mount, driven through `MountState` directly (no FUSE).

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{MetadataExt, symlink};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ctm_fs::{Errno, FileKind, MountOptions, MountState};
use ctm_repo::{BranchName, Identity, Repo};

mod common;
use common::{Counting, corrupt_chunks, lookup_path, packed_chunks, random_bytes, read_all};

struct Fixture {
    backend: Arc<Counting>,
    state: MountState,
    src: tempfile::TempDir,
    _dirs: tempfile::TempDir,
}

const BIG: usize = 24 << 20;

async fn mount() -> Fixture {
    let src = tempfile::tempdir().unwrap();
    let root = src.path();
    fs::create_dir_all(root.join("a/b/c")).unwrap();
    fs::create_dir_all(root.join("empty")).unwrap();
    fs::write(root.join("big.bin"), random_bytes(1, BIG)).unwrap();
    fs::write(root.join("small.txt"), b"hello\n").unwrap();
    fs::write(root.join("one_chunk.bin"), random_bytes(2, 100_000)).unwrap();
    fs::write(root.join("a/b/c/deep.txt"), b"deep").unwrap();
    symlink("small.txt", root.join("link")).unwrap();

    let backend = Arc::new(Counting::default());
    let identity = Identity {
        user: "me".into(),
        hostname: "host".into(),
        machine_id: [1; 16],
    };
    let (writer, _) = Repo::init(backend.clone(), identity.clone()).await.unwrap();
    writer
        .import(root, &BranchName::new("main").unwrap(), "")
        .await
        .unwrap();
    backend.reset();
    // The mount is another machine: it knows only what the bucket tells it.
    let repo = Repo::open(backend.clone(), identity).await.unwrap();

    let dirs = tempfile::tempdir().unwrap();
    let state = MountState::open(
        Arc::new(repo),
        &dirs.path().join("cache"),
        &dirs.path().join("state"),
        &"main".parse().unwrap(),
        MountOptions {
            read_only: true,
            ..MountOptions::default()
        },
    )
    .await
    .unwrap();
    Fixture {
        backend,
        state,
        src,
        _dirs: dirs,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Node {
    File(u16, Vec<u8>),
    Dir(u16),
    Link(Vec<u8>),
}

async fn walk_mount(state: &MountState) -> BTreeMap<String, Node> {
    let mut out = BTreeMap::new();
    let mut stack = vec![(1u64, String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for item in state.readdir(dir).await.unwrap() {
            let name = String::from_utf8(item.name.clone()).unwrap();
            let path = format!("{prefix}{name}");
            let attr = state.lookup(dir, &item.name).await.unwrap();
            assert_eq!(attr.kind, item.kind);
            let node = match attr.kind {
                FileKind::Dir => {
                    stack.push((attr.ino, format!("{path}/")));
                    Node::Dir(attr.mode)
                }
                FileKind::Symlink => Node::Link(state.readlink(attr.ino).await.unwrap()),
                FileKind::File => {
                    let data = read_all(state, attr.ino).await;
                    assert_eq!(data.len() as u64, attr.size);
                    Node::File(attr.mode, data)
                }
            };
            out.insert(path, node);
        }
    }
    out
}

fn walk_source(root: &Path) -> BTreeMap<String, Node> {
    let mut out = BTreeMap::new();
    let mut stack = vec![(root.to_path_buf(), String::new())];
    while let Some((dir, prefix)) = stack.pop() {
        for e in fs::read_dir(&dir).unwrap() {
            let e = e.unwrap();
            let path = format!("{prefix}{}", e.file_name().to_str().unwrap());
            let m = fs::symlink_metadata(e.path()).unwrap();
            let mode = (m.mode() & 0o7777) as u16;
            let node = if m.file_type().is_symlink() {
                Node::Link(
                    fs::read_link(e.path())
                        .unwrap()
                        .into_os_string()
                        .into_encoded_bytes(),
                )
            } else if m.is_dir() {
                stack.push((e.path(), format!("{path}/")));
                Node::Dir(mode)
            } else {
                Node::File(mode, fs::read(e.path()).unwrap())
            };
            out.insert(path, node);
        }
    }
    out
}

#[tokio::test]
async fn the_mount_matches_the_source() {
    let f = mount().await;
    assert_eq!(walk_mount(&f.state).await, walk_source(f.src.path()));
}

#[tokio::test]
async fn stat_and_open_download_no_file_contents() {
    let f = mount().await;
    let ino = lookup_path(&f.state, "big.bin").await.unwrap();
    let attr = f.state.getattr(ino).await.unwrap();
    assert_eq!(attr.size, BIG as u64);
    let fh = f.state.open_file(ino, false).await.unwrap();
    f.state.release(fh).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(f.backend.chunk_gets.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reading_the_first_mib_downloads_a_few_chunks() {
    let f = mount().await;
    let ino = lookup_path(&f.state, "big.bin").await.unwrap();
    let fh = f.state.open_file(ino, false).await.unwrap();
    let mut got = Vec::new();
    while got.len() < 1 << 20 {
        got.extend(
            f.state
                .read(fh, ino, got.len() as u64, 128 * 1024)
                .await
                .unwrap(),
        );
    }
    assert_eq!(got, random_bytes(1, BIG)[..got.len()]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let gets = f.backend.chunk_gets.load(Ordering::SeqCst);
    assert!((1..=10).contains(&gets), "{gets} chunk GETs for 1 MiB");
}

#[tokio::test]
async fn every_chunk_is_downloaded_once_and_then_served_from_cache() {
    let f = mount().await;
    let ino = lookup_path(&f.state, "big.bin").await.unwrap();
    assert_eq!(read_all(&f.state, ino).await, random_bytes(1, BIG));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let chunks = packed_chunks(&f.backend.inner).await.len();
    // big.bin and one_chunk.bin share no chunks; big.bin has all but one. Readahead fetches
    // runs of them with one GET, and every byte comes down once, except that the first chunk
    // may come twice: the reads before the stream is recognised as sequential fetch blocks,
    // and caching those runs in the background.
    let gets = f.backend.chunk_gets.load(Ordering::SeqCst);
    assert!(
        gets < (chunks - 1) / 2,
        "{gets} GETs for {} chunks",
        chunks - 1
    );
    let fetched = f.state.cache_stats().unwrap().fetched_bytes;
    assert!(
        (BIG as u64..=BIG as u64 + (4 << 20)).contains(&fetched),
        "{fetched}"
    );
    read_all(&f.state, ino).await;
    assert_eq!(f.backend.chunk_gets.load(Ordering::SeqCst), gets);
}

#[tokio::test]
async fn random_reads_return_the_right_bytes() {
    let f = mount().await;
    let data = random_bytes(1, BIG);
    let ino = lookup_path(&f.state, "big.bin").await.unwrap();
    let fh = f.state.open_file(ino, false).await.unwrap();
    for (off, len) in [
        (0, 10),
        (5_000_000, 3_000_000),
        (BIG - 7, 100),
        (BIG + 5, 10),
    ] {
        let got = f.state.read(fh, ino, off as u64, len).await.unwrap();
        let end = (off + len as usize).min(BIG);
        let want = if off >= BIG { &[][..] } else { &data[off..end] };
        assert_eq!(got, want, "read({off}, {len})");
    }
}

#[tokio::test]
async fn missing_names_and_links() {
    let f = mount().await;
    assert_eq!(lookup_path(&f.state, "nope").await, Err(Errno::ENOENT));
    assert_eq!(
        lookup_path(&f.state, "small.txt/x").await,
        Err(Errno::ENOTDIR)
    );
    let link = lookup_path(&f.state, "link").await.unwrap();
    assert_eq!(f.state.readlink(link).await.unwrap(), b"small.txt");
}

#[tokio::test]
async fn lookups_are_counted_and_forgotten() {
    let f = mount().await;
    let a = lookup_path(&f.state, "small.txt").await.unwrap();
    let b = lookup_path(&f.state, "small.txt").await.unwrap();
    assert_eq!(a, b);
    f.state.forget(a, 2);
    assert_eq!(f.state.getattr(a).await, Err(Errno::ENOENT));
    f.state.forget(1, 1);
    assert!(
        f.state.getattr(1).await.is_ok(),
        "the root is never forgotten"
    );
}

#[tokio::test]
async fn a_corrupt_chunk_reads_as_eio() {
    let f = mount().await;
    corrupt_chunks(&f.backend.inner).await;
    let ino = lookup_path(&f.state, "one_chunk.bin").await.unwrap();
    let fh = f.state.open_file(ino, false).await.unwrap();
    // A read of the whole chunk verifies it.
    assert_eq!(f.state.read(fh, ino, 0, 200_000).await, Err(Errno::EIO));
}

#[tokio::test]
async fn a_corrupt_chunk_filled_by_blocks_is_evicted_once_complete() {
    let f = mount().await;
    corrupt_chunks(&f.backend.inner).await;
    let ino = lookup_path(&f.state, "one_chunk.bin").await.unwrap();
    let fh = f.state.open_file(ino, false).await.unwrap();
    // Partial reads can't be verified: the first block is served as stored.
    let first = f.state.read(fh, ino, 0, 10).await.unwrap();
    assert_eq!(first, random_bytes(2, 100_000)[..10]);
    // Reading the second (corrupt) block, on another handle so it isn't taken for a
    // sequential read, completes the chunk; verification then evicts it.
    let other = f.state.open_file(ino, false).await.unwrap();
    f.state.read(other, ino, 70_000, 10).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(f.state.cache_stats().unwrap().objects, 0);
}

#[tokio::test]
async fn a_random_read_downloads_one_block() {
    let f = mount().await;
    let data = random_bytes(1, BIG);
    let ino = lookup_path(&f.state, "big.bin").await.unwrap();
    let fh = f.state.open_file(ino, false).await.unwrap();
    let off = 13_000_000;
    let got = f.state.read(fh, ino, off, 4096).await.unwrap();
    assert_eq!(got, &data[off as usize..off as usize + 4096]);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let fetched = f.state.cache_stats().unwrap().fetched_bytes;
    assert!(fetched <= 2 * 64 * 1024, "{fetched} bytes for a 4 KiB read");
    // Cached: reading it again downloads nothing, even in a new mount of the same cache.
    let gets = f.backend.chunk_gets.load(Ordering::SeqCst);
    assert_eq!(
        f.state.read(fh, ino, off + 100, 100).await.unwrap(),
        &data[off as usize + 100..off as usize + 200]
    );
    assert_eq!(f.backend.chunk_gets.load(Ordering::SeqCst), gets);
}

#[tokio::test]
async fn the_metadata_tree_is_prefetched_in_the_background() {
    let f = mount().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = f.backend.meta_gets.load(Ordering::SeqCst);
    assert!(before >= 5, "trees fetched in the background: {before}");
    let deep = lookup_path(&f.state, "a/b/c/deep.txt").await.unwrap();
    assert_eq!(read_all(&f.state, deep).await, b"deep");
    assert_eq!(f.backend.meta_gets.load(Ordering::SeqCst), before);
}

#[tokio::test]
async fn read_only_mounts_refuse_writes() {
    let f = mount().await;
    let ino = lookup_path(&f.state, "small.txt").await.unwrap();
    assert_eq!(f.state.open_file(ino, true).await, Err(Errno::EROFS));
}

#[tokio::test]
async fn a_fresh_mount_reads_everything_without_the_index() {
    let f = mount().await;
    let files = walk_mount(&f.state).await;
    assert_eq!(files.len(), 9);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(f.backend.index_requests.load(Ordering::SeqCst), 0);
}
