//! The local caches: whole-chunk files with LRU eviction tracked in `cache.db`, and
//! never-evicted metadata files. Several mount processes share one cache directory.

use std::io::Read;

use ctm_core::Id;
use ctm_store::cache::{ChunkCache, MetaCache};

fn id(n: u8) -> Id {
    Id([n; 32])
}

fn read_all(mut f: std::fs::File) -> Vec<u8> {
    let mut v = Vec::new();
    f.read_to_end(&mut v).unwrap();
    v
}

#[test]
fn chunks_round_trip_and_count_hits_and_misses() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    assert!(cache.get(&id(1), 3).unwrap().is_none());
    cache.insert(&id(1), b"abc").unwrap();
    assert_eq!(read_all(cache.get(&id(1), 3).unwrap().unwrap()), b"abc");
    cache.record_fetch(3);
    cache.flush().unwrap();
    let s = cache.stats().unwrap();
    assert_eq!((s.bytes, s.objects), (3, 1));
    assert_eq!((s.hits, s.misses, s.fetched_bytes), (1, 1, 3));
}

#[test]
fn a_file_of_the_wrong_length_is_a_miss() {
    // After a power loss, a renamed but unsynced file can come back empty.
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    cache.insert(&id(1), b"abcd").unwrap();
    assert!(cache.get(&id(1), 5).unwrap().is_none());
}

#[test]
fn evicts_least_recently_used_over_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 3000).unwrap();
    for n in 1..=3 {
        cache.insert(&id(n), &[n; 1000]).unwrap();
    }
    // Touch 1, so 2 is now the least recently used.
    assert!(cache.get(&id(1), 1000).unwrap().is_some());
    cache.flush().unwrap();
    cache.insert(&id(4), &[4; 1000]).unwrap();
    assert!(cache.get(&id(2), 1000).unwrap().is_none());
    for n in [1, 3, 4] {
        assert!(cache.get(&id(n), 1000).unwrap().is_some(), "{n} evicted");
    }
    assert!(cache.stats().unwrap().bytes <= 3000);
}

#[test]
fn an_open_chunk_survives_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1000).unwrap();
    cache.insert(&id(1), &[1; 1000]).unwrap();
    let open = cache.get(&id(1), 1000).unwrap().unwrap();
    cache.insert(&id(2), &[2; 1000]).unwrap();
    assert!(cache.get(&id(1), 1000).unwrap().is_none());
    assert_eq!(read_all(open), vec![1; 1000]);
}

#[test]
fn two_processes_share_one_cache() {
    let dir = tempfile::tempdir().unwrap();
    let a = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    let b = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    a.insert(&id(1), b"from a").unwrap();
    assert_eq!(read_all(b.get(&id(1), 6).unwrap().unwrap()), b"from a");
    // Both insert the same chunk at once: both succeed.
    a.insert(&id(2), b"same").unwrap();
    b.insert(&id(2), b"same").unwrap();
    let s = b.stats().unwrap();
    assert_eq!((s.objects, s.bytes), (2, 10));
}

#[test]
fn stats_persist_across_opens() {
    let dir = tempfile::tempdir().unwrap();
    {
        let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
        cache.insert(&id(1), b"xyz").unwrap();
        assert!(cache.get(&id(9), 1).unwrap().is_none());
        cache.record_fetch(3);
        // Dropping flushes the batched counters.
    }
    let s = ChunkCache::open(dir.path(), 1 << 20)
        .unwrap()
        .stats()
        .unwrap();
    assert_eq!((s.objects, s.misses, s.fetched_bytes), (1, 1, 3));
}

#[test]
fn metadata_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let meta = MetaCache::open(dir.path()).unwrap();
    assert_eq!(meta.get(&id(1)).unwrap(), None);
    meta.insert(&id(1), b"tree bytes").unwrap();
    assert_eq!(meta.get(&id(1)).unwrap().unwrap(), b"tree bytes");
    let again = MetaCache::open(dir.path()).unwrap();
    assert_eq!(again.get(&id(1)).unwrap().unwrap(), b"tree bytes");
}
