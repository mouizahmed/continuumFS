//! The local caches: sparse chunk files filled whole or by 64 KiB block, with LRU eviction
//! tracked in `cache.db`, and never-evicted metadata files. Several mount processes share one
//! cache directory.

use ctm_core::Id;
use ctm_store::cache::{BLOCK, ChunkCache, MetaCache, all_blocks, blocks_of};

fn id(n: u8) -> Id {
    Id([n; 32])
}

fn whole(cache: &ChunkCache, n: u8, len: u32) -> Option<Vec<u8>> {
    cache.read(&id(n), len, 0..len).unwrap()
}

#[test]
fn chunks_round_trip_and_count_hits_and_misses() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    assert!(whole(&cache, 1, 3).is_none());
    cache.insert(&id(1), b"abc").unwrap();
    assert_eq!(whole(&cache, 1, 3).unwrap(), b"abc");
    assert_eq!(cache.read(&id(1), 3, 1..2).unwrap().unwrap(), b"b");
    cache.record_fetch(3);
    cache.flush().unwrap();
    let s = cache.stats().unwrap();
    assert_eq!((s.bytes, s.objects), (3, 1));
    assert_eq!((s.hits, s.misses, s.fetched_bytes), (2, 1, 3));
}

#[test]
fn block_bitmaps() {
    assert_eq!(all_blocks(1), 1);
    assert_eq!(all_blocks(BLOCK), 1);
    assert_eq!(all_blocks(BLOCK + 1), 0b11);
    assert_eq!(all_blocks(4 << 20), u64::MAX);
    assert_eq!(blocks_of(&(0..1)), 1);
    assert_eq!(blocks_of(&(BLOCK - 1..BLOCK + 1)), 0b11);
    assert_eq!(blocks_of(&(3 * BLOCK..4 * BLOCK)), 0b1000);
    assert_eq!(blocks_of(&(5..5)), 0);
    assert_eq!(blocks_of(&(0..4 << 20)), u64::MAX);
}

#[test]
fn blocks_fill_a_chunk_until_it_is_complete_and_verified() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 30).unwrap();
    let len = 2 * BLOCK + 100;
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    let b = BLOCK as usize;
    assert!(
        !cache
            .insert_blocks(&id(1), len, 1, &data[b..2 * b])
            .unwrap()
    );
    // Only block 1 is there.
    let mid = BLOCK + 10..BLOCK + 20;
    assert_eq!(
        cache.read(&id(1), len, mid.clone()).unwrap().unwrap(),
        &data[mid.start as usize..mid.end as usize]
    );
    assert!(cache.read(&id(1), len, 0..10).unwrap().is_none());
    assert!(
        cache
            .read(&id(1), len, BLOCK - 1..BLOCK + 1)
            .unwrap()
            .is_none()
    );
    assert!(!cache.has_all(&id(1), len).unwrap());
    assert_eq!(cache.stats().unwrap().bytes, u64::from(BLOCK));
    // The last two blocks (the second one short) complete it: time to verify.
    assert!(!cache.insert_blocks(&id(1), len, 0, &data[..b]).unwrap());
    assert!(cache.insert_blocks(&id(1), len, 2, &data[2 * b..]).unwrap());
    assert!(cache.has_all(&id(1), len).unwrap());
    assert_eq!(cache.stats().unwrap().bytes, u64::from(len));
    assert!(
        cache
            .verify(&id(1), len, |bytes| bytes == &data[..])
            .unwrap()
    );
    assert_eq!(whole(&cache, 1, len).unwrap(), data);
    // Verified chunks aren't checked again.
    assert!(cache.verify(&id(1), len, |_| false).unwrap());
}

#[test]
fn a_chunk_that_fails_verification_is_evicted() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 30).unwrap();
    assert!(cache.insert_blocks(&id(1), 10, 0, b"0123456789").unwrap());
    assert!(!cache.verify(&id(1), 10, |_| false).unwrap());
    assert!(whole(&cache, 1, 10).is_none());
    assert_eq!(cache.stats().unwrap().objects, 0);
}

#[test]
fn a_file_cut_short_is_a_miss() {
    // After a power loss, a renamed but unsynced file can come back empty.
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    cache.insert(&id(1), b"abcd").unwrap();
    for f in walk(&dir.path().join("chunks")) {
        std::fs::write(f, b"").unwrap();
    }
    assert!(whole(&cache, 1, 4).is_none());
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

#[test]
fn evicts_least_recently_used_over_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let cache = ChunkCache::open(dir.path(), 3000).unwrap();
    for n in 1..=3 {
        cache.insert(&id(n), &[n; 1000]).unwrap();
    }
    // Touch 1, so 2 is now the least recently used.
    assert!(whole(&cache, 1, 1000).is_some());
    cache.flush().unwrap();
    cache.insert(&id(4), &[4; 1000]).unwrap();
    assert!(whole(&cache, 2, 1000).is_none());
    for n in [1, 3, 4] {
        assert!(whole(&cache, n, 1000).is_some(), "{n} evicted");
    }
    assert!(cache.stats().unwrap().bytes <= 3000);
    assert_eq!(walk(&dir.path().join("chunks")).len(), 3);
}

#[test]
fn two_processes_share_one_cache() {
    let dir = tempfile::tempdir().unwrap();
    let a = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    let b = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    a.insert(&id(1), b"from a").unwrap();
    assert_eq!(whole(&b, 1, 6).unwrap(), b"from a");
    // Both insert the same chunk at once: both succeed.
    a.insert(&id(2), b"same").unwrap();
    b.insert(&id(2), b"same").unwrap();
    let s = b.stats().unwrap();
    assert_eq!((s.objects, s.bytes), (2, 10));
    // Blocks one process adds are visible to the other.
    a.insert_blocks(&id(3), 2 * BLOCK, 1, &vec![3; BLOCK as usize])
        .unwrap();
    assert!(
        b.read(&id(3), 2 * BLOCK, BLOCK..BLOCK + 1)
            .unwrap()
            .is_some()
    );
    // One evicts what the other remembers: the other sees a miss, not stale data.
    assert!(whole(&a, 1, 6).is_some());
    b.evict(&id(1)).unwrap();
    assert!(whole(&a, 1, 6).is_none());
}

#[test]
fn stats_persist_across_opens() {
    let dir = tempfile::tempdir().unwrap();
    {
        let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
        cache.insert(&id(1), b"xyz").unwrap();
        assert!(whole(&cache, 9, 1).is_none());
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
fn a_cache_from_an_older_version_is_replaced() {
    let dir = tempfile::tempdir().unwrap();
    // What v0.2 left: whole-chunk files and a `chunks` table without generations.
    std::fs::create_dir_all(dir.path().join("chunks/ab")).unwrap();
    std::fs::write(dir.path().join("chunks/ab/old"), b"old chunk").unwrap();
    let db = rusqlite::Connection::open(dir.path().join("cache.db")).unwrap();
    db.execute_batch(
        "CREATE TABLE chunks (id BLOB PRIMARY KEY, size INTEGER NOT NULL,
                              last_access INTEGER NOT NULL);
         INSERT INTO chunks VALUES (x'01', 9, 1);",
    )
    .unwrap();
    drop(db);
    let cache = ChunkCache::open(dir.path(), 1 << 20).unwrap();
    assert_eq!(cache.stats().unwrap().objects, 0);
    assert!(!dir.path().join("chunks").exists());
    cache.insert(&id(1), b"new").unwrap();
    assert_eq!(whole(&cache, 1, 3).unwrap(), b"new");
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
