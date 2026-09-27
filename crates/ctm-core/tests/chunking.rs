//! Chunking, partial re-chunking, pagination, and tree diff.

use std::collections::BTreeMap;
use std::io;

use ctm_core::chunker::{chunk_all, next_cut};
use ctm_core::diff::{Change, diff_trees};
use ctm_core::layout::{PAGE_MAX, paginate};
use ctm_core::rechunk::{Changes, ReadAt, Segment, rechunk_partial};
use ctm_core::{ChunkRef, ChunkerParams, Content, DirEntry, Id, Tree};
use proptest::collection::vec;
use proptest::prelude::*;

const SMALL: ChunkerParams = ChunkerParams::TEST_SMALL;

fn random_bytes(seed: u64, len: usize) -> Vec<u8> {
    // xorshift: fast, deterministic, and incompressible enough for content-defined cuts.
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

proptest! {
    #[test]
    fn chunks_cover_the_input_within_bounds(seed: u64, len in 0usize..50_000) {
        let data = random_bytes(seed, len);
        let lens = chunk_all(&SMALL, &data);
        prop_assert_eq!(lens.iter().map(|&l| l as usize).sum::<usize>(), len);
        for (i, &l) in lens.iter().enumerate() {
            prop_assert!((1..=SMALL.max).contains(&l));
            if i + 1 < lens.len() {
                prop_assert!(l >= SMALL.min);
            }
        }
        prop_assert_eq!(chunk_all(&SMALL, &data), lens);
    }

    /// Feeding the cutter windows of any size gives the same cuts as chunking at once.
    #[test]
    fn streaming_cuts_match(seed: u64, len in 0usize..30_000, window in 1usize..3000) {
        let data = random_bytes(seed, len);
        let mut lens = Vec::new();
        let mut start = 0;
        let mut end = 0;
        while start < data.len() {
            end = (end + window).min(data.len()).max(end);
            match next_cut(&SMALL, &data[start..end], end == data.len()) {
                Some(n) => {
                    lens.push(n as u32);
                    start += n;
                    end = end.max(start);
                }
                None => prop_assert!(end < data.len(), "no cut at end of file"),
            }
        }
        prop_assert_eq!(lens, chunk_all(&SMALL, &data));
    }
}

#[derive(Debug, Clone)]
enum Edit {
    Write { at: usize, len: usize, seed: u64 },
    Truncate(usize),
}

fn arb_edit() -> impl Strategy<Value = Edit> {
    prop_oneof![
        3 => (0usize..40_000, 1usize..3000, any::<u64>())
            .prop_map(|(at, len, seed)| Edit::Write { at, len, seed }),
        1 => (0usize..40_000).prop_map(Edit::Truncate),
    ]
}

/// Applies edits the way working state does, tracking extents and `base_visible`.
fn apply(base: &[u8], edits: &[Edit]) -> (Vec<u8>, Changes) {
    let mut file = base.to_vec();
    let mut changes = Changes {
        base_len: base.len() as u64,
        base_visible: base.len() as u64,
        size: base.len() as u64,
        extents: Vec::new(),
    };
    for e in edits {
        match *e {
            Edit::Write { at, len, seed } => {
                if file.len() < at + len {
                    file.resize(at + len, 0);
                }
                file[at..at + len].copy_from_slice(&random_bytes(seed, len));
                changes.extents.push(at as u64..(at + len) as u64);
            }
            Edit::Truncate(n) => {
                file.resize(n, 0);
                changes.base_visible = changes.base_visible.min(n as u64);
                for r in &mut changes.extents {
                    r.end = r.end.min(n as u64);
                }
            }
        }
        changes.size = file.len() as u64;
    }
    changes.extents.retain(|r| r.start < r.end);
    changes.extents.sort_by_key(|r| r.start);
    let mut merged: Vec<std::ops::Range<u64>> = Vec::new();
    for r in changes.extents.drain(..) {
        match merged.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    changes.extents = merged;
    (file, changes)
}

struct Reader<'a> {
    data: &'a [u8],
    bytes_read: u64,
}

impl ReadAt for Reader<'_> {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()> {
        let start = offset as usize;
        buf.copy_from_slice(&self.data[start..start + buf.len()]);
        self.bytes_read += buf.len() as u64;
        Ok(())
    }
}

fn expand(segments: &[Segment], base: &[u32]) -> Vec<u32> {
    segments
        .iter()
        .map(|s| match *s {
            Segment::Base(i) => base[i],
            Segment::New { len, .. } => len,
        })
        .collect()
}

proptest! {
    /// The core promise of partial writes: the result equals a full re-chunk.
    #[test]
    fn partial_rechunk_equals_full_rechunk(
        seed: u64,
        base_len in 0usize..40_000,
        edits in vec(arb_edit(), 0..6),
    ) {
        let base = random_bytes(seed, base_len);
        let base_lens = chunk_all(&SMALL, &base);
        let (merged, changes) = apply(&base, &edits);
        let mut reader = Reader { data: &merged, bytes_read: 0 };
        let segments = rechunk_partial(&SMALL, &base_lens, &changes, &mut reader).unwrap();
        prop_assert_eq!(expand(&segments, &base_lens), chunk_all(&SMALL, &merged));

        // Kept base chunks must sit at their original offsets.
        let base_offsets: Vec<u64> = base_lens.iter().scan(0u64, |o, &l| { let s = *o; *o += u64::from(l); Some(s) }).collect();
        let mut offset = 0u64;
        for s in &segments {
            match *s {
                Segment::Base(i) => {
                    prop_assert_eq!(base_offsets[i], offset);
                    offset += u64::from(base_lens[i]);
                }
                Segment::New { offset: o, len } => {
                    prop_assert_eq!(o, offset);
                    offset += u64::from(len);
                }
            }
        }
    }
}

#[test]
fn append_reads_only_the_tail() {
    let base = random_bytes(7, 200_000);
    let base_lens = chunk_all(&SMALL, &base);
    let (merged, changes) = apply(
        &base,
        &[Edit::Write {
            at: base.len(),
            len: 1,
            seed: 1,
        }],
    );
    let mut reader = Reader {
        data: &merged,
        bytes_read: 0,
    };
    let segments = rechunk_partial(&SMALL, &base_lens, &changes, &mut reader).unwrap();
    assert_eq!(expand(&segments, &base_lens), chunk_all(&SMALL, &merged));
    assert!(
        reader.bytes_read <= u64::from(SMALL.max) + 1,
        "read {} bytes to append 1",
        reader.bytes_read
    );
    let kept = segments
        .iter()
        .filter(|s| matches!(s, Segment::Base(_)))
        .count();
    assert_eq!(kept, base_lens.len() - 1);
}

#[test]
fn pages_are_full_except_the_last() {
    for n in [0usize, 1, 4095, 4096, 4097, 8192, 10_000] {
        let chunks: Vec<ChunkRef> = (0..n)
            .map(|i| ChunkRef {
                id: Id([(i % 251) as u8; 32]),
                len: 1 + i as u32,
            })
            .collect();
        let pages = paginate(&chunks);
        assert_eq!(pages.len(), n.div_ceil(PAGE_MAX));
        for (i, p) in pages.iter().enumerate() {
            if i + 1 < pages.len() {
                assert_eq!(p.chunks.len(), PAGE_MAX);
            }
        }
        let flat: Vec<ChunkRef> = pages.into_iter().flat_map(|p| p.chunks).collect();
        assert_eq!(flat, chunks);
    }
}

fn arb_small_tree() -> impl Strategy<Value = Tree> {
    proptest::collection::btree_map("[a-e]{1,2}", (0u8..3, 0u8..4), 0..12).prop_map(|m| Tree {
        entries: m
            .into_iter()
            .map(|(name, (kind, v))| DirEntry {
                name: name.into_bytes(),
                mode: 0o644,
                mtime_ns: 0,
                size: if kind == 1 { 0 } else { 1 },
                content: match kind {
                    0 => Content::Inline(vec![v]),
                    1 => Content::Dir(Id([v; 32])),
                    _ => Content::Symlink(vec![b'a' + v]),
                },
                btime_ns: None,
                xattrs: None,
            })
            .collect(),
    })
}

proptest! {
    #[test]
    fn diff_matches_brute_force(old in arb_small_tree(), new in arb_small_tree()) {
        let a: BTreeMap<_, _> = old.entries.iter().map(|e| (e.name.clone(), e.clone())).collect();
        let b: BTreeMap<_, _> = new.entries.iter().map(|e| (e.name.clone(), e.clone())).collect();
        let mut names: Vec<_> = a.keys().chain(b.keys()).cloned().collect();
        names.sort();
        names.dedup();
        let expected: Vec<Change> = names
            .into_iter()
            .filter_map(|n| match (a.get(&n), b.get(&n)) {
                (Some(x), None) => Some(Change::Removed(x.clone())),
                (None, Some(y)) => Some(Change::Added(y.clone())),
                (Some(x), Some(y)) if x != y => Some(Change::Modified { old: x.clone(), new: y.clone() }),
                _ => None,
            })
            .collect();
        prop_assert_eq!(diff_trees(&old, &new), expected);
    }
}

/// The same promise with the real chunk sizes (256 KiB / 1 MiB / 4 MiB): edit the middle,
/// append, and truncate a 24 MiB file.
#[test]
fn partial_rechunk_with_default_params() {
    let p = ChunkerParams::DEFAULT;
    let base = random_bytes(42, 24 << 20);
    let base_lens = chunk_all(&p, &base);
    assert!(base_lens.len() > 8);
    for edits in [
        vec![Edit::Write {
            at: 10 << 20,
            len: 100,
            seed: 1,
        }],
        vec![Edit::Write {
            at: base.len(),
            len: 1,
            seed: 2,
        }],
        vec![Edit::Truncate((24 << 20) - 1)],
        vec![
            Edit::Write {
                at: 1 << 20,
                len: 5000,
                seed: 3,
            },
            Edit::Write {
                at: 20 << 20,
                len: 1,
                seed: 4,
            },
        ],
    ] {
        let (merged, changes) = apply(&base, &edits);
        let mut reader = Reader {
            data: &merged,
            bytes_read: 0,
        };
        let segments = rechunk_partial(&p, &base_lens, &changes, &mut reader).unwrap();
        assert_eq!(
            expand(&segments, &base_lens),
            chunk_all(&p, &merged),
            "{edits:?}"
        );
        assert!(
            reader.bytes_read < 16 << 20,
            "{edits:?} read {} bytes",
            reader.bytes_read
        );
    }
}
