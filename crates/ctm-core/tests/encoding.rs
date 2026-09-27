//! Encoding: round-trips, strict canonical decoding, and pinned hex vectors.

use ctm_core::{
    Chunk, ChunkList, ChunkPage, ChunkRef, Commit, CommitKind, Content, DecodeError, DirEntry,
    Encoded, FormatParams, Id, LogEntry, LogSegment, Object, ObjectType, PageRef, RepoKey, Tree,
    Xattr,
};
use proptest::collection::{btree_map, vec};
use proptest::prelude::*;

const P: FormatParams = FormatParams::DEFAULT;

fn arb_id() -> impl Strategy<Value = Id> {
    any::<[u8; 32]>().prop_map(Id)
}

fn arb_name() -> impl Strategy<Value = Vec<u8>> {
    // Map bytes instead of filtering, so long runs never hit proptest's reject limit.
    vec(1u8..=254, 1..=40).prop_map(|mut n| {
        for b in &mut n {
            if *b >= b'/' {
                *b += 1; // skip '/'
            }
        }
        if n == b"." || n == b".." {
            n = b"x".to_vec();
        }
        n
    })
}

fn arb_content() -> impl Strategy<Value = (Content, u64)> {
    let max = u64::from(P.chunker.max);
    prop_oneof![
        vec(any::<u8>(), 0..=4096).prop_map(|b| {
            let n = b.len() as u64;
            (Content::Inline(b), n)
        }),
        (arb_id(), 4097..=max).prop_map(|(id, n)| (Content::Chunk(id), n)),
        (arb_id(), 4097..=u64::MAX / 2).prop_map(|(id, n)| (Content::ChunkList(id), n)),
        arb_id().prop_map(|id| (Content::Dir(id), 0)),
        vec(1u8..=255, 1..=200).prop_map(|t| {
            let n = t.len() as u64;
            (Content::Symlink(t), n)
        }),
    ]
}

fn arb_xattrs() -> impl Strategy<Value = Option<Vec<Xattr>>> {
    proptest::option::of(
        btree_map(vec(1u8..=255, 1..=20), vec(any::<u8>(), 0..=64), 0..4).prop_map(|m| {
            m.into_iter()
                .map(|(name, value)| Xattr { name, value })
                .collect()
        }),
    )
}

fn entry(name: Vec<u8>, (content, size): (Content, u64), mode: u16, mtime_ns: i64) -> DirEntry {
    let mode = if matches!(content, Content::Symlink(_)) {
        0o777
    } else {
        mode
    };
    DirEntry {
        name,
        mode,
        mtime_ns,
        size,
        content,
        btime_ns: None,
        xattrs: None,
    }
}

fn arb_entry_for(name: Vec<u8>) -> impl Strategy<Value = DirEntry> {
    (
        arb_content(),
        0u16..=0o7777,
        any::<i64>(),
        proptest::option::of(any::<i64>()),
        arb_xattrs(),
    )
        .prop_map(move |(c, mode, mtime, btime_ns, xattrs)| DirEntry {
            btime_ns,
            xattrs,
            ..entry(name.clone(), c, mode, mtime)
        })
}

fn arb_tree() -> impl Strategy<Value = Tree> {
    btree_map(arb_name(), Just(()), 0..30)
        .prop_flat_map(|names| names.into_keys().map(arb_entry_for).collect::<Vec<_>>())
        .prop_map(|entries| Tree { entries })
}

fn arb_page() -> impl Strategy<Value = ChunkPage> {
    vec(
        (arb_id(), 1..=P.chunker.max).prop_map(|(id, len)| ChunkRef { id, len }),
        1..=64,
    )
    .prop_map(|chunks| ChunkPage { chunks })
}

fn arb_list() -> impl Strategy<Value = ChunkList> {
    vec(
        (arb_id(), 1..=u64::MAX / 1024).prop_map(|(id, total_len)| PageRef { id, total_len }),
        1..=32,
    )
    .prop_map(|pages| ChunkList { pages })
}

fn arb_kind() -> impl Strategy<Value = CommitKind> {
    (1u8..=6).prop_map(|k| CommitKind::from_u8(k).unwrap())
}

fn arb_commit() -> impl Strategy<Value = Commit> {
    (
        arb_id(),
        any::<i64>(),
        "[a-z]{1,8}@[a-z0-9-]{1,20}",
        any::<[u8; 16]>(),
        arb_kind(),
        ".{0,200}",
    )
        .prop_map(
            |(root_tree, time_ns, author, machine_id, kind, message)| Commit {
                root_tree,
                time_ns,
                author,
                machine_id,
                kind,
                message,
            },
        )
}

fn arb_log() -> impl Strategy<Value = LogSegment> {
    (
        proptest::option::of(arb_id()),
        vec(
            (any::<i64>(), arb_id(), arb_kind(), ".{0,80}").prop_map(
                |(time_ns, commit, kind, message)| LogEntry {
                    time_ns,
                    commit,
                    kind,
                    message,
                },
            ),
            1..=50,
        ),
    )
        .prop_map(|(prev, entries)| LogSegment { prev, entries })
}

fn round_trip<T: Object + PartialEq + std::fmt::Debug>(obj: &T) {
    let bytes = obj.encode();
    assert_eq!(&T::decode(&bytes, &P).unwrap(), obj);
    // Strict decoding: no trailing bytes, and no strict prefix decodes.
    let mut longer = bytes.clone();
    longer.push(0);
    assert_eq!(T::decode(&longer, &P), Err(DecodeError::TrailingBytes(1)));
    for cut in [0, bytes.len() / 2, bytes.len().saturating_sub(1)] {
        if cut < bytes.len() {
            assert!(
                T::decode(&bytes[..cut], &P).is_err(),
                "prefix {cut} decoded"
            );
        }
    }
}

proptest! {
    #[test]
    fn tree_round_trips(t in arb_tree()) { round_trip(&t); }

    #[test]
    fn chunk_page_round_trips(p in arb_page()) { round_trip(&p); }

    #[test]
    fn chunk_list_round_trips(l in arb_list()) { round_trip(&l); }

    #[test]
    fn commit_round_trips(c in arb_commit()) { round_trip(&c); }

    #[test]
    fn log_segment_round_trips(s in arb_log()) { round_trip(&s); }

    #[test]
    fn chunk_round_trips(b in vec(any::<u8>(), 1..10_000)) {
        let c = Chunk(b);
        assert_eq!(Chunk::decode(&c.encode(), &P).unwrap(), c);
    }

    #[test]
    fn encoding_is_deterministic(t in arb_tree()) {
        assert_eq!(t.encode(), t.clone().encode());
    }
}

fn file(name: &str, data: &[u8]) -> DirEntry {
    entry(
        name.as_bytes().to_vec(),
        (Content::Inline(data.to_vec()), data.len() as u64),
        0o644,
        1_700_000_000_000_000_000,
    )
}

#[test]
fn unsorted_or_duplicate_entries_are_rejected() {
    let mut unsorted = Tree {
        entries: vec![file("b", b"1"), file("a", b"2")],
    }
    .encode();
    assert_eq!(Tree::decode(&unsorted, &P), Err(DecodeError::Unsorted));
    unsorted = Tree {
        entries: vec![file("a", b"1"), file("a", b"2")],
    }
    .encode();
    assert_eq!(Tree::decode(&unsorted, &P), Err(DecodeError::Unsorted));
}

#[test]
fn invalid_names_are_rejected() {
    for name in [&b"."[..], b"..", b"a/b", b"a\0b", b""] {
        let bytes = Tree {
            entries: vec![DirEntry {
                name: name.to_vec(),
                ..file("x", b"")
            }],
        }
        .encode();
        assert!(Tree::decode(&bytes, &P).is_err(), "{name:?} accepted");
    }
}

#[test]
fn kind_must_match_content() {
    // Tree: count u32, then the entry: name_len u8, name, kind u8, …
    let mut bytes = Tree {
        entries: vec![file("f", b"hello")],
    }
    .encode();
    let kind_at = 4 + 1 + 1;
    assert_eq!(bytes[kind_at], 1);
    bytes[kind_at] = 2;
    assert!(Tree::decode(&bytes, &P).is_err());
}

#[test]
fn inline_size_and_chunk_size_limits() {
    let big_inline = Tree {
        entries: vec![file("f", &[7u8; 4097])],
    }
    .encode();
    assert!(Tree::decode(&big_inline, &P).is_err());
    let small_chunk = Tree {
        entries: vec![entry(
            b"f".to_vec(),
            (Content::Chunk(Id([1; 32])), 4096),
            0o644,
            0,
        )],
    }
    .encode();
    assert!(Tree::decode(&small_chunk, &P).is_err());
}

#[test]
fn ids_are_keyed_and_typed() {
    let a = RepoKey([1; 32]);
    let b = RepoKey([2; 32]);
    let payload = b"same bytes";
    assert_ne!(
        Id::compute(&a, ObjectType::Chunk, payload),
        Id::compute(&b, ObjectType::Chunk, payload)
    );
    assert_ne!(
        Id::compute(&a, ObjectType::Chunk, payload),
        Id::compute(&a, ObjectType::Tree, payload)
    );
}

#[test]
fn verified_decode_catches_corruption() {
    use ctm_core::encoding::{VerifyError, decode_verified};
    let key = RepoKey([9; 32]);
    let tree = Tree {
        entries: vec![file("a", b"x")],
    };
    let enc = Encoded::new(&key, &tree);
    let mut stored = enc.to_stored();
    assert_eq!(
        decode_verified::<Tree>(&key, &enc.id, &stored, &P).unwrap(),
        tree
    );
    let last = stored.len() - 1;
    stored[last] ^= 1;
    assert!(matches!(
        decode_verified::<Tree>(&key, &enc.id, &stored, &P),
        Err(VerifyError::HashMismatch { .. })
    ));
}

/// One fixed example of every type, so an accidental encoding change shows up as a diff.
#[test]
fn pinned_vectors() {
    let key = RepoKey([0x42; 32]);
    let id = |b: u8| Id([b; 32]);
    let mut out = String::new();
    let mut add = |name: &str, enc: Encoded| {
        out += &format!(
            "{name}\n  id      {}\n  payload {}\n",
            enc.id,
            hex(&enc.payload)
        );
    };
    add("chunk", Encoded::new(&key, &Chunk(b"continuum".to_vec())));
    add(
        "chunk_page",
        Encoded::new(
            &key,
            &ChunkPage {
                chunks: vec![
                    ChunkRef {
                        id: id(1),
                        len: 1 << 20,
                    },
                    ChunkRef { id: id(2), len: 7 },
                ],
            },
        ),
    );
    add(
        "chunk_list",
        Encoded::new(
            &key,
            &ChunkList {
                pages: vec![PageRef {
                    id: id(3),
                    total_len: 4 << 30,
                }],
            },
        ),
    );
    add(
        "tree",
        Encoded::new(
            &key,
            &Tree {
                entries: vec![
                    file("a.txt", b"hi"),
                    entry(b"dir".to_vec(), (Content::Dir(id(4)), 0), 0o755, 5),
                    entry(
                        b"link".to_vec(),
                        (Content::Symlink(b"a.txt".to_vec()), 5),
                        0,
                        6,
                    ),
                ],
            },
        ),
    );
    add(
        "commit",
        Encoded::new(
            &key,
            &Commit {
                root_tree: id(5),
                time_ns: 1_758_888_000_000_000_000,
                author: "me@laptop".into(),
                machine_id: [0xab; 16],
                kind: CommitKind::Manual,
                message: "first".into(),
            },
        ),
    );
    add(
        "log_segment",
        Encoded::new(
            &key,
            &LogSegment {
                prev: Some(id(6)),
                entries: vec![LogEntry {
                    time_ns: 1,
                    commit: id(7),
                    kind: CommitKind::Import,
                    message: String::new(),
                }],
            },
        ),
    );
    insta::assert_snapshot!(out);
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
