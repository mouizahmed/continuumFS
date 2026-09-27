//! Packfiles, location hints, and index segments.

use ctm_core::pack::{
    Entry, HINT_LEN, Location, PackBuilder, PackId, decode_index, encode_index, read_trailer,
};
use ctm_core::{Chunk, Commit, CommitKind, Encoded, Id, RepoKey};

fn objects() -> Vec<Encoded> {
    let key = RepoKey([3; 32]);
    let mut out: Vec<Encoded> = (0..5u8)
        .map(|i| Encoded::new(&key, &Chunk(vec![i; 1000 + i as usize])))
        .collect();
    out.push(Encoded::new(
        &key,
        &Commit {
            root_tree: Id([1; 32]),
            time_ns: 5,
            author: "a@b".into(),
            machine_id: [0; 16],
            kind: CommitKind::Manual,
            message: "m".into(),
        },
    ));
    out
}

/// Builds a pack of `objects()`; the commit's one reference gets `hint`.
fn build(id: PackId, hint: Option<Location>) -> (Vec<u8>, Vec<Location>) {
    let mut b = PackBuilder::new(id);
    let locs = objects()
        .iter()
        .map(|o| b.add(o, &vec![hint; o.refs.len()]))
        .collect();
    (b.finish().0, locs)
}

fn slice<'a>(pack: &'a [u8], loc: &Location) -> &'a [u8] {
    &pack[loc.range().start as usize..loc.range().end as usize]
}

#[test]
fn packs_round_trip_through_their_trailer() {
    let objs = objects();
    let hint = Location {
        pack: PackId([4; 16]),
        offset: 1234,
        len: 99,
    };
    let (pack, locs) = build(PackId([9; 16]), Some(hint));
    let (id, read) = read_trailer(&pack).unwrap();
    assert_eq!(id, PackId([9; 16]));
    for ((o, loc), (rid, rty, rloc)) in objs.iter().zip(&locs).zip(&read) {
        assert_eq!((*rid, *rty, rloc), (o.id, o.ty, loc));
        let e = Entry::parse(slice(&pack, loc)).unwrap();
        assert_eq!((e.ty, e.payload), (o.ty, &o.payload[..]));
        assert_eq!(e.to_stored(), o.to_stored());
        assert_eq!(e.hints.len(), o.refs.len());
    }
    let commit = Entry::parse(slice(&pack, &locs[5])).unwrap();
    assert_eq!(commit.hints, [Some(hint)]);
    assert!(locs[0].key(true).starts_with("packs/data/"));
    assert!(locs[5].key(false).starts_with("packs/meta/"));
}

#[test]
fn unknown_hints_are_zeros() {
    let (pack, locs) = build(PackId([9; 16]), None);
    let bytes = slice(&pack, &locs[5]);
    assert_eq!(&bytes[bytes.len() - HINT_LEN..], &[0; HINT_LEN]);
    assert_eq!(Entry::parse(bytes).unwrap().hints, [None]);
    assert_eq!(Location::from_hint(&[0; HINT_LEN]), None);
}

#[test]
fn entries_with_wrong_lengths_are_rejected() {
    let (pack, locs) = build(PackId([9; 16]), None);
    let loc = locs[5];
    let whole = slice(&pack, &loc);
    assert!(Entry::parse(&whole[..whole.len() - 1]).is_err());
    let longer = Location {
        len: loc.len + 1,
        ..loc
    };
    assert!(Entry::parse(slice(&pack, &longer)).is_err());
    let mut flagged = whole.to_vec();
    flagged[1] = 1;
    assert!(Entry::parse(&flagged).is_err());
}

#[test]
fn a_damaged_pack_is_rejected() {
    let (pack, _) = build(PackId([1; 16]), None);
    for at in [0, 30, pack.len() / 2, pack.len() - 13, pack.len() - 1] {
        let mut bad = pack.clone();
        bad[at] ^= 0xff;
        assert!(
            read_trailer(&bad).is_err(),
            "flipping byte {at} went unnoticed"
        );
    }
    assert!(read_trailer(&pack[..pack.len() - 1]).is_err());
}

#[test]
fn index_segments_round_trip_and_reject_damage() {
    let (pack, _) = build(PackId([2; 16]), None);
    let (_, entries) = read_trailer(&pack).unwrap();
    let seg = encode_index(&entries);
    assert_eq!(decode_index(&seg).unwrap(), entries);
    assert_eq!(decode_index(&encode_index(&[])).unwrap(), vec![]);
    for at in [0, 9, seg.len() / 2, seg.len() - 1] {
        let mut bad = seg.clone();
        bad[at] ^= 0xff;
        assert!(
            decode_index(&bad).is_err(),
            "flipping byte {at} went unnoticed"
        );
    }
}

#[test]
fn pack_ids_print_as_hex() {
    let id = PackId([0xab; 16]);
    assert_eq!(id.to_string(), "ab".repeat(16));
    assert_eq!(id.to_string().parse::<PackId>().unwrap(), id);
}

#[test]
fn objects_list_their_references_in_payload_order() {
    use ctm_core::{ChunkPage, ChunkRef, LogEntry, LogSegment, Object};
    let page = ChunkPage {
        chunks: vec![
            ChunkRef {
                id: Id([1; 32]),
                len: 5,
            },
            ChunkRef {
                id: Id([2; 32]),
                len: 5,
            },
        ],
    };
    assert_eq!(page.refs(), [Id([1; 32]), Id([2; 32])]);
    let log = LogSegment {
        prev: Some(Id([7; 32])),
        entries: vec![LogEntry {
            time_ns: 1,
            commit: Id([8; 32]),
            kind: CommitKind::Manual,
            message: String::new(),
        }],
    };
    assert_eq!(log.refs(), [Id([7; 32]), Id([8; 32])]);
}
