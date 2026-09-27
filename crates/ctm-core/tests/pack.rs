//! Packfiles and index segments.

use ctm_core::pack::{PackBuilder, PackId, decode_index, encode_index, read_trailer};
use ctm_core::{Chunk, Commit, CommitKind, Encoded, Id, ObjectType, RepoKey};

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

#[test]
fn packs_round_trip_through_their_trailer() {
    let objs = objects();
    let mut b = PackBuilder::new(PackId([9; 16]));
    let locs: Vec<_> = objs.iter().map(|o| b.add(o)).collect();
    let (pack, listed) = b.finish();
    let (id, read) = read_trailer(&pack).unwrap();
    assert_eq!(id, PackId([9; 16]));
    assert_eq!(read, listed);
    for ((o, loc), (rid, rloc)) in objs.iter().zip(&locs).zip(&read) {
        assert_eq!((*rid, rloc), (o.id, loc));
        let r = loc.range();
        assert_eq!(&pack[r.start as usize..r.end as usize], &o.payload[..]);
    }
    assert_eq!(read[5].1.ty, ObjectType::Commit);
    assert!(read[0].1.pack_key().starts_with("packs/data/"));
    assert!(read[5].1.pack_key().starts_with("packs/meta/"));
}

#[test]
fn a_damaged_pack_is_rejected() {
    let mut b = PackBuilder::new(PackId([1; 16]));
    for o in objects() {
        b.add(&o);
    }
    let (pack, _) = b.finish();
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
    let mut b = PackBuilder::new(PackId([2; 16]));
    for o in objects() {
        b.add(&o);
    }
    let (_, entries) = b.finish();
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
