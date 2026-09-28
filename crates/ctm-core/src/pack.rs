//! Packfiles, location hints, and index segments: many objects per bucket object (R1, R3).
//!
//! ```text
//! pack:    "CTMPACK1" pack_id
//!          repeat { type u8, flags u8, raw_len u32, stored_len u32, hints_len u32, payload, hints }
//!          count u32, count × { id, offset u32, stored_len u32, hints_len u32, type u8, flags u8 },
//!          blake3(header + entries), trailer_len u32, "CTMPACK1"
//! hint:    pack_id 16B, offset u32, len u32          (all zero: unknown)
//! segment: "CTMIDX01" count u32, count × { id, pack_id, offset u32, len u32, type u8 },
//!          blake3(everything above)
//! ```
//!
//! A [`Location`] (in a hint, a segment, or a trailer) is a whole entry: its offset is the
//! entry's `type` byte, and its length covers the header, payload, and hints, so one ranged
//! GET returns an object together with where its children are.

use std::fmt;
use std::str::FromStr;

use crate::encoding::{DecodeError, Encoded};
use crate::id::Id;
use crate::object::ObjectType;

const PACK_MAGIC: &[u8; 8] = b"CTMPACK1";
const INDEX_MAGIC: &[u8; 8] = b"CTMIDX01";
/// A pack is closed once it reaches this size.
pub const PACK_TARGET: usize = 32 << 20;
/// Bytes before an entry's payload.
pub const ENTRY_HEADER: usize = 14;
pub const HINT_LEN: usize = 24;
const TRAILER_ENTRY: usize = 32 + 4 + 4 + 4 + 1 + 1;
const INDEX_ENTRY: usize = 32 + 16 + 4 + 4 + 1;

/// A pack's name: 16 random bytes, written as 32 hex characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PackId(pub [u8; 16]);

impl fmt::Display for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for PackId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PackId({})", &hex::encode(self.0)[..12])
    }
}

impl FromStr for PackId {
    type Err = DecodeError;

    fn from_str(s: &str) -> Result<PackId, DecodeError> {
        let mut id = [0u8; 16];
        hex::decode_to_slice(s, &mut id).map_err(|_| DecodeError::Invalid("pack ID"))?;
        Ok(PackId(id))
    }
}

/// An object in a pack or index segment: its ID, type, and where its entry is.
pub type Listed = (Id, ObjectType, Location);

/// Where a pack entry is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Location {
    pub pack: PackId,
    /// Where the entry (its `type` byte) starts in the pack.
    pub offset: u32,
    /// The whole entry: header, payload, and hints.
    pub len: u32,
}

impl Location {
    /// The entry's byte range in the pack.
    pub fn range(&self) -> std::ops::Range<u64> {
        let start = u64::from(self.offset);
        start..start + u64::from(self.len)
    }

    /// The pack's key; `data` says whether it holds chunks.
    pub fn key(&self, data: bool) -> String {
        pack_key(self.pack, data)
    }

    /// As a hint: 24 bytes.
    pub fn to_hint(&self) -> [u8; HINT_LEN] {
        let mut b = [0u8; HINT_LEN];
        b[..16].copy_from_slice(&self.pack.0);
        b[16..20].copy_from_slice(&self.offset.to_le_bytes());
        b[20..].copy_from_slice(&self.len.to_le_bytes());
        b
    }

    /// Parses a hint; all zeros (unknown) and impossible lengths are `None`.
    pub fn from_hint(b: &[u8]) -> Option<Location> {
        let b: &[u8; HINT_LEN] = b.try_into().ok()?;
        let loc = Location {
            pack: PackId(b[..16].try_into().expect("16 bytes")),
            offset: u32_at(b, 16),
            len: u32_at(b, 20),
        };
        (loc.len as usize > ENTRY_HEADER && u64::from(loc.offset) >= 24).then_some(loc)
    }
}

/// Data packs hold chunks; meta packs everything else.
pub fn pack_key(pack: PackId, data: bool) -> String {
    format!("packs/{}/{pack}", if data { "data" } else { "meta" })
}

/// Builds one pack in memory.
pub struct PackBuilder {
    id: PackId,
    buf: Vec<u8>,
    entries: Vec<Listed>,
}

impl PackBuilder {
    pub fn new(id: PackId) -> PackBuilder {
        let mut buf = Vec::with_capacity(1 << 20);
        buf.extend_from_slice(PACK_MAGIC);
        buf.extend_from_slice(&id.0);
        PackBuilder {
            id,
            buf,
            entries: Vec::new(),
        }
    }

    pub fn id(&self) -> PackId {
        self.id
    }

    /// Bytes so far (without the trailer).
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Appends an object (uncompressed) with one hint per reference in `obj.refs`, and returns
    /// where its entry is.
    pub fn add(&mut self, obj: &Encoded, hints: &[Option<Location>]) -> Location {
        assert_eq!(hints.len(), obj.refs.len(), "one hint per reference");
        let offset = u32::try_from(self.buf.len()).expect("packs are under 4 GiB");
        let len = u32::try_from(obj.payload.len()).expect("objects are under 4 GiB");
        let hints_len = (hints.len() * HINT_LEN) as u32;
        self.buf.push(obj.ty as u8);
        self.buf.push(0);
        self.buf.extend_from_slice(&len.to_le_bytes()); // raw_len
        self.buf.extend_from_slice(&len.to_le_bytes()); // stored_len
        self.buf.extend_from_slice(&hints_len.to_le_bytes());
        self.buf.extend_from_slice(&obj.payload);
        for h in hints {
            self.buf
                .extend_from_slice(&h.map_or([0; HINT_LEN], |l| l.to_hint()));
        }
        let loc = Location {
            pack: self.id,
            offset,
            len: self.buf.len() as u32 - offset,
        };
        self.entries.push((obj.id, obj.ty, loc));
        loc
    }

    /// The finished pack, and where each object in it is.
    pub fn finish(self) -> (Vec<u8>, Vec<Listed>) {
        let mut buf = self.buf;
        let hash = blake3::hash(&buf);
        let trailer_start = buf.len();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (id, ty, loc) in &self.entries {
            // stored_len and hints_len, copied from the entry's header.
            let at = loc.offset as usize;
            let lens: [u8; 8] = buf[at + 6..at + 14].try_into().expect("8 bytes");
            let flags = buf[at + 1];
            buf.extend_from_slice(&id.0);
            buf.extend_from_slice(&loc.offset.to_le_bytes());
            buf.extend_from_slice(&lens);
            buf.push(*ty as u8);
            buf.push(flags);
        }
        buf.extend_from_slice(hash.as_bytes());
        let trailer_len = (buf.len() - trailer_start) as u32;
        buf.extend_from_slice(&trailer_len.to_le_bytes());
        buf.extend_from_slice(PACK_MAGIC);
        (buf, self.entries)
    }
}

/// One pack entry, as fetched by a ranged GET of its [`Location`].
#[derive(Debug)]
pub struct Entry<'a> {
    pub ty: ObjectType,
    pub flags: u8,
    pub payload: &'a [u8],
    /// One per reference in the payload; `None` where unknown.
    pub hints: Vec<Option<Location>>,
}

impl Entry<'_> {
    /// Parses an entry, checking that its lengths add up to exactly `bytes`.
    pub fn parse(bytes: &[u8]) -> Result<Entry<'_>, DecodeError> {
        if bytes.len() < ENTRY_HEADER {
            return Err(DecodeError::Truncated);
        }
        let ty = ObjectType::from_u8(bytes[0]).ok_or(DecodeError::UnknownTag {
            what: "object type",
            value: bytes[0],
        })?;
        let flags = bytes[1];
        let (raw_len, stored_len, hints_len) = (
            u32_at(bytes, 2) as usize,
            u32_at(bytes, 6) as usize,
            u32_at(bytes, 10) as usize,
        );
        if flags != 0 || raw_len != stored_len {
            return Err(DecodeError::Invalid("pack entry flags"));
        }
        if hints_len % HINT_LEN != 0
            || ENTRY_HEADER
                .checked_add(stored_len)
                .and_then(|n| n.checked_add(hints_len))
                != Some(bytes.len())
        {
            return Err(DecodeError::Invalid("pack entry length"));
        }
        let payload = &bytes[ENTRY_HEADER..ENTRY_HEADER + stored_len];
        let hints = bytes[ENTRY_HEADER + stored_len..]
            .as_chunks::<HINT_LEN>()
            .0
            .iter()
            .map(|h| Location::from_hint(h))
            .collect();
        Ok(Entry {
            ty,
            flags,
            payload,
            hints,
        })
    }

    /// The stored form, `[type][flags][payload]`.
    pub fn to_stored(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + self.payload.len());
        out.push(self.ty as u8);
        out.push(self.flags);
        out.extend_from_slice(self.payload);
        out
    }
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

/// Reads and checks a whole pack's trailer: where each object is.
pub fn read_trailer(pack: &[u8]) -> Result<(PackId, Vec<Listed>), DecodeError> {
    let n = pack.len();
    if n < 24 + 4 + 32 + 4 + 8 || &pack[..8] != PACK_MAGIC || &pack[n - 8..] != PACK_MAGIC {
        return Err(DecodeError::Invalid("pack framing"));
    }
    let id = PackId(pack[8..24].try_into().expect("16 bytes"));
    let trailer_len = u32_at(pack, n - 12) as usize;
    let trailer_start = n
        .checked_sub(12 + trailer_len)
        .filter(|s| *s >= 24)
        .ok_or(DecodeError::Invalid("pack trailer length"))?;
    let trailer = &pack[trailer_start..n - 12];
    let count = u32_at(trailer, 0) as usize;
    if trailer.len() != 4 + count * TRAILER_ENTRY + 32 {
        return Err(DecodeError::Invalid("pack trailer length"));
    }
    if blake3::hash(&pack[..trailer_start]).as_bytes()[..] != trailer[trailer.len() - 32..] {
        return Err(DecodeError::Invalid("pack checksum"));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let e = &trailer[4 + i * TRAILER_ENTRY..][..TRAILER_ENTRY];
        let ty = ObjectType::from_u8(e[44]).ok_or(DecodeError::UnknownTag {
            what: "object type",
            value: e[44],
        })?;
        let len = (ENTRY_HEADER as u64 + u64::from(u32_at(e, 36)) + u64::from(u32_at(e, 40)))
            .try_into()
            .map_err(|_| DecodeError::Invalid("pack entry length"))?;
        let loc = Location {
            pack: id,
            offset: u32_at(e, 32),
            len,
        };
        if loc.offset < 24 || loc.range().end > trailer_start as u64 {
            return Err(DecodeError::Invalid("pack entry out of range"));
        }
        out.push((Id(e[..32].try_into().expect("32 bytes")), ty, loc));
    }
    Ok((id, out))
}

/// An index segment listing `entries`.
pub fn encode_index(entries: &[Listed]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + entries.len() * INDEX_ENTRY + 32);
    buf.extend_from_slice(INDEX_MAGIC);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, ty, loc) in entries {
        buf.extend_from_slice(&id.0);
        buf.extend_from_slice(&loc.pack.0);
        buf.extend_from_slice(&loc.offset.to_le_bytes());
        buf.extend_from_slice(&loc.len.to_le_bytes());
        buf.push(*ty as u8);
    }
    let hash = blake3::hash(&buf);
    buf.extend_from_slice(hash.as_bytes());
    buf
}

pub fn decode_index(bytes: &[u8]) -> Result<Vec<Listed>, DecodeError> {
    if bytes.len() < 12 + 32 || &bytes[..8] != INDEX_MAGIC {
        return Err(DecodeError::Invalid("index segment framing"));
    }
    let count = u32_at(bytes, 8) as usize;
    if bytes.len() != 12 + count * INDEX_ENTRY + 32 {
        return Err(DecodeError::Invalid("index segment length"));
    }
    let body = &bytes[..bytes.len() - 32];
    if blake3::hash(body).as_bytes()[..] != bytes[body.len()..] {
        return Err(DecodeError::Invalid("index segment checksum"));
    }
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let e = &body[12 + i * INDEX_ENTRY..][..INDEX_ENTRY];
        let ty = ObjectType::from_u8(e[56]).ok_or(DecodeError::UnknownTag {
            what: "object type",
            value: e[56],
        })?;
        out.push((
            Id(e[..32].try_into().expect("32 bytes")),
            ty,
            Location {
                pack: PackId(e[32..48].try_into().expect("16 bytes")),
                offset: u32_at(e, 48),
                len: u32_at(e, 52),
            },
        ));
    }
    Ok(out)
}
