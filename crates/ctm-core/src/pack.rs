//! Packfiles and index segments: many objects per bucket object (R1).
//!
//! ```text
//! pack:    "CTMPACK1" pack_id
//!          repeat { type u8, flags u8, raw_len u32, stored_len u32, hints_len u32, payload, hints }
//!          count u32, count × { id, offset u32, stored_len u32, hints_len u32, type u8, flags u8 },
//!          blake3(header + entries), trailer_len u32, "CTMPACK1"
//! segment: "CTMIDX01" count u32, count × { id, pack_id, offset u32, stored_len u32, type u8, flags u8 },
//!          blake3(everything above)
//! ```
//!
//! A trailer offset points at an entry's `type` byte; an index offset points at its payload.

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
const ENTRY_HEADER: u32 = 14;
const TRAILER_ENTRY: usize = 32 + 4 + 4 + 4 + 1 + 1;
const INDEX_ENTRY: usize = 32 + 16 + 4 + 4 + 1 + 1;

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

/// Where an object's payload is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location {
    pub pack: PackId,
    /// Where the payload starts in the pack.
    pub offset: u32,
    pub stored_len: u32,
    pub ty: ObjectType,
    pub flags: u8,
}

impl Location {
    /// The byte range of the payload in the pack.
    pub fn range(&self) -> std::ops::Range<u64> {
        let start = u64::from(self.offset);
        start..start + u64::from(self.stored_len)
    }

    /// The pack's key: data packs hold chunks, meta packs everything else.
    pub fn pack_key(&self) -> String {
        pack_key(self.pack, self.ty.is_data())
    }
}

pub fn pack_key(pack: PackId, data: bool) -> String {
    format!("packs/{}/{pack}", if data { "data" } else { "meta" })
}

/// Builds one pack in memory.
pub struct PackBuilder {
    id: PackId,
    buf: Vec<u8>,
    entries: Vec<(Id, Location)>,
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

    /// Appends an object (uncompressed, no hints) and returns where its payload is.
    pub fn add(&mut self, obj: &Encoded) -> Location {
        let len = u32::try_from(obj.payload.len()).expect("objects are under 4 GiB");
        self.buf.push(obj.ty as u8);
        self.buf.push(0);
        self.buf.extend_from_slice(&len.to_le_bytes()); // raw_len
        self.buf.extend_from_slice(&len.to_le_bytes()); // stored_len
        self.buf.extend_from_slice(&0u32.to_le_bytes()); // hints_len
        let offset = u32::try_from(self.buf.len()).expect("packs are under 4 GiB");
        self.buf.extend_from_slice(&obj.payload);
        let loc = Location {
            pack: self.id,
            offset,
            stored_len: len,
            ty: obj.ty,
            flags: 0,
        };
        self.entries.push((obj.id, loc));
        loc
    }

    /// The finished pack, and where each object in it is.
    pub fn finish(self) -> (Vec<u8>, Vec<(Id, Location)>) {
        let mut buf = self.buf;
        let hash = blake3::hash(&buf);
        let trailer_start = buf.len();
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (id, loc) in &self.entries {
            buf.extend_from_slice(&id.0);
            buf.extend_from_slice(&(loc.offset - ENTRY_HEADER).to_le_bytes());
            buf.extend_from_slice(&loc.stored_len.to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes());
            buf.push(loc.ty as u8);
            buf.push(loc.flags);
        }
        buf.extend_from_slice(hash.as_bytes());
        let trailer_len = (buf.len() - trailer_start) as u32;
        buf.extend_from_slice(&trailer_len.to_le_bytes());
        buf.extend_from_slice(PACK_MAGIC);
        (buf, self.entries)
    }
}

fn u32_at(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}

/// Reads and checks a whole pack's trailer: where each object is.
pub fn read_trailer(pack: &[u8]) -> Result<(PackId, Vec<(Id, Location)>), DecodeError> {
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
        let entry = u32_at(e, 32);
        let loc = Location {
            pack: id,
            offset: entry + ENTRY_HEADER,
            stored_len: u32_at(e, 36),
            ty,
            flags: e[45],
        };
        if loc.range().end > trailer_start as u64 {
            return Err(DecodeError::Invalid("pack entry out of range"));
        }
        out.push((Id(e[..32].try_into().expect("32 bytes")), loc));
    }
    Ok((id, out))
}

/// An index segment listing `entries`.
pub fn encode_index(entries: &[(Id, Location)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + entries.len() * INDEX_ENTRY + 32);
    buf.extend_from_slice(INDEX_MAGIC);
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (id, loc) in entries {
        buf.extend_from_slice(&id.0);
        buf.extend_from_slice(&loc.pack.0);
        buf.extend_from_slice(&loc.offset.to_le_bytes());
        buf.extend_from_slice(&loc.stored_len.to_le_bytes());
        buf.push(loc.ty as u8);
        buf.push(loc.flags);
    }
    let hash = blake3::hash(&buf);
    buf.extend_from_slice(hash.as_bytes());
    buf
}

pub fn decode_index(bytes: &[u8]) -> Result<Vec<(Id, Location)>, DecodeError> {
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
            Location {
                pack: PackId(e[32..48].try_into().expect("16 bytes")),
                offset: u32_at(e, 48),
                stored_len: u32_at(e, 52),
                ty,
                flags: e[57],
            },
        ));
    }
    Ok(out)
}
