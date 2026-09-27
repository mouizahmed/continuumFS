//! The hand-written byte-level encoding of every object type.
//!
//! Integers are fixed-width little-endian. Decoding is strict: each value has exactly
//! one valid encoding, so equal values always get equal IDs.

use crate::id::{Id, RepoKey};
use crate::layout::PAGE_MAX;
use crate::object::{
    Chunk, ChunkList, ChunkPage, ChunkRef, Commit, CommitKind, Content, DirEntry, FILE_ID_END,
    FILE_ID_MIN, FormatParams, Kind, LogEntry, LogSegment, ObjectType, PageRef, Tree, Xattr,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("payload ends early")]
    Truncated,
    #[error("{0} trailing bytes after the payload")]
    TrailingBytes(usize),
    #[error("unknown {what} value {value}")]
    UnknownTag { what: &'static str, value: u8 },
    #[error("invalid {0}")]
    Invalid(&'static str),
    #[error("entries out of order or duplicated")]
    Unsorted,
    #[error("object type {found:?} where {expected:?} was expected")]
    WrongType {
        expected: ObjectType,
        found: Option<ObjectType>,
    },
}

/// An object type with a canonical encoding.
pub trait Object: Sized {
    /// The type written.
    const TYPE: ObjectType;

    /// The payload bytes (without the `[type][flags]` framing).
    fn encode(&self) -> Vec<u8>;

    /// Parses a payload, rejecting anything that isn't the canonical encoding of a valid value.
    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError>;

    /// The types read: `TYPE`, plus older types that decode into the same value.
    fn reads(ty: ObjectType) -> bool {
        ty == Self::TYPE
    }

    /// Parses a payload stored as `ty` (one of the types [`Object::reads`] accepts).
    fn decode_as(
        ty: ObjectType,
        payload: &[u8],
        params: &FormatParams,
    ) -> Result<Self, DecodeError> {
        let _ = ty;
        Self::decode(payload, params)
    }
}

/// An encoded object ready to store: its ID and payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Encoded {
    pub id: Id,
    pub ty: ObjectType,
    pub payload: Vec<u8>,
}

impl Encoded {
    pub fn new<T: Object>(key: &RepoKey, obj: &T) -> Encoded {
        let payload = obj.encode();
        Encoded {
            id: Id::compute(key, T::TYPE, &payload),
            ty: T::TYPE,
            payload,
        }
    }

    /// The stored form: `[type u8][flags u8][payload]`, with `flags = 0` in v0.
    pub fn to_stored(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + self.payload.len());
        out.push(self.ty as u8);
        out.push(0);
        out.extend_from_slice(&self.payload);
        out
    }
}

/// Splits a stored object into its type and payload, checking that `T` reads the type and
/// that `flags == 0`.
pub fn unframe<T: Object>(stored: &[u8]) -> Result<(ObjectType, &[u8]), DecodeError> {
    let [ty, flags, payload @ ..] = stored else {
        return Err(DecodeError::Truncated);
    };
    let found = ObjectType::from_u8(*ty);
    let Some(ty) = found.filter(|t| T::reads(*t)) else {
        return Err(DecodeError::WrongType {
            expected: T::TYPE,
            found,
        });
    };
    if *flags != 0 {
        return Err(DecodeError::Invalid("flags (must be 0)"));
    }
    Ok((ty, payload))
}

/// Decodes a stored object and checks that it hashes to `id`.
pub fn decode_verified<T: Object>(
    key: &RepoKey,
    id: &Id,
    stored: &[u8],
    params: &FormatParams,
) -> Result<T, VerifyError> {
    let (ty, payload) = unframe::<T>(stored)?;
    let actual = Id::compute(key, ty, payload);
    if actual != *id {
        return Err(VerifyError::HashMismatch {
            expected: *id,
            actual,
        });
    }
    Ok(T::decode_as(ty, payload, params)?)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("object hashes to {actual}, not {expected}")]
    HashMismatch { expected: Id, actual: Id },
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

/// Longest commit or log message, in bytes.
const MESSAGE_MAX: usize = 64 * 1024;
/// Longest symlink target, in bytes (Linux `PATH_MAX` minus the NUL).
const SYMLINK_MAX: usize = 4095;

// Content tags in a `DirEntry`.
const INLINE: u8 = 1;
const CHUNK: u8 = 2;
const CHUNK_LIST: u8 = 3;
const DIR: u8 = 4;
const SYMLINK: u8 = 5;

struct Writer(Vec<u8>);

impl Writer {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u16(&mut self, v: u16) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_le_bytes());
    }
    fn id(&mut self, id: &Id) {
        self.0.extend_from_slice(&id.0);
    }
    fn raw(&mut self, b: &[u8]) {
        self.0.extend_from_slice(b);
    }
    // Lengths are bounded by the types' invariants; encoders only see valid values.
    fn bytes8(&mut self, b: &[u8]) {
        self.u8(b.len() as u8);
        self.raw(b);
    }
    fn bytes16(&mut self, b: &[u8]) {
        self.u16(b.len() as u16);
        self.raw(b);
    }
    fn bytes32(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.raw(b);
    }
    fn opt<T>(&mut self, v: Option<T>, f: impl FnOnce(&mut Self, T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                f(self, v);
            }
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }
    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }
    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }
    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }
    fn id(&mut self) -> Result<Id, DecodeError> {
        Ok(Id(self.array()?))
    }
    fn bytes8(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u8()?;
        self.take(n.into())
    }
    fn bytes16(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u16()?;
        self.take(n.into())
    }
    fn bytes32(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u32()?;
        self.take(n as usize)
    }
    fn utf8_16(&mut self) -> Result<String, DecodeError> {
        utf8(self.bytes16()?)
    }
    fn utf8_32(&mut self) -> Result<String, DecodeError> {
        utf8(self.bytes32()?)
    }
    fn opt<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, DecodeError>,
    ) -> Result<Option<T>, DecodeError> {
        match self.u8()? {
            0 => Ok(None),
            1 => f(self).map(Some),
            value => Err(DecodeError::UnknownTag {
                what: "option",
                value,
            }),
        }
    }
    /// Fails unless every byte was consumed.
    fn finish(self) -> Result<(), DecodeError> {
        match self.buf.len() {
            0 => Ok(()),
            n => Err(DecodeError::TrailingBytes(n)),
        }
    }
}

fn utf8(b: &[u8]) -> Result<String, DecodeError> {
    String::from_utf8(b.to_vec()).map_err(|_| DecodeError::Invalid("UTF-8 string"))
}

fn check(ok: bool, what: &'static str) -> Result<(), DecodeError> {
    if ok {
        Ok(())
    } else {
        Err(DecodeError::Invalid(what))
    }
}

fn commit_kind(b: u8) -> Result<CommitKind, DecodeError> {
    CommitKind::from_u8(b).ok_or(DecodeError::UnknownTag {
        what: "commit kind",
        value: b,
    })
}

fn decode_with<T>(
    payload: &[u8],
    f: impl FnOnce(&mut Reader<'_>) -> Result<T, DecodeError>,
) -> Result<T, DecodeError> {
    let mut r = Reader { buf: payload };
    let v = f(&mut r)?;
    r.finish()?;
    Ok(v)
}

impl Object for Chunk {
    const TYPE: ObjectType = ObjectType::Chunk;

    fn encode(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        check(
            !payload.is_empty() && payload.len() <= params.chunker.max as usize,
            "chunk length",
        )?;
        Ok(Chunk(payload.to_vec()))
    }
}

impl Object for ChunkPage {
    const TYPE: ObjectType = ObjectType::ChunkPage;

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer(Vec::with_capacity(2 + self.chunks.len() * 36));
        w.u16(self.chunks.len() as u16);
        for c in &self.chunks {
            w.id(&c.id);
            w.u32(c.len);
        }
        w.0
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        decode_with(payload, |r| {
            let count = r.u16()? as usize;
            check((1..=PAGE_MAX).contains(&count), "chunk page count")?;
            let mut chunks = Vec::with_capacity(count);
            for _ in 0..count {
                let id = r.id()?;
                let len = r.u32()?;
                check((1..=params.chunker.max).contains(&len), "chunk length")?;
                chunks.push(ChunkRef { id, len });
            }
            Ok(ChunkPage { chunks })
        })
    }
}

impl Object for ChunkList {
    const TYPE: ObjectType = ObjectType::ChunkList;

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer(Vec::with_capacity(4 + self.pages.len() * 40));
        w.u32(self.pages.len() as u32);
        for p in &self.pages {
            w.id(&p.id);
            w.u64(p.total_len);
        }
        w.0
    }

    fn decode(payload: &[u8], _: &FormatParams) -> Result<Self, DecodeError> {
        decode_with(payload, |r| {
            let count = r.u32()? as usize;
            check(count >= 1, "chunk list count")?;
            // Each page is 40 bytes; don't trust the count for the allocation.
            let mut pages = Vec::with_capacity(count.min(r.buf.len() / 40));
            for _ in 0..count {
                let id = r.id()?;
                let total_len = r.u64()?;
                check(total_len >= 1, "page length")?;
                pages.push(PageRef { id, total_len });
            }
            Ok(ChunkList { pages })
        })
    }
}

fn encode_entry(w: &mut Writer, e: &DirEntry, with_file_id: bool) {
    w.bytes8(&e.name);
    w.u8(e.content.kind() as u8);
    w.u16(e.mode);
    w.i64(e.mtime_ns);
    w.u64(e.size);
    match &e.content {
        Content::Inline(b) => {
            w.u8(INLINE);
            w.bytes16(b);
        }
        Content::Chunk(id) => {
            w.u8(CHUNK);
            w.id(id);
        }
        Content::ChunkList(id) => {
            w.u8(CHUNK_LIST);
            w.id(id);
        }
        Content::Dir(id) => {
            w.u8(DIR);
            w.id(id);
        }
        Content::Symlink(t) => {
            w.u8(SYMLINK);
            w.bytes16(t);
        }
    }
    w.opt(e.btime_ns, Writer::i64);
    w.opt(e.xattrs.as_ref(), |w, xs| {
        w.u16(xs.len() as u16);
        for x in xs {
            w.bytes8(&x.name);
            w.bytes32(&x.value);
        }
    });
    if with_file_id {
        w.u64(e.file_id.expect("every entry of a Tree has a file ID"));
    }
}

fn valid_name(n: &[u8]) -> bool {
    !n.is_empty() && n != b"." && n != b".." && !n.iter().any(|&b| b == b'/' || b == 0)
}

fn decode_entry(
    r: &mut Reader<'_>,
    params: &FormatParams,
    with_file_id: bool,
) -> Result<DirEntry, DecodeError> {
    let name = r.bytes8()?;
    check(valid_name(name), "entry name")?;
    let kind = match r.u8()? {
        1 => Kind::File,
        2 => Kind::Dir,
        3 => Kind::Symlink,
        value => {
            return Err(DecodeError::UnknownTag {
                what: "entry kind",
                value,
            });
        }
    };
    let mode = r.u16()?;
    check(mode & !0o7777 == 0, "mode")?;
    let mtime_ns = r.i64()?;
    let size = r.u64()?;
    let inline_max = u64::from(params.inline_max);
    let content = match r.u8()? {
        INLINE => {
            let b = r.bytes16()?;
            check(b.len() as u64 == size && size <= inline_max, "inline size")?;
            Content::Inline(b.to_vec())
        }
        CHUNK => {
            check(
                size > inline_max && size <= u64::from(params.chunker.max),
                "single-chunk size",
            )?;
            Content::Chunk(r.id()?)
        }
        CHUNK_LIST => {
            check(size > inline_max, "chunk list size")?;
            Content::ChunkList(r.id()?)
        }
        DIR => {
            check(size == 0, "directory size")?;
            Content::Dir(r.id()?)
        }
        SYMLINK => {
            let t = r.bytes16()?;
            check(
                !t.is_empty()
                    && t.len() <= SYMLINK_MAX
                    && !t.contains(&0)
                    && t.len() as u64 == size,
                "symlink target",
            )?;
            check(mode == 0o777, "symlink mode")?;
            Content::Symlink(t.to_vec())
        }
        value => {
            return Err(DecodeError::UnknownTag {
                what: "content",
                value,
            });
        }
    };
    check(content.kind() == kind, "content does not match kind")?;
    let btime_ns = r.opt(Reader::i64)?;
    let xattrs = r.opt(|r| {
        let count = r.u16()?;
        let mut xs: Vec<Xattr> = Vec::with_capacity(count.into());
        for _ in 0..count {
            let name = r.bytes8()?;
            check(!name.is_empty(), "xattr name")?;
            if xs.last().is_some_and(|last| last.name.as_slice() >= name) {
                return Err(DecodeError::Unsorted);
            }
            let value = r.bytes32()?.to_vec();
            xs.push(Xattr {
                name: name.to_vec(),
                value,
            });
        }
        Ok(xs)
    })?;
    let file_id = if with_file_id {
        let id = r.u64()?;
        check((FILE_ID_MIN..FILE_ID_END).contains(&id), "file ID")?;
        Some(id)
    } else {
        None
    };
    Ok(DirEntry {
        name: name.to_vec(),
        mode,
        mtime_ns,
        size,
        content,
        btime_ns,
        xattrs,
        file_id,
    })
}

/// The payload of a [`ObjectType::LegacyTree`] (format version 1), for tests of upgrading.
#[doc(hidden)]
pub fn encode_legacy_tree(tree: &Tree) -> Vec<u8> {
    let mut w = Writer(Vec::new());
    w.u32(tree.entries.len() as u32);
    for e in &tree.entries {
        encode_entry(&mut w, e, false);
    }
    w.0
}

impl Object for Tree {
    const TYPE: ObjectType = ObjectType::Tree;

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer(Vec::new());
        w.u32(self.entries.len() as u32);
        for e in &self.entries {
            encode_entry(&mut w, e, true);
        }
        w.0
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        Self::decode_as(ObjectType::Tree, payload, params)
    }

    fn reads(ty: ObjectType) -> bool {
        matches!(ty, ObjectType::Tree | ObjectType::LegacyTree)
    }

    fn decode_as(
        ty: ObjectType,
        payload: &[u8],
        params: &FormatParams,
    ) -> Result<Self, DecodeError> {
        let with_file_id = ty == ObjectType::Tree;
        decode_with(payload, |r| {
            let count = r.u32()? as usize;
            // An entry is at least 32 bytes; don't trust the count for the allocation.
            let mut entries: Vec<DirEntry> = Vec::with_capacity(count.min(r.buf.len() / 32));
            for _ in 0..count {
                let e = decode_entry(r, params, with_file_id)?;
                if entries.last().is_some_and(|last| last.name >= e.name) {
                    return Err(DecodeError::Unsorted);
                }
                entries.push(e);
            }
            Ok(Tree { entries })
        })
    }
}

impl Object for Commit {
    const TYPE: ObjectType = ObjectType::Commit;

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer(Vec::new());
        w.id(&self.root_tree);
        w.i64(self.time_ns);
        w.bytes16(self.author.as_bytes());
        w.raw(&self.machine_id);
        w.u8(self.kind as u8);
        w.bytes32(self.message.as_bytes());
        w.0
    }

    fn decode(payload: &[u8], _: &FormatParams) -> Result<Self, DecodeError> {
        decode_with(payload, |r| {
            let root_tree = r.id()?;
            let time_ns = r.i64()?;
            let author = r.utf8_16()?;
            check((1..=255).contains(&author.len()), "author")?;
            let machine_id = r.array()?;
            let kind = commit_kind(r.u8()?)?;
            let message = r.utf8_32()?;
            check(message.len() <= MESSAGE_MAX, "message length")?;
            Ok(Commit {
                root_tree,
                time_ns,
                author,
                machine_id,
                kind,
                message,
            })
        })
    }
}

impl Object for LogSegment {
    const TYPE: ObjectType = ObjectType::LogSegment;

    fn encode(&self) -> Vec<u8> {
        let mut w = Writer(Vec::new());
        w.opt(self.prev.as_ref(), Writer::id);
        w.u16(self.entries.len() as u16);
        for e in &self.entries {
            w.i64(e.time_ns);
            w.id(&e.commit);
            w.u8(e.kind as u8);
            w.bytes32(e.message.as_bytes());
        }
        w.0
    }

    fn decode(payload: &[u8], _: &FormatParams) -> Result<Self, DecodeError> {
        decode_with(payload, |r| {
            let prev = r.opt(Reader::id)?;
            let count = r.u16()? as usize;
            check(
                (1..=LogSegment::MAX_ENTRIES).contains(&count),
                "log segment count",
            )?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let time_ns = r.i64()?;
                let commit = r.id()?;
                let kind = commit_kind(r.u8()?)?;
                let message = r.utf8_32()?;
                check(message.len() <= MESSAGE_MAX, "message length")?;
                entries.push(LogEntry {
                    time_ns,
                    commit,
                    kind,
                    message,
                });
            }
            Ok(LogSegment { prev, entries })
        })
    }
}
