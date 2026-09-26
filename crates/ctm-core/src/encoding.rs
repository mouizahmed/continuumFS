//! The hand-written byte-level encoding of every object type.
//!
//! Integers are fixed-width little-endian. Decoding is strict: each value has exactly
//! one valid encoding, so equal values always get equal IDs.

use crate::id::{Id, RepoKey};
use crate::object::{
    Chunk, ChunkList, ChunkPage, Commit, FormatParams, LogSegment, ObjectType, Tree,
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
    const TYPE: ObjectType;

    /// The payload bytes (without the `[type][flags]` framing).
    fn encode(&self) -> Vec<u8>;

    /// Parses a payload, rejecting anything that isn't the canonical encoding of a valid value.
    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError>;
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

/// Splits a stored object into its type and payload, checking the type and that `flags == 0`.
pub fn unframe(stored: &[u8], expected: ObjectType) -> Result<&[u8], DecodeError> {
    let _ = (stored, expected);
    todo!("M1: object framing")
}

/// Decodes a stored object and checks that it hashes to `id`.
pub fn decode_verified<T: Object>(
    key: &RepoKey,
    id: &Id,
    stored: &[u8],
    params: &FormatParams,
) -> Result<T, VerifyError> {
    let _ = (key, id, stored, params);
    todo!("M1: verified decode")
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum VerifyError {
    #[error("object hashes to {actual}, not {expected}")]
    HashMismatch { expected: Id, actual: Id },
    #[error(transparent)]
    Decode(#[from] DecodeError),
}

impl Object for Chunk {
    const TYPE: ObjectType = ObjectType::Chunk;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: Chunk encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: Chunk decoding")
    }
}

impl Object for ChunkPage {
    const TYPE: ObjectType = ObjectType::ChunkPage;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: ChunkPage encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: ChunkPage decoding")
    }
}

impl Object for ChunkList {
    const TYPE: ObjectType = ObjectType::ChunkList;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: ChunkList encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: ChunkList decoding")
    }
}

impl Object for Tree {
    const TYPE: ObjectType = ObjectType::Tree;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: Tree encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: Tree decoding")
    }
}

impl Object for Commit {
    const TYPE: ObjectType = ObjectType::Commit;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: Commit encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: Commit decoding")
    }
}

impl Object for LogSegment {
    const TYPE: ObjectType = ObjectType::LogSegment;

    fn encode(&self) -> Vec<u8> {
        todo!("M1: LogSegment encoding")
    }

    fn decode(payload: &[u8], params: &FormatParams) -> Result<Self, DecodeError> {
        let _ = (payload, params);
        todo!("M1: LogSegment decoding")
    }
}
