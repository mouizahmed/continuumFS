//! Continuum's object model: object IDs, the byte-level encoding, content-defined
//! chunking, canonical chunk lists, partial re-chunking, and tree diff.
//!
//! Nothing in this crate does I/O.

pub mod chunker;
pub mod diff;
pub mod encoding;
pub mod id;
pub mod layout;
pub mod object;
pub mod pack;
pub mod rechunk;

pub use chunker::ChunkerParams;
pub use encoding::{DecodeError, Encoded, Object};
pub use id::{Id, RepoKey};
pub use object::{
    Chunk, ChunkList, ChunkPage, ChunkRef, Commit, CommitKind, Content, DirEntry, FILE_ID_END,
    FILE_ID_MIN, FormatParams, Kind, LogEntry, LogSegment, ObjectType, PageRef, Tree, Xattr,
    file_id_from,
};
