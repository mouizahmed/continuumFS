//! The content-addressed object types. Their byte-level encoding is in [`crate::encoding`].

use crate::chunker::ChunkerParams;
use crate::id::Id;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ObjectType {
    Chunk = 0x01,
    ChunkPage = 0x02,
    ChunkList = 0x03,
    Tree = 0x04,
    Commit = 0x05,
    LogSegment = 0x06,
}

impl ObjectType {
    pub fn from_u8(b: u8) -> Option<ObjectType> {
        Some(match b {
            0x01 => ObjectType::Chunk,
            0x02 => ObjectType::ChunkPage,
            0x03 => ObjectType::ChunkList,
            0x04 => ObjectType::Tree,
            0x05 => ObjectType::Commit,
            0x06 => ObjectType::LogSegment,
            _ => return None,
        })
    }

    /// Chunks live under `chunks/`; everything else under `meta/`.
    pub fn is_data(self) -> bool {
        self == ObjectType::Chunk
    }
}

/// Per-repo parameters that decoding checks against, from the bucket's `config`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormatParams {
    pub chunker: ChunkerParams,
    pub inline_max: u32,
}

impl FormatParams {
    pub const DEFAULT: FormatParams = FormatParams {
        chunker: ChunkerParams::DEFAULT,
        inline_max: 4096,
    };
}

/// Raw file bytes, `1 ≤ len ≤ chunker.max`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk(pub Vec<u8>);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    pub id: Id,
    pub len: u32,
}

/// Up to [`crate::layout::PAGE_MAX`] chunk references.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkPage {
    pub chunks: Vec<ChunkRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRef {
    pub id: Id,
    /// Sum of the page's chunk lengths.
    pub total_len: u64,
}

/// The pages of a file with two or more chunks, in canonical form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkList {
    pub pages: Vec<PageRef>,
}

/// One directory, entries strictly ascending by name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tree {
    pub entries: Vec<DirEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    /// 1–255 bytes; no `/` or NUL; not `.` or `..`.
    pub name: Vec<u8>,
    /// Permission bits only (`mode & !0o7777 == 0`); `0o777` for symlinks.
    pub mode: u16,
    pub mtime_ns: i64,
    /// File: its length. Dir: 0. Symlink: the target's length.
    pub size: u64,
    pub content: Content,
    /// Reserved for macOS birth time; always `None` on Linux.
    pub btime_ns: Option<i64>,
    /// Reserved for macOS xattrs; always `None` on Linux.
    pub xattrs: Option<Vec<Xattr>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Content {
    /// A file of at most `inline_max` bytes, including the empty file.
    Inline(Vec<u8>),
    /// A file larger than `inline_max` that FastCDC cuts into exactly one chunk.
    Chunk(Id),
    /// A file that FastCDC cuts into two or more chunks.
    ChunkList(Id),
    /// A directory's child tree.
    Dir(Id),
    /// A symlink target: 1–4095 bytes, no NUL.
    Symlink(Vec<u8>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    File = 1,
    Dir = 2,
    Symlink = 3,
}

impl Content {
    /// The encoded `kind` byte is derived from the content, so the two can never disagree.
    pub fn kind(&self) -> Kind {
        match self {
            Content::Inline(_) | Content::Chunk(_) | Content::ChunkList(_) => Kind::File,
            Content::Dir(_) => Kind::Dir,
            Content::Symlink(_) => Kind::Symlink,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Xattr {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CommitKind {
    Auto = 1,
    Manual = 2,
    Merge = 3,
    Restore = 4,
    Reset = 5,
    Import = 6,
}

impl CommitKind {
    pub fn from_u8(b: u8) -> Option<CommitKind> {
        Some(match b {
            1 => CommitKind::Auto,
            2 => CommitKind::Manual,
            3 => CommitKind::Merge,
            4 => CommitKind::Restore,
            5 => CommitKind::Reset,
            6 => CommitKind::Import,
            _ => return None,
        })
    }
}

/// A snapshot of a whole tree. Commits don't hash their parents; history lives in the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub root_tree: Id,
    pub time_ns: i64,
    /// `"<user>@<hostname>"`, 1–255 bytes.
    pub author: String,
    pub machine_id: [u8; 16],
    pub kind: CommitKind,
    /// May be empty; at most 64 KiB.
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub time_ns: i64,
    pub commit: Id,
    pub kind: CommitKind,
    pub message: String,
}

/// Up to 1000 log entries, oldest first, chained to the next-older segment by `prev`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogSegment {
    pub prev: Option<Id>,
    pub entries: Vec<LogEntry>,
}

impl LogSegment {
    pub const MAX_ENTRIES: usize = 1000;
}
