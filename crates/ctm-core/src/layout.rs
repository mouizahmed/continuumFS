//! Canonical form of a file's content: the same bytes always produce the same entry.

use crate::object::{ChunkPage, ChunkRef};

/// Chunk references per full page.
pub const PAGE_MAX: usize = 4096;

/// Splits a file's chunks into pages of exactly [`PAGE_MAX`], with only the last page partial.
pub fn paginate(chunks: &[ChunkRef]) -> Vec<ChunkPage> {
    chunks
        .chunks(PAGE_MAX)
        .map(|c| ChunkPage { chunks: c.to_vec() })
        .collect()
}
