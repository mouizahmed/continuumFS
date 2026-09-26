//! Partial re-chunking: re-cut only the chunks around each change, with a result identical
//! to a full re-chunk of the merged file.

use std::io;
use std::ops::Range;

use crate::chunker::ChunkerParams;

/// Random access to the merged bytes of a dirty file (staging over base, zeros past
/// `base_visible`).
pub trait ReadAt {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> io::Result<()>;
}

/// What changed in a file since its base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changes {
    pub base_len: u64,
    /// Base bytes below this offset can still show through; only ever shrinks.
    pub base_visible: u64,
    pub size: u64,
    /// Dirty byte ranges: sorted, non-overlapping, merged.
    pub extents: Vec<Range<u64>>,
}

/// One chunk of the new file, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Segment {
    /// Base chunk number `n`, unchanged.
    Base(usize),
    /// A new chunk: `len` merged bytes starting at `offset`.
    New { offset: u64, len: u32 },
}

/// Plans the new file's chunks. `base` holds the base file's chunk lengths.
///
/// Only the merged bytes around each change are read. The resulting boundaries equal
/// [`crate::chunker::chunk_all`] over the whole merged file.
pub fn rechunk_partial(
    params: &ChunkerParams,
    base: &[u32],
    changes: &Changes,
    merged: &mut dyn ReadAt,
) -> io::Result<Vec<Segment>> {
    let _ = (params, base, changes, merged);
    todo!("M1: partial re-chunk")
}
