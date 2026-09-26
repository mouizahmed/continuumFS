//! FastCDC 2020 content-defined chunking (`fastcdc` crate, `v2020`, version pinned).

/// Chunk size bounds, fixed per repo in the bucket's `config`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkerParams {
    pub min: u32,
    pub avg: u32,
    pub max: u32,
}

impl ChunkerParams {
    /// 256 KiB / 1 MiB / 4 MiB.
    pub const DEFAULT: ChunkerParams = ChunkerParams {
        min: 256 * 1024,
        avg: 1024 * 1024,
        max: 4 * 1024 * 1024,
    };

    /// The smallest bounds `fastcdc` accepts; used by tests to get many chunks from little data.
    pub const TEST_SMALL: ChunkerParams = ChunkerParams {
        min: 64,
        avg: 256,
        max: 1024,
    };
}

/// The length of the next chunk of `data`, which starts at a chunk boundary.
///
/// `eof` says whether `data` runs to the end of the file. Returns `None` when `data` is too
/// short to decide (fewer than `max` bytes and not at end of file).
pub fn next_cut(params: &ChunkerParams, data: &[u8], eof: bool) -> Option<usize> {
    let _ = (params, data, eof);
    todo!("M1: FastCDC cut")
}

/// The chunk lengths of a whole byte string. Partial re-chunking must always agree with this.
pub fn chunk_all(params: &ChunkerParams, data: &[u8]) -> Vec<u32> {
    let _ = (params, data);
    todo!("M1: full chunking")
}
