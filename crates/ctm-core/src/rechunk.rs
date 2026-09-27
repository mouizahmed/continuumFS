//! Partial re-chunking: re-cut only the chunks around each change, with a result identical
//! to a full re-chunk of the merged file.

use std::io;
use std::ops::Range;

use crate::chunker::{ChunkerParams, next_cut};

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
///
/// A cut at offset `c` depends on the bytes of its chunk and on byte `c` itself, so each change
/// restarts at the base chunk containing the byte before it. From there, new chunks are cut
/// until a cut lands exactly on a base boundary past the change; the base chunks after it are
/// then unchanged. A change that shrinks or grows the file (the tail) runs to end of file.
///
/// FastCDC 2020 tests cut points two bytes at a time and stops early near end of file, so a
/// cut is only possible at least one byte before EOF, and the last byte is tested only when the
/// remaining length is even. When EOF moves, the tail therefore starts at most `size − 1`, which
/// re-cuts every base boundary within two bytes of the new end.
pub fn rechunk_partial(
    params: &ChunkerParams,
    base: &[u32],
    changes: &Changes,
    merged: &mut dyn ReadAt,
) -> io::Result<Vec<Segment>> {
    // starts[i] is the offset of base chunk i; starts[base.len()] is the base length.
    let mut starts = Vec::with_capacity(base.len() + 1);
    let mut at = 0u64;
    starts.push(0);
    for &len in base {
        at += u64::from(len);
        starts.push(at);
    }
    if at != changes.base_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "base chunk lengths don't add up to base_len",
        ));
    }

    let regions = regions(changes);
    let size = changes.size;
    // Index of the base chunk a change starting at `s` restarts from.
    let restart = |s: u64| -> usize {
        if s == 0 || base.is_empty() {
            0
        } else {
            // The last chunk starting at or before byte s - 1 (past the end: the last chunk).
            starts[..base.len()].partition_point(|&b| b < s) - 1
        }
    };

    let mut buf = Window::default();
    let mut out = Vec::new();
    let mut next_base = 0; // first base chunk not yet emitted or superseded
    let mut i = 0;
    while i < regions.len() {
        let from = restart(regions[i].start).max(next_base);
        out.extend((next_base..from).map(Segment::Base));
        let mut pos = starts[from];
        let mut end = regions[i].end;
        i += 1;
        loop {
            if pos >= size {
                return Ok(out);
            }
            let want = (size - pos).min(u64::from(params.max)) as usize;
            let data = buf.get(merged, pos, want)?;
            let n = next_cut(params, data, pos + want as u64 == size)
                .expect("a full window or end of file always has a cut");
            out.push(Segment::New {
                offset: pos,
                len: n as u32,
            });
            pos += n as u64;
            // Absorb later changes whose restart point we've already passed.
            while i < regions.len() && starts[restart(regions[i].start)] < pos {
                end = end.max(regions[i].end);
                i += 1;
            }
            // Resync once past the change, on a base boundary, before end of file.
            if pos >= end
                && pos < size
                && let Ok(k) = starts[..base.len()].binary_search(&pos)
            {
                next_base = k;
                break;
            }
        }
    }
    out.extend((next_base..base.len()).map(Segment::Base));
    Ok(out)
}

/// The changed regions, sorted and merged: the dirty extents, plus the tail
/// `[min(base_visible, size − 1), size)` when the file no longer ends where its base did.
fn regions(changes: &Changes) -> Vec<Range<u64>> {
    let tail = (changes.base_visible < changes.base_len || changes.size != changes.base_len)
        .then_some(changes.base_visible.min(changes.size.saturating_sub(1)));
    let mut out: Vec<Range<u64>> = Vec::new();
    let limit = tail.unwrap_or(changes.size);
    for r in &changes.extents {
        let r = r.start..r.end.min(limit);
        if r.start >= r.end {
            continue;
        }
        match out.last_mut() {
            Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
            _ => out.push(r),
        }
    }
    if let Some(t) = tail {
        match out.last_mut() {
            Some(last) if t <= last.end => last.end = changes.size.max(last.end),
            _ => out.push(t..changes.size),
        }
    }
    out
}

/// The merged bytes read so far, so overlapping windows are never read twice.
#[derive(Default)]
struct Window {
    start: u64,
    bytes: Vec<u8>,
}

impl Window {
    fn get(&mut self, src: &mut dyn ReadAt, pos: u64, len: usize) -> io::Result<&[u8]> {
        let have_end = self.start + self.bytes.len() as u64;
        if pos < self.start || pos > have_end {
            self.start = pos;
            self.bytes.clear();
        } else {
            self.bytes.drain(..(pos - self.start) as usize);
            self.start = pos;
        }
        let have = self.bytes.len();
        if have < len {
            self.bytes.resize(len, 0);
            src.read_at(pos + have as u64, &mut self.bytes[have..])?;
        }
        Ok(&self.bytes[..len])
    }
}
