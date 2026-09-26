//! One level of tree diff. Callers recurse into directory pairs whose tree IDs differ, so
//! unchanged subtrees are never fetched.

use crate::object::{DirEntry, Tree};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Change {
    Added(DirEntry),
    Removed(DirEntry),
    Modified { old: DirEntry, new: DirEntry },
}

impl Change {
    pub fn name(&self) -> &[u8] {
        match self {
            Change::Added(e) | Change::Removed(e) | Change::Modified { new: e, .. } => &e.name,
        }
    }
}

/// The entries that differ between two directories, in name order. Identical entries
/// (including directories with equal tree IDs) are never returned.
pub fn diff_trees(old: &Tree, new: &Tree) -> Vec<Change> {
    let _ = (old, new);
    todo!("M1: tree diff")
}
