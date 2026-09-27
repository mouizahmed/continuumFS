//! One level of tree diff. Callers recurse into directory pairs whose tree IDs differ, so
//! unchanged subtrees are never fetched.

use std::cmp::Ordering;

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
    let mut out = Vec::new();
    let mut a = old.entries.iter().peekable();
    let mut b = new.entries.iter().peekable();
    loop {
        match (a.peek(), b.peek()) {
            (None, None) => return out,
            (Some(x), None) => {
                out.push(Change::Removed((*x).clone()));
                a.next();
            }
            (None, Some(y)) => {
                out.push(Change::Added((*y).clone()));
                b.next();
            }
            (Some(x), Some(y)) => match x.name.cmp(&y.name) {
                Ordering::Less => {
                    out.push(Change::Removed((*x).clone()));
                    a.next();
                }
                Ordering::Greater => {
                    out.push(Change::Added((*y).clone()));
                    b.next();
                }
                Ordering::Equal => {
                    if x != y {
                        out.push(Change::Modified {
                            old: (*x).clone(),
                            new: (*y).clone(),
                        });
                    }
                    a.next();
                    b.next();
                }
            },
        }
    }
}
