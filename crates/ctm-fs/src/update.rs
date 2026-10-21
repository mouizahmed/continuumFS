//! Following the branch (v0.6): `ctm update`, and mounts that update themselves when their
//! branch moves on another machine.
//!
//! An update applies what changed on the branch since the mount's base to the mount in place,
//! keeping uncommitted local changes. It first diffs the base tree against the branch's head
//! (only the bucket's trees), then, in one pass under the state lock, finds the in-memory nodes
//! those changes touch and either applies everything or nothing:
//!
//! - an entry the branch changed that is also changed here (or deleted here) is a conflict, and
//!   nothing is applied; the mount's next push auto-forks, as before;
//! - a clean node for a changed entry is dropped (detached if open, so open handles keep
//!   reading what they opened), and the kernel's caches for it are invalidated; the next
//!   lookup sees the new entry;
//! - a directory both sides changed beneath keeps its local changes and gets the branch's tree
//!   as its new base.
//!
//! Then the base becomes the branch's head. Commits waiting to be pushed must go first: their
//! trees don't have the branch's changes.

use std::collections::BTreeMap;

use ctm_core::{Content, DirEntry, Id};

use crate::commit::save_base;
use crate::db;
use crate::inode::ROOT;
use crate::state::{Base, Inner, Invalidate, MountState};
use crate::{Error, Result};

/// What `update` did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateOutcome {
    /// The mount already shows the branch's head (or isn't of a branch).
    UpToDate,
    /// Moved to `commit`; `changed` entries came from the branch.
    Updated { commit: Id, changed: usize },
    /// Entries changed both here and on the branch; nothing was applied.
    Conflicts(Vec<String>),
    /// Local commits are still waiting to be pushed; push (or let them auto-fork) first.
    Unpushed,
    /// The branch no longer exists (deleted, or renamed).
    Gone,
}

/// One entry that differs between the base tree and the branch's head.
struct Change {
    /// Names from the root to the entry's directory.
    dir: Vec<Vec<u8>>,
    name: Vec<u8>,
    /// The branch's entry; `None` if it was removed.
    new: Option<DirEntry>,
    /// Both are directories: the entries below that differ follow as their own changes.
    both_dirs: bool,
}

fn path(dir: &[Vec<u8>], name: &[u8]) -> String {
    let mut parts: Vec<String> = dir
        .iter()
        .map(|p| String::from_utf8_lossy(p).into_owned())
        .collect();
    parts.push(String::from_utf8_lossy(name).into_owned());
    parts.join("/")
}

impl MountState {
    /// Brings the mount up to its branch's head, keeping uncommitted changes.
    pub async fn update(&self) -> Result<UpdateOutcome> {
        let Some(branch) = self.branch() else {
            return Ok(UpdateOutcome::UpToDate);
        };
        // No writes, commits, or pushes meanwhile.
        let _gate = self.gate.write().await;
        let _pushing = self.pushing.lock().await;
        if !self.read_only && !self.lock().db().pending()?.is_empty() {
            return Ok(UpdateOutcome::Unpushed);
        }
        let (remote, etag) = match self.repo.read_ref(&branch).await {
            Ok(x) => x,
            Err(ctm_repo::Error::UnknownRef(_)) => return Ok(UpdateOutcome::Gone),
            Err(e) => return Err(e.into()),
        };
        let base = self.lock().base.clone();
        if base.etag.as_ref() == Some(&etag) || base.commit == remote.head {
            return Ok(UpdateOutcome::UpToDate);
        }
        let new_root = self
            .repo
            .get::<ctm_core::Commit>(&remote.head)
            .await?
            .root_tree;
        let mut changes = Vec::new();
        self.diff_for_update(base.root, new_root, Vec::new(), &mut changes)
            .await?;

        let mut invalidations = Vec::new();
        let mut dropped = Vec::new();
        {
            let mut inner = self.lock();
            let conflicts = conflicts(&inner, &changes);
            if !conflicts.is_empty() {
                return Ok(UpdateOutcome::Conflicts(conflicts));
            }
            let mut saved = Vec::new();
            for c in &changes {
                let Some(parent) = find_dir(&inner, &c.dir) else {
                    continue;
                };
                invalidations.push(Invalidate::Entry {
                    parent,
                    name: c.name.clone(),
                });
                invalidations.push(Invalidate::Inode(parent));
                let Some(ino) = inner.inodes.child(parent, &c.name) else {
                    continue;
                };
                if c.both_dirs {
                    let new = c.new.clone().expect("both sides have it");
                    let node = inner.inodes.get_mut(ino).expect("indexed");
                    if node.changed {
                        node.entry.content = new.content;
                        saved.push(ino);
                    } else {
                        node.entry = new;
                    }
                    invalidations.push(Invalidate::Inode(ino));
                } else {
                    drop_subtree(&mut inner, ino, &mut dropped);
                    invalidations.push(Invalidate::Inode(ino));
                }
            }
            let root = inner.inodes.get_mut(ROOT).expect("root");
            root.entry.content = Content::Dir(new_root);
            if root.changed {
                saved.push(ROOT);
            }
            let next = Base {
                commit: remote.head,
                root: new_root,
                branch_ref: Some(remote),
                etag: Some(etag),
                ..base
            };
            let Inner { inodes, db, .. } = &mut *inner;
            if let Some(db) = db.as_mut() {
                db.tx(|t| {
                    for i in &saved {
                        db::save_node(t, *i, inodes.get(*i).expect("live"))?;
                    }
                    save_base(t, &next)
                })?;
            }
            inner.base = next;
        }
        for ino in &dropped {
            self.drop_staging(*ino);
        }
        invalidations.push(Invalidate::Inode(ROOT));
        for i in invalidations {
            self.invalidate(i);
        }
        Ok(UpdateOutcome::Updated {
            commit: self.base_commit(),
            changed: changes.iter().filter(|c| !c.both_dirs).count(),
        })
    }

    /// The entries that differ between two trees, parents before what's below them.
    async fn diff_for_update(
        &self,
        old: Id,
        new: Id,
        dir: Vec<Vec<u8>>,
        out: &mut Vec<Change>,
    ) -> Result<()> {
        if old == new {
            return Ok(());
        }
        let load = |id: Id| async move {
            self.tree(id)
                .await
                .map_err(|e| Error::Io(std::io::Error::from_raw_os_error(e.0)))
        };
        let (a, b) = (load(old).await?, load(new).await?);
        let a: BTreeMap<&[u8], &DirEntry> =
            a.entries.iter().map(|e| (e.name.as_slice(), e)).collect();
        let b_map: BTreeMap<&[u8], &DirEntry> =
            b.entries.iter().map(|e| (e.name.as_slice(), e)).collect();
        let mut names: Vec<&[u8]> = a.keys().chain(b_map.keys()).copied().collect();
        names.sort();
        names.dedup();
        for name in names {
            let (eo, en) = (a.get(name).copied(), b_map.get(name).copied());
            if eo == en {
                continue;
            }
            let dirs = match (eo.map(|e| &e.content), en.map(|e| &e.content)) {
                (Some(Content::Dir(x)), Some(Content::Dir(y))) => Some((*x, *y)),
                _ => None,
            };
            out.push(Change {
                dir: dir.clone(),
                name: name.to_vec(),
                new: en.cloned(),
                both_dirs: dirs.is_some(),
            });
            if let Some((x, y)) = dirs {
                let mut below = dir.clone();
                below.push(name.to_vec());
                Box::pin(self.diff_for_update(x, y, below, out)).await?;
            }
        }
        Ok(())
    }
}

/// The in-memory node for a directory path, if it's in memory.
fn find_dir(inner: &Inner, dir: &[Vec<u8>]) -> Option<u64> {
    let mut ino = ROOT;
    for part in dir {
        ino = inner.inodes.child(ino, part)?;
    }
    Some(ino)
}

/// Changed entries the branch also changed: a changed node or a deletion here, at an entry
/// the branch changed (other than a directory both sides changed beneath).
fn conflicts(inner: &Inner, changes: &[Change]) -> Vec<String> {
    let mut out = Vec::new();
    for c in changes {
        let Some(parent) = find_dir(inner, &c.dir) else {
            continue;
        };
        let deleted_here = inner.whiteouts.contains(&(parent, c.name.clone()));
        let node = inner.inodes.child(parent, &c.name);
        let changed_here = node
            .and_then(|i| inner.inodes.get(i))
            .is_some_and(|n| n.changed);
        let clash = if c.both_dirs {
            deleted_here
        } else {
            deleted_here || changed_here
        };
        if clash {
            out.push(path(&c.dir, &c.name));
        }
    }
    out
}

/// Drops a clean node and everything in memory below it (all clean: a changed node marks its
/// ancestors changed). Open ones are detached, to go at their last release.
fn drop_subtree(inner: &mut Inner, ino: u64, dropped: &mut Vec<u64>) {
    let mut stack = vec![ino];
    let mut all = Vec::new();
    while let Some(d) = stack.pop() {
        all.push(d);
        for (_, c) in inner.inodes.children(d) {
            stack.push(c);
        }
    }
    for i in all {
        if inner.inodes.get(i).is_some_and(|n| n.open > 0) {
            inner.inodes.detach(i);
            let n = inner.inodes.get_mut(i).expect("live");
            n.unlinked = true;
        } else {
            inner.inodes.remove(i);
            inner.extents.remove(&i);
        }
        dropped.push(i);
    }
}
