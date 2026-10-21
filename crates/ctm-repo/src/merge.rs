//! `ctm merge <src> --into <dst>` (R7): a three-way merge of directory entries.
//!
//! The base comes from refs, never from commits (which have no parents): the commit of `src`
//! last merged into `dst` (`dst.merge_bases`), the commit of `dst` last merged into `src`, or
//! the fork point when one branch was forked from the other (or the earlier fork point when
//! both were forked from the same branch); the most recent of those. Whole
//! entries are merged, never bytes inside a file: an entry changed on only one side since the
//! base takes that side, one changed on both sides the same way is kept, and one changed on both
//! sides differently is a conflict, refused unless a resolution is given.

use std::collections::BTreeMap;

use ctm_core::{Commit, CommitKind, Content, DirEntry, Encoded, Id, LogEntry, Tree};

use crate::refs::BranchName;
use crate::time::now_ns;
use crate::{Error, Repo, Result};

/// A dst that keeps moving is given up on after this many tries.
const MERGE_TRIES: usize = 5;

/// How conflicts (entries both sides changed differently) are resolved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolve {
    /// Keep `dst`'s version.
    Ours,
    /// Take `src`'s version.
    Theirs,
    /// Keep `dst`'s version at the path, and `src`'s next to it as `<stem>.<src>.<ext>`.
    KeepBoth,
}

#[derive(Clone, Debug, Default)]
pub struct MergeOptions {
    /// `None` refuses the merge if anything conflicts.
    pub resolve: Option<Resolve>,
    /// Report only; write nothing.
    pub dry_run: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    /// Nothing in `src` since the base is missing from `dst`.
    UpToDate,
    Merged {
        /// The merge commit; `None` for a dry run.
        commit: Option<Id>,
        /// Paths whose entry was taken from `src` (a directory taken whole ends in `/`).
        taken: Vec<String>,
        /// Conflicting paths, resolved as asked.
        resolved: Vec<String>,
    },
    /// Conflicting paths; nothing was written.
    Conflicts(Vec<String>),
}

#[derive(Default)]
struct Merging {
    new_trees: Vec<Encoded>,
    taken: Vec<String>,
    conflicts: Vec<String>,
}

/// Entries are the same for merging when content and mode match (times don't count).
fn same(a: Option<&DirEntry>, b: Option<&DirEntry>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.content == b.content && a.mode == b.mode,
        _ => false,
    }
}

fn join(prefix: &str, name: &[u8]) -> String {
    let name = String::from_utf8_lossy(name);
    if prefix.is_empty() {
        name.into_owned()
    } else {
        format!("{prefix}/{name}")
    }
}

/// `report.pdf` → `report.<src>.pdf`; `data` → `data.<src>`; then `-2`, `-3`… if taken.
fn both_name(name: &[u8], src: &str, taken: &BTreeMap<Vec<u8>, DirEntry>) -> Vec<u8> {
    let name = String::from_utf8_lossy(name);
    let (stem, ext) = match name.rfind('.') {
        Some(i) if i > 0 => (&name[..i], &name[i..]),
        _ => (&name[..], ""),
    };
    let mut n = 1;
    loop {
        let suffix = if n == 1 {
            String::new()
        } else {
            format!("-{n}")
        };
        let candidate = format!("{stem}.{src}{suffix}{ext}").into_bytes();
        if !taken.contains_key(&candidate) {
            return candidate;
        }
        n += 1;
    }
}

impl Repo {
    /// Merges `src` into `dst`. Writes a `merge` commit on `dst` (CAS, retried if `dst`
    /// moves) and records `src`'s head as the base for their next merge.
    pub async fn merge(
        &self,
        src: &BranchName,
        dst: &BranchName,
        opts: &MergeOptions,
    ) -> Result<MergeOutcome> {
        for _ in 0..MERGE_TRIES {
            let (s, _) = self.read_ref(src).await?;
            let (d, etag) = self.read_ref(dst).await?;
            if s.head == d.head {
                return Ok(MergeOutcome::UpToDate);
            }
            let base = self.merge_base(src, &s, dst, &d).await?;
            if base == s.head {
                return Ok(MergeOutcome::UpToDate);
            }
            let tree =
                |c: Id| async move { Ok::<_, Error>(self.get::<Commit>(&c).await?.root_tree) };
            let (base_tree, ours, theirs) =
                (tree(base).await?, tree(d.head).await?, tree(s.head).await?);
            let mut m = Merging::default();
            let root = self
                .merge_dirs(
                    Some(base_tree),
                    ours,
                    theirs,
                    String::new(),
                    src.as_str(),
                    opts.resolve,
                    &mut m,
                )
                .await?;
            if !m.conflicts.is_empty() && opts.resolve.is_none() {
                return Ok(MergeOutcome::Conflicts(m.conflicts));
            }
            if root == ours {
                return Ok(MergeOutcome::UpToDate);
            }
            if opts.dry_run {
                return Ok(MergeOutcome::Merged {
                    commit: None,
                    taken: m.taken,
                    resolved: m.conflicts,
                });
            }
            self.upgrade_format().await?;
            self.put_objects(m.new_trees).await?;
            let message = format!("merge {src} into {dst}");
            let commit = Commit {
                root_tree: root,
                time_ns: now_ns(),
                author: self.identity().author(),
                machine_id: self.identity().machine_id,
                kind: CommitKind::Merge,
                message: message.clone(),
            };
            let enc = Encoded::new(self.key(), &commit);
            let id = enc.id;
            self.put_objects(vec![enc]).await?;
            let mut next = self
                .append_log(
                    &d,
                    vec![LogEntry {
                        time_ns: commit.time_ns,
                        commit: id,
                        kind: CommitKind::Merge,
                        message,
                    }],
                )
                .await?;
            next.merge_bases.insert(s.key(src), s.head);
            match self.cas_ref(dst, &next, &etag).await {
                Ok(_) => {
                    return Ok(MergeOutcome::Merged {
                        commit: Some(id),
                        taken: m.taken,
                        resolved: m.conflicts,
                    });
                }
                Err(Error::Store(ctm_store::Error::PreconditionFailed(_))) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(Error::Io(std::io::Error::other(format!(
            "{dst} kept moving during the merge; try again"
        ))))
    }

    /// The most recent commit both branches are known to contain.
    async fn merge_base(
        &self,
        src: &BranchName,
        s: &crate::BranchRef,
        dst: &BranchName,
        d: &crate::BranchRef,
    ) -> Result<Id> {
        let mut candidates = Vec::new();
        // Branches are known by their keys, not names: a branch deleted and made again under
        // the same name shares nothing with the old one.
        let (skey, dkey) = (s.key(src), d.key(dst));
        candidates.extend(d.merge_bases.get(&skey).copied());
        candidates.extend(s.merge_bases.get(&dkey).copied());
        if let Some(f) = s.forked_from.as_ref().filter(|f| f.parent_key() == dkey) {
            candidates.push(f.commit);
        }
        if let Some(f) = d.forked_from.as_ref().filter(|f| f.parent_key() == skey) {
            candidates.push(f.commit);
        }
        let time = |c: Id| async move { Ok::<_, Error>(self.get::<Commit>(&c).await?.time_ns) };
        // Siblings, forked from the same branch: the earlier fork point is in both.
        if let (Some(sf), Some(df)) = (&s.forked_from, &d.forked_from)
            && sf.parent_key() == df.parent_key()
        {
            let earlier = if time(sf.commit).await? <= time(df.commit).await? {
                sf.commit
            } else {
                df.commit
            };
            candidates.push(earlier);
        }
        let mut best: Option<(i64, Id)> = None;
        for c in candidates {
            let t = time(c).await?;
            if best.is_none_or(|(bt, _)| t > bt) {
                best = Some((t, c));
            }
        }
        best.map(|(_, c)| c).ok_or_else(|| Error::NoMergeBase {
            src: src.to_string(),
            dst: dst.to_string(),
        })
    }

    /// Merges one directory level; returns the merged tree's ID (new trees go in `m`).
    #[allow(clippy::too_many_arguments)]
    async fn merge_dirs(
        &self,
        base: Option<Id>,
        ours: Id,
        theirs: Id,
        prefix: String,
        src: &str,
        resolve: Option<Resolve>,
        m: &mut Merging,
    ) -> Result<Id> {
        // Whole subtrees decided without looking inside.
        if ours == theirs || base == Some(theirs) {
            return Ok(ours);
        }
        if base == Some(ours) {
            // Only src changed here: take its subtree, and report what that changes.
            let mut changes = Vec::new();
            let path = |p: &str| p.as_bytes().to_vec();
            self.diff_dirs(ours, theirs, path(&prefix), &mut changes)
                .await?;
            m.taken.extend(
                changes
                    .into_iter()
                    .map(|c| String::from_utf8_lossy(&c.path).into_owned()),
            );
            return Ok(theirs);
        }
        let entries = |t: Tree| -> BTreeMap<Vec<u8>, DirEntry> {
            t.entries.into_iter().map(|e| (e.name.clone(), e)).collect()
        };
        let b = match base {
            Some(id) => entries(self.get::<Tree>(&id).await?),
            None => BTreeMap::new(),
        };
        let o = entries(self.get::<Tree>(&ours).await?);
        let t = entries(self.get::<Tree>(&theirs).await?);
        let mut names: Vec<&Vec<u8>> = o.keys().chain(t.keys()).chain(b.keys()).collect();
        names.sort();
        names.dedup();
        let mut out: BTreeMap<Vec<u8>, DirEntry> = BTreeMap::new();
        let mut extra: Vec<DirEntry> = Vec::new();
        for name in names {
            let (eb, eo, et) = (b.get(name), o.get(name), t.get(name));
            let path = join(&prefix, name);
            let chosen: Option<DirEntry> = if same(eo, et) || same(et, eb) {
                eo.cloned()
            } else if same(eo, eb) {
                m.taken.push(path);
                et.cloned()
            } else if let (Some(o_e), Some(t_e)) = (eo, et)
                && let (Content::Dir(od), Content::Dir(td)) = (&o_e.content, &t_e.content)
            {
                let bd = match eb.map(|e| &e.content) {
                    Some(Content::Dir(x)) => Some(*x),
                    _ => None,
                };
                let merged = Box::pin(self.merge_dirs(bd, *od, *td, path, src, resolve, m)).await?;
                Some(DirEntry {
                    content: Content::Dir(merged),
                    ..o_e.clone()
                })
            } else if eo.is_none() || et.is_none() {
                // Changed on one side, deleted on the other: the change wins.
                if eo.is_none() {
                    m.taken.push(path);
                }
                eo.or(et).cloned()
            } else {
                m.conflicts.push(path);
                match resolve {
                    None | Some(Resolve::Ours) => eo.cloned(),
                    Some(Resolve::Theirs) => et.cloned(),
                    Some(Resolve::KeepBoth) => {
                        let mut copy = et.cloned().expect("both sides have it");
                        copy.file_id = Some(crate::new_file_id());
                        extra.push(copy);
                        eo.cloned()
                    }
                }
            };
            if let Some(e) = chosen {
                out.insert(e.name.clone(), e);
            }
        }
        for mut copy in extra {
            copy.name = both_name(&copy.name, src, &out);
            out.insert(copy.name.clone(), copy);
        }
        let tree = Tree {
            entries: out.into_values().collect(),
        };
        let enc = Encoded::new(self.key(), &tree);
        let id = enc.id;
        m.new_trees.push(enc);
        Ok(id)
    }
}
