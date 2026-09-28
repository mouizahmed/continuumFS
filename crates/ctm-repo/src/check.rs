use std::collections::HashSet;

use ctm_core::{Chunk, ChunkList, ChunkPage, Commit, Content, Id, LogSegment, Tree};

use crate::{Repo, Result};

/// Reachability check (`ctm fsck`): from every branch and snapshot,
/// walks every commit in the logs and every object they reference, and checks that each one
/// exists and hash-verifies. Returns the problems found.
pub async fn check(repo: &Repo) -> Result<Vec<String>> {
    let mut c = Checker {
        repo,
        seen: HashSet::new(),
        problems: Vec::new(),
    };
    let mut commits = Vec::new();
    for name in repo.list_branches().await? {
        let (r, _) = match repo.read_ref(&name).await {
            Ok(x) => x,
            Err(e) => {
                c.problems.push(format!("branch {name}: {e}"));
                continue;
            }
        };
        commits.push(r.head);
        let mut next = Some(r.log);
        while let Some(id) = next.take() {
            if !c.seen.insert(id) {
                break;
            }
            match repo.get::<LogSegment>(&id).await {
                Ok(seg) => {
                    commits.extend(seg.entries.iter().map(|e| e.commit));
                    next = seg.prev;
                }
                Err(e) => c.problems.push(format!("log segment {id}: {e}")),
            }
        }
    }
    for name in repo.list_snapshots().await? {
        match repo.read_snapshot(&name).await {
            Ok(s) => commits.push(s.commit),
            Err(e) => c.problems.push(format!("snapshot {name}: {e}")),
        }
    }
    for id in commits {
        if let Some(commit) = c.fetch::<Commit>(id, "commit").await {
            c.tree(commit.root_tree).await;
        }
    }
    Ok(c.problems)
}

/// Like [`check`], for one ref: its commit, and for a branch every commit in its log.
pub async fn check_ref(repo: &Repo, spec: &crate::RefSpec) -> Result<Vec<String>> {
    let mut c = Checker {
        repo,
        seen: HashSet::new(),
        problems: Vec::new(),
    };
    let resolved = repo.resolve(spec).await?;
    let mut commits = vec![resolved.commit];
    if let Some((_, r, _)) = resolved.branch {
        let mut next = Some(r.log);
        while let Some(id) = next.take() {
            if !c.seen.insert(id) {
                break;
            }
            match repo.get::<LogSegment>(&id).await {
                Ok(seg) => {
                    commits.extend(seg.entries.iter().map(|e| e.commit));
                    next = seg.prev;
                }
                Err(e) => c.problems.push(format!("log segment {id}: {e}")),
            }
        }
    }
    for id in commits {
        if let Some(commit) = c.fetch::<Commit>(id, "commit").await {
            c.tree(commit.root_tree).await;
        }
    }
    Ok(c.problems)
}

struct Checker<'a> {
    repo: &'a Repo,
    seen: HashSet<Id>,
    problems: Vec<String>,
}

impl Checker<'_> {
    /// Fetches an object once; later calls for the same ID return `None`.
    async fn fetch<T: ctm_core::Object>(&mut self, id: Id, what: &str) -> Option<T> {
        if !self.seen.insert(id) {
            return None;
        }
        match self.repo.get::<T>(&id).await {
            Ok(obj) => Some(obj),
            Err(e) => {
                self.problems.push(format!("{what} {id}: {e}"));
                None
            }
        }
    }

    async fn tree(&mut self, root: Id) {
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            let Some(tree) = self.fetch::<Tree>(id, "tree").await else {
                continue;
            };
            for e in tree.entries {
                match e.content {
                    Content::Dir(t) => stack.push(t),
                    Content::Chunk(c) => {
                        self.fetch::<Chunk>(c, "chunk").await;
                    }
                    Content::ChunkList(l) => {
                        let Some(list) = self.fetch::<ChunkList>(l, "chunk list").await else {
                            continue;
                        };
                        for p in list.pages {
                            let Some(page) = self.fetch::<ChunkPage>(p.id, "chunk page").await
                            else {
                                continue;
                            };
                            for c in page.chunks {
                                self.fetch::<Chunk>(c.id, "chunk").await;
                            }
                        }
                    }
                    Content::Inline(_) | Content::Symlink(_) => {}
                }
            }
        }
    }
}
