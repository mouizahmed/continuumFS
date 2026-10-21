//! Deleting, undeleting, and renaming refs, and resetting a branch (v0.6).
//!
//! A delete leaves a tombstone under `refs/deleted/` holding the ref as it was. GC keeps
//! everything a tombstone reaches for [`TOMBSTONE_DAYS`], during which `undelete` puts the ref
//! back; after that the tombstone is dropped and the data collected like any other garbage.
//!
//! A branch delete writes the tombstone, then marks the ref deleted with a CAS (so a push racing
//! the delete fails and auto-forks, instead of landing on a ref about to disappear), then
//! removes it. A delete that stops part way leaves either a live branch with a spare tombstone,
//! or a marked ref that reads as gone and that the next create of the name replaces.

use std::time::Duration;

use ctm_core::{CommitKind, LogEntry};
use ctm_store::PutMode;

use crate::refs::{
    BranchName, BranchRef, BranchTombstone, SNAPSHOT_TOMBSTONES, SnapshotTombstone, TOMBSTONES,
    parse_tombstone_key,
};
use crate::refspec::RefSpec;
use crate::time::{now_ns, rfc3339};
use crate::{Error, Repo, Result};

/// How long a deleted ref can be undeleted (and its data is kept).
pub const TOMBSTONE_DAYS: u64 = 7;
pub(crate) const TOMBSTONE_TTL: Duration = Duration::from_secs(TOMBSTONE_DAYS * 86_400);

/// A ref that keeps moving is given up on after this many tries.
const TRIES: usize = 5;

/// A deleted branch or snapshot that can still be undeleted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deleted {
    pub name: BranchName,
    pub snapshot: bool,
    /// Unix seconds.
    pub deleted_at: i64,
}

fn moved(what: &str) -> Error {
    Error::Io(std::io::Error::other(format!(
        "{what} kept moving; try again"
    )))
}

fn is_lost_race(e: &Error) -> bool {
    matches!(e, Error::Store(ctm_store::Error::PreconditionFailed(_)))
}

impl Repo {
    /// Deletes a branch, keeping a tombstone `undelete_branch` can restore it from.
    pub async fn delete_branch(&self, name: &BranchName) -> Result<BranchRef> {
        for _ in 0..TRIES {
            let (r, etag) = self.read_ref_any(name).await?;
            if r.deleted_at.is_some() {
                // A delete that stopped after marking it: finish it.
                self.backend().delete(&name.branch_key()).await?;
                return Err(Error::UnknownRef(name.to_string()));
            }
            let now = now_ns();
            let tombstone = BranchTombstone {
                deleted_at: rfc3339(now),
                deleted_by: self.identity().updated_by(),
                branch: r.clone(),
            };
            let key = self
                .put_tombstone(|secs| name.branch_tombstone_key(secs), now, &tombstone)
                .await?;
            let marked = BranchRef {
                deleted_at: Some(rfc3339(now)),
                ..r.clone()
            };
            match self.cas_ref(name, &marked, &etag).await {
                Ok(_) => {
                    self.backend().delete(&name.branch_key()).await?;
                    return Ok(r);
                }
                Err(e) if is_lost_race(&e) => {
                    // It moved: the tombstone holds the old head. Start over.
                    self.backend().delete(&key).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(moved(name.as_str()))
    }

    /// Restores the branch most recently deleted under `name`.
    pub async fn undelete_branch(&self, name: &BranchName) -> Result<BranchRef> {
        let (key, _) = self.newest_tombstone(name, false).await?;
        let (body, _) = self.backend().get(&key).await?;
        let t: BranchTombstone = serde_json::from_slice(&body).map_err(|e| Error::CorruptJson {
            what: key.clone(),
            detail: e.to_string(),
        })?;
        let r = BranchRef {
            deleted_at: None,
            ..t.branch
        };
        self.create_ref(name, &r).await?;
        self.backend().delete(&key).await?;
        Ok(r)
    }

    /// Deletes a snapshot, keeping a tombstone `undelete_snapshot` can restore it from.
    pub async fn delete_snapshot(&self, name: &BranchName) -> Result<()> {
        let snapshot = self.read_snapshot(name).await?;
        let now = now_ns();
        let tombstone = SnapshotTombstone {
            deleted_at: rfc3339(now),
            deleted_by: self.identity().updated_by(),
            snapshot,
        };
        self.put_tombstone(|secs| name.snapshot_tombstone_key(secs), now, &tombstone)
            .await?;
        self.backend().delete(&name.snapshot_key()).await?;
        Ok(())
    }

    /// Restores the snapshot most recently deleted under `name`.
    pub async fn undelete_snapshot(&self, name: &BranchName) -> Result<()> {
        let (key, _) = self.newest_tombstone(name, true).await?;
        let (body, _) = self.backend().get(&key).await?;
        let t: SnapshotTombstone =
            serde_json::from_slice(&body).map_err(|e| Error::CorruptJson {
                what: key.clone(),
                detail: e.to_string(),
            })?;
        let body = serde_json::to_vec_pretty(&t.snapshot).expect("snapshot serializes");
        match self
            .backend()
            .put(&name.snapshot_key(), body.into(), PutMode::CreateOnly)
            .await
        {
            Err(ctm_store::Error::PreconditionFailed(_)) => {
                return Err(Error::AlreadyExists(format!("snapshot {name}")));
            }
            r => r?,
        };
        self.backend().delete(&key).await?;
        Ok(())
    }

    /// Branches and snapshots deleted within the last [`TOMBSTONE_DAYS`], newest first.
    pub async fn list_deleted(&self) -> Result<Vec<Deleted>> {
        let cutoff = (now_ns() - TOMBSTONE_TTL.as_nanos() as i64) / 1_000_000_000;
        let mut out: Vec<Deleted> = Vec::new();
        for key in self.backend().list(TOMBSTONES).await? {
            let parsed = match key.strip_prefix(SNAPSHOT_TOMBSTONES) {
                Some(_) => parse_tombstone_key(&key, SNAPSHOT_TOMBSTONES).map(|p| (p, true)),
                None => parse_tombstone_key(&key, TOMBSTONES).map(|p| (p, false)),
            };
            if let Some(((name, secs), snapshot)) = parsed
                && secs >= cutoff
            {
                out.push(Deleted {
                    name,
                    snapshot,
                    deleted_at: secs,
                });
            }
        }
        out.sort_by_key(|d| std::cmp::Reverse(d.deleted_at));
        Ok(out)
    }

    /// Renames a branch. Its `id` stays, so merges and forks still recognize it; a machine
    /// that still has the old name mounted auto-forks on its next push.
    pub async fn rename_branch(&self, old: &BranchName, new: &BranchName) -> Result<BranchRef> {
        for _ in 0..TRIES {
            let (r, etag) = self.read_ref(old).await?;
            let renamed = BranchRef {
                id: Some(r.key(old)),
                ..r.clone()
            };
            self.create_ref(new, &renamed).await?;
            let marked = BranchRef {
                deleted_at: Some(rfc3339(now_ns())),
                ..r
            };
            match self.cas_ref(old, &marked, &etag).await {
                Ok(_) => {
                    self.backend().delete(&old.branch_key()).await?;
                    return Ok(renamed);
                }
                Err(e) if is_lost_race(&e) => {
                    self.backend().delete(&new.branch_key()).await?;
                }
                Err(e) => return Err(e),
            }
        }
        Err(moved(old.as_str()))
    }

    /// Points a branch back at `to`'s commit, with a `reset` entry in its log; the old head
    /// stays in the log (and is dropped later by retention like any other entry).
    pub async fn reset(&self, branch: &BranchName, to: &RefSpec) -> Result<BranchRef> {
        let target = self.resolve(to).await?.commit;
        for _ in 0..TRIES {
            let (r, etag) = self.read_ref(branch).await?;
            let next = self
                .append_log(
                    &r,
                    vec![LogEntry {
                        time_ns: now_ns(),
                        commit: target,
                        kind: CommitKind::Reset,
                        message: format!("reset to {}", &target.to_hex()[..12]),
                    }],
                )
                .await?;
            match self.cas_ref(branch, &next, &etag).await {
                Ok(_) => return Ok(next),
                Err(e) if is_lost_race(&e) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(moved(branch.as_str()))
    }

    /// Writes a tombstone create-only, at the first free second from `now`.
    async fn put_tombstone<T: serde::Serialize>(
        &self,
        key_at: impl Fn(i64) -> String,
        now: i64,
        tombstone: &T,
    ) -> Result<String> {
        let body = bytes::Bytes::from(serde_json::to_vec_pretty(tombstone).expect("serializes"));
        let mut secs = now / 1_000_000_000;
        loop {
            let key = key_at(secs);
            match self
                .backend()
                .put(&key, body.clone(), PutMode::CreateOnly)
                .await
            {
                Ok(_) => return Ok(key),
                Err(ctm_store::Error::PreconditionFailed(_)) => secs += 1,
                Err(e) => return Err(e.into()),
            }
        }
    }

    /// The newest tombstone for `name` that can still be undeleted.
    async fn newest_tombstone(&self, name: &BranchName, snapshot: bool) -> Result<(String, i64)> {
        let prefix = if snapshot {
            SNAPSHOT_TOMBSTONES
        } else {
            TOMBSTONES
        };
        let newest = self
            .backend()
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|k| {
                let (n, secs) = parse_tombstone_key(&k, prefix)?;
                (n == *name).then_some((k, secs))
            })
            .max_by_key(|(_, secs)| *secs);
        let what = if snapshot {
            format!("snap/{name}")
        } else {
            name.to_string()
        };
        let Some((key, secs)) = newest else {
            return Err(Error::UnknownRef(format!("deleted {what}")));
        };
        let age = now_ns() / 1_000_000_000 - secs;
        if age > TOMBSTONE_TTL.as_secs() as i64 {
            return Err(Error::Io(std::io::Error::other(format!(
                "{what} was deleted more than {TOMBSTONE_DAYS} days ago; its data may be gone"
            ))));
        }
        Ok((key, secs))
    }
}
