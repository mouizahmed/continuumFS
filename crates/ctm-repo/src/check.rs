use crate::{Repo, Result};

/// Test-only reachability check (exposed as `ctm fsck` in R6): from every branch and snapshot,
/// walks every commit in the logs and every object they reference, and checks that each one
/// exists and hash-verifies. Returns the problems found.
pub async fn check(repo: &Repo) -> Result<Vec<String>> {
    let _ = repo;
    todo!("M1: reachability check")
}
