//! The v0 ref syntax: a branch name, `snap/<name>`, a commit ID prefix (8+ hex characters),
//! each optionally followed by `:<path>`. `~N` and `@time` arrive in R8.

use std::str::FromStr;

use crate::refs::BranchName;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RefTarget {
    Branch(BranchName),
    Snapshot(BranchName),
    /// 8–64 lowercase hex characters, resolved by listing `meta/<prefix>`.
    CommitPrefix(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefSpec {
    pub target: RefTarget,
    /// A path inside the ref (`<ref>:<path>`), without a leading `/`.
    pub path: Option<String>,
}

impl FromStr for RefSpec {
    type Err = crate::Error;

    fn from_str(s: &str) -> crate::Result<RefSpec> {
        let bad = || crate::Error::UnknownRef(s.to_string());
        let (target, path) = match s.split_once(':') {
            Some((t, p)) => (t, (!p.is_empty()).then_some(p)),
            None => (s, None),
        };
        if path.is_some_and(|p| p.starts_with('/')) {
            return Err(bad());
        }
        let target = if let Some(snap) = target.strip_prefix("snap/") {
            RefTarget::Snapshot(BranchName::new(snap).map_err(|_| bad())?)
        } else if target.len() >= 8
            && target.len() <= 64
            && target
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            RefTarget::CommitPrefix(target.to_string())
        } else {
            RefTarget::Branch(BranchName::new(target).map_err(|_| bad())?)
        };
        Ok(RefSpec {
            target,
            path: path.map(|p| p.trim_end_matches('/').to_string()),
        })
    }
}
