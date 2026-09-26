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
        let _ = s;
        todo!("M1: ref syntax")
    }
}
