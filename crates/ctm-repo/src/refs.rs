//! Branch and snapshot refs: small JSON objects under `refs/`.
//!
//! Readers ignore fields they don't know, so later milestones can add fields (hints in R3,
//! `last_merge_base` in R7) without breaking v0 clients.

use std::fmt;

use serde::{Deserialize, Serialize};

use ctm_core::Id;

/// A valid branch or snapshot name: 1–100 characters from `[A-Za-z0-9._-]`, not starting with
/// `.` or `-`, no `..`, and not all hex with 8 or more characters.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BranchName(String);

impl BranchName {
    pub fn new(name: &str) -> crate::Result<BranchName> {
        let _ = name;
        todo!("M1: name validation")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The branch a lost push moves to: `<self>.<host>`, then `-2`, `-3`, … when taken.
    /// `host` is lowercased, with characters outside `[a-z0-9-]` replaced by `-`.
    pub fn auto_fork(&self, host: &str, attempt: u32) -> BranchName {
        let _ = (host, attempt);
        todo!("M3: auto-fork name")
    }

    pub fn branch_key(&self) -> String {
        format!("refs/branches/{}", self.0)
    }

    pub fn snapshot_key(&self) -> String {
        format!("refs/snapshots/{}", self.0)
    }
}

impl fmt::Display for BranchName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchRef {
    #[serde(with = "id_hex")]
    pub head: Id,
    #[serde(with = "id_hex")]
    pub log: Id,
    /// `None` for a branch made by `import`.
    pub forked_from: Option<ForkedFrom>,
    pub updated_at: String,
    /// `"<user>@<hostname>/<machine-id>"`.
    pub updated_by: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkedFrom {
    /// The ref as given: a branch name, `snap/<name>`, or a full commit ID.
    #[serde(rename = "ref")]
    pub from: String,
    #[serde(with = "id_hex")]
    pub commit: Id,
    pub at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    #[serde(with = "id_hex")]
    pub commit: Id,
    pub created_at: String,
    pub created_by: String,
}

mod id_hex {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use ctm_core::Id;

    pub fn serialize<S: Serializer>(id: &Id, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&id.to_hex())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Id, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}
