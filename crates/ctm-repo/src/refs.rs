//! Branch and snapshot refs: small JSON objects under `refs/`.
//!
//! Readers ignore fields they don't know, so later milestones can add fields (hints in R3,
//! `last_merge_base` in R7) without breaking v0 clients.

use std::fmt;

use serde::{Deserialize, Serialize};

use ctm_core::Id;
use ctm_core::pack::Location;

/// A valid branch or snapshot name: 1–100 characters from `[A-Za-z0-9._-]`, not starting with
/// `.` or `-`, no `..`, and not all hex with 8 or more characters.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BranchName(String);

impl BranchName {
    pub fn new(name: &str) -> crate::Result<BranchName> {
        let chars_ok = name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
        let looks_like_commit = name.len() >= 8 && name.bytes().all(|b| b.is_ascii_hexdigit());
        let valid = (1..=100).contains(&name.len())
            && chars_ok
            && !name.starts_with(['.', '-'])
            && !name.contains("..")
            && !looks_like_commit;
        if valid {
            Ok(BranchName(name.to_string()))
        } else {
            Err(crate::Error::InvalidName(name.to_string()))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The branch a lost push moves to: `<self>.<host>`, then `-2`, `-3`, … for later
    /// attempts. `host` is lowercased, with characters outside `[a-z0-9-]` replaced by `-`.
    pub fn auto_fork(&self, host: &str, attempt: u32) -> crate::Result<BranchName> {
        let host: String = host
            .chars()
            .map(|c| {
                let c = c.to_ascii_lowercase();
                if c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let name = format!("{}.{host}", self.0);
        if attempt <= 1 {
            BranchName::new(&name)
        } else {
            BranchName::new(&format!("{name}-{attempt}"))
        }
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
    /// Where `head` and `log` are stored (R3). Written by every ref write; untrusted.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "hint_hex")]
    pub head_hint: Option<Location>,
    #[serde(default, skip_serializing_if = "Option::is_none", with = "hint_hex")]
    pub log_hint: Option<Location>,
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
    /// Where `commit` is stored (R3); untrusted.
    #[serde(default, skip_serializing_if = "Option::is_none", with = "hint_hex")]
    pub commit_hint: Option<Location>,
    pub created_at: String,
    pub created_by: String,
}

pub(crate) mod id_hex {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    use ctm_core::Id;

    pub fn serialize<S: Serializer>(id: &Id, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&id.to_hex())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Id, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

/// A hint as 48 hex characters. One that doesn't parse reads as absent: hints are untrusted.
mod hint_hex {
    use serde::{Deserialize, Deserializer, Serializer};

    use ctm_core::pack::Location;

    pub fn serialize<S: Serializer>(hint: &Option<Location>, s: S) -> Result<S::Ok, S::Error> {
        match hint {
            Some(loc) => s.serialize_str(&hex::encode(loc.to_hint())),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Location>, D::Error> {
        let s = Option::<String>::deserialize(d)?;
        Ok(s.and_then(|s| hex::decode(s).ok())
            .and_then(|b| Location::from_hint(&b)))
    }
}
