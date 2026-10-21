//! Branch and snapshot refs: small JSON objects under `refs/`.
//!
//! Readers ignore fields they don't know, so later milestones can add fields (hints in R3,
//! `merge_bases` in R7, `id` and `deleted_at` in v0.6) without breaking v0 clients.

use std::collections::BTreeMap;
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

    /// Where a deleted branch's tombstone goes: `refs/deleted/<name>.<unix-secs>`.
    pub fn branch_tombstone_key(&self, secs: i64) -> String {
        format!("{TOMBSTONES}{}.{secs}", self.0)
    }

    /// Where a deleted snapshot's tombstone goes: `refs/deleted/snap/<name>.<unix-secs>`.
    pub fn snapshot_tombstone_key(&self, secs: i64) -> String {
        format!("{SNAPSHOT_TOMBSTONES}{}.{secs}", self.0)
    }
}

/// Tombstones of deleted branches; snapshots' are under `snap/` (names can't contain `/`).
pub const TOMBSTONES: &str = "refs/deleted/";
pub const SNAPSHOT_TOMBSTONES: &str = "refs/deleted/snap/";

/// A tombstone key's name and deletion time; `None` for anything else.
pub fn parse_tombstone_key(key: &str, prefix: &str) -> Option<(BranchName, i64)> {
    let rest = key.strip_prefix(prefix)?;
    let (name, secs) = rest.rsplit_once('.')?;
    Some((BranchName::new(name).ok()?, secs.parse().ok()?))
}

/// A new branch's `id`: 16 random hex characters.
pub fn new_branch_id() -> String {
    format!(
        "{:016x}",
        getrandom::u64().expect("the OS random source works")
    )
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
    /// For each branch merged into this one (R7), by its [key](BranchRef::key): the commit of
    /// it merged last, the base for its next merge. Kept by forks, and a GC root.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty", with = "id_map")]
    pub merge_bases: BTreeMap<String, Id>,
    /// Set when the branch is created (v0.6) and kept across renames; see [`BranchRef::key`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Set by `ctm branch delete` just before the ref is removed: readers treat it as gone, and
    /// a push that races the delete fails its CAS (and auto-forks) instead of being lost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_at: Option<String>,
    pub updated_at: String,
    /// `"<user>@<hostname>/<machine-id>"`.
    pub updated_by: String,
}

impl BranchRef {
    /// What merge bases and fork points know this branch by: its `id`, or for a branch made
    /// before v0.6, the name it was created with (`name`, unless it was since renamed, which
    /// sets `id` to that name). Never a valid ID and a branch name at once: IDs are 16 hex
    /// characters, which names can't be.
    pub fn key(&self, name: &BranchName) -> String {
        self.id.clone().unwrap_or_else(|| name.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkedFrom {
    /// The ref as given: a branch name, `snap/<name>`, or a full commit ID.
    #[serde(rename = "ref")]
    pub from: String,
    #[serde(with = "id_hex")]
    pub commit: Id,
    pub at: String,
    /// The [key](BranchRef::key) of the branch forked from (v0.6); absent for a snapshot or
    /// commit, and for forks made before v0.6.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl ForkedFrom {
    /// The key of the branch forked from: `id`, or the name for a fork made before v0.6.
    pub fn parent_key(&self) -> &str {
        self.id.as_deref().unwrap_or(&self.from)
    }
}

/// What `ctm branch delete` leaves for `undelete`, and GC keeps the branch's data for.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchTombstone {
    pub deleted_at: String,
    pub deleted_by: String,
    pub branch: BranchRef,
}

/// What `ctm snapshot delete` leaves.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTombstone {
    pub deleted_at: String,
    pub deleted_by: String,
    pub snapshot: SnapshotRef,
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

/// A map of names to IDs, with the IDs as hex.
mod id_map {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serializer, de::Error, ser::SerializeMap};

    use ctm_core::Id;

    pub fn serialize<S: Serializer>(map: &BTreeMap<String, Id>, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(map.len()))?;
        for (k, v) in map {
            m.serialize_entry(k, &v.to_hex())?;
        }
        m.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, Id>, D::Error> {
        BTreeMap::<String, String>::deserialize(d)?
            .into_iter()
            .map(|(k, v)| Ok((k, v.parse().map_err(D::Error::custom)?)))
            .collect()
    }
}
