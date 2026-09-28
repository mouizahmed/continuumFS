//! `~/.config/continuum/config.toml`: this machine's ID and the repos it is connected to.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalConfig {
    /// Generated on first run.
    pub machine_id: String,
    pub default_repo: Option<String>,
    #[serde(default)]
    pub repos: BTreeMap<String, RepoEntry>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub commit: CommitConfig,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoEntry {
    /// `s3://bucket/prefix` or `file:///path`.
    pub url: String,
    /// S3-compatible endpoint; absent for AWS S3.
    pub endpoint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheConfig {
    /// For example `"20GiB"`.
    pub chunks_max: String,
}

impl Default for CacheConfig {
    fn default() -> CacheConfig {
        CacheConfig {
            chunks_max: "20GiB".to_string(),
        }
    }
}

/// When mounts commit on their own (R2).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitConfig {
    /// Commit once a mount has had no writes for this long.
    pub quiet_secs: u64,
    /// Commit a mount that has had uncommitted changes for this long, even if writes go on.
    pub max_dirty_secs: u64,
}

impl Default for CommitConfig {
    fn default() -> CommitConfig {
        CommitConfig {
            quiet_secs: 5,
            max_dirty_secs: 60,
        }
    }
}

impl LocalConfig {
    /// Loads the config, creating it with a new machine ID if it doesn't exist.
    pub fn load_or_create(path: &Path) -> io::Result<LocalConfig> {
        match fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{}: {e}", path.display()),
                )
            }),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let config = LocalConfig {
                    machine_id: uuid::Uuid::new_v4().to_string(),
                    ..LocalConfig::default()
                };
                config.save(path)?;
                Ok(config)
            }
            Err(e) => Err(e),
        }
    }

    /// Records a repo as `[repos.<last URL path component>]` and makes it the default.
    pub fn connect(&mut self, url: &str, endpoint: Option<&str>) -> String {
        let name = url
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("default")
            .to_string();
        self.repos.insert(
            name.clone(),
            RepoEntry {
                url: url.to_string(),
                endpoint: endpoint.map(str::to_string),
            },
        );
        self.default_repo = Some(name.clone());
        name
    }

    /// The default repo, or an error telling the user to run `ctm init`.
    pub fn default_repo(&self) -> io::Result<(&str, &RepoEntry)> {
        self.default_repo
            .as_deref()
            .and_then(|name| self.repos.get(name).map(|e| (name, e)))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "no repo configured; run `ctm init <url>` first",
                )
            })
    }

    /// The machine ID as 16 bytes.
    pub fn machine_id(&self) -> io::Result<[u8; 16]> {
        uuid::Uuid::parse_str(&self.machine_id)
            .map(|u| *u.as_bytes())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("machine_id: {e}")))
    }

    /// Writes the config atomically (temp file, then rename).
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let dir = path.parent().expect("config path has a parent");
        fs::create_dir_all(dir)?;
        let text = toml::to_string_pretty(self)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tmp = dir.join(".config.toml.tmp");
        fs::write(&tmp, text)?;
        fs::rename(&tmp, path)
    }
}
