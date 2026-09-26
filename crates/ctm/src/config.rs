//! `~/.config/continuum/config.toml`: this machine's ID and the repos it is connected to.

use std::collections::BTreeMap;
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

impl LocalConfig {
    /// Loads the config, creating it with a new machine ID if it doesn't exist.
    pub fn load_or_create(path: &Path) -> std::io::Result<LocalConfig> {
        let _ = path;
        todo!("M1: LocalConfig::load_or_create")
    }

    /// Records a repo as `[repos.<last URL path component>]` and makes it the default.
    pub fn connect(&mut self, url: &str, endpoint: Option<&str>) -> String {
        let _ = (url, endpoint);
        todo!("M1: LocalConfig::connect")
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let _ = path;
        todo!("M1: LocalConfig::save")
    }
}
