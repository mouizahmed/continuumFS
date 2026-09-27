//! The bucket's `config` object, written once by `ctm init`.

use serde::{Deserialize, Serialize};

use ctm_core::{ChunkerParams, FormatParams, RepoKey};

pub const FORMAT_VERSION: u32 = 1;
const ALGORITHM: &str = "fastcdc-v2020";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoConfig {
    pub format_version: u32,
    /// 32 lowercase hex characters.
    pub repo_id: String,
    /// 64 lowercase hex characters.
    pub chunk_id_key: String,
    pub chunker: ChunkerConfig,
    pub inline_max: u32,
    pub created_at: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkerConfig {
    /// Always `"fastcdc-v2020"`.
    pub algorithm: String,
    pub min: u32,
    pub avg: u32,
    pub max: u32,
}

impl RepoConfig {
    /// A new config with a random repo ID and key, and the default chunker.
    pub fn generate() -> RepoConfig {
        let mut repo_id = [0u8; 16];
        let mut key = [0u8; 32];
        getrandom::fill(&mut repo_id).expect("the OS random source works");
        getrandom::fill(&mut key).expect("the OS random source works");
        let c = ChunkerParams::DEFAULT;
        RepoConfig {
            format_version: FORMAT_VERSION,
            repo_id: hex::encode(repo_id),
            chunk_id_key: hex::encode(key),
            chunker: ChunkerConfig {
                algorithm: ALGORITHM.to_string(),
                min: c.min,
                avg: c.avg,
                max: c.max,
            },
            inline_max: FormatParams::DEFAULT.inline_max,
            created_at: crate::time::rfc3339(crate::time::now_ns()),
        }
    }

    /// Parses and checks a `config` object.
    pub fn parse(bytes: &[u8]) -> crate::Result<RepoConfig> {
        let corrupt = |detail: String| crate::Error::CorruptJson {
            what: "config".into(),
            detail,
        };
        let config: RepoConfig =
            serde_json::from_slice(bytes).map_err(|e| corrupt(e.to_string()))?;
        if config.format_version > FORMAT_VERSION {
            return Err(crate::Error::UnsupportedFormat(config.format_version));
        }
        if config.chunker.algorithm != ALGORITHM {
            return Err(corrupt(format!(
                "unknown chunker {:?}",
                config.chunker.algorithm
            )));
        }
        if config.repo_id.len() != 32 || hex::decode(&config.repo_id).is_err() {
            return Err(corrupt("repo_id".into()));
        }
        config.key()?;
        Ok(config)
    }

    pub fn key(&self) -> crate::Result<RepoKey> {
        let mut key = [0u8; 32];
        hex::decode_to_slice(&self.chunk_id_key, &mut key).map_err(|_| {
            crate::Error::CorruptJson {
                what: "config".into(),
                detail: "chunk_id_key".into(),
            }
        })?;
        Ok(RepoKey(key))
    }

    pub fn params(&self) -> FormatParams {
        FormatParams {
            chunker: ChunkerParams {
                min: self.chunker.min,
                avg: self.chunker.avg,
                max: self.chunker.max,
            },
            inline_max: self.inline_max,
        }
    }
}
