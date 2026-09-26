//! The bucket's `config` object, written once by `ctm init`.

use serde::{Deserialize, Serialize};

use ctm_core::{ChunkerParams, FormatParams, RepoKey};

pub const FORMAT_VERSION: u32 = 1;

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
        todo!("M1: RepoConfig::generate")
    }

    pub fn key(&self) -> crate::Result<RepoKey> {
        todo!("M1: RepoConfig::key")
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
