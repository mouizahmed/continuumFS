//! The per-mount control socket: newline-delimited JSON requests from the CLI to the mount
//! process.

use std::path::Path;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Commit { message: String },
    Status,
    Restore { path: String, at: String },
    Unmount { commit: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum Response {
    Ok { message: String },
    Error { message: String },
}

/// Sends one request to the mount process listening on `socket` and waits for its answer.
pub async fn call(socket: &Path, request: &Request) -> std::io::Result<Response> {
    let _ = (socket, request);
    todo!("M2: control socket client")
}
