//! Local directories, following the XDG base directory spec.

use std::env;
use std::path::PathBuf;

fn xdg(var: &str, fallback: &str) -> PathBuf {
    match env::var_os(var) {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir),
        _ => PathBuf::from(env::var_os("HOME").unwrap_or_default()).join(fallback),
    }
}

/// `~/.config/continuum/config.toml`
pub fn config_file() -> PathBuf {
    xdg("XDG_CONFIG_HOME", ".config").join("continuum/config.toml")
}

/// `~/.cache/continuum/<repo-id>/`
pub fn cache_dir(repo_id: &str) -> PathBuf {
    xdg("XDG_CACHE_HOME", ".cache")
        .join("continuum")
        .join(repo_id)
}

/// `~/.local/share/continuum/mounts/`: one working-state directory per mount.
pub fn mounts_dir() -> PathBuf {
    xdg("XDG_DATA_HOME", ".local/share").join("continuum/mounts")
}

/// `$XDG_RUNTIME_DIR/continuum/<mount-id>.sock`
pub fn control_socket(mount_id: &str) -> PathBuf {
    let runtime = env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(env::temp_dir);
    runtime.join("continuum").join(format!("{mount_id}.sock"))
}
