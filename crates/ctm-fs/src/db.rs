//! `state.db`: a mount's working state, persisted so it survives a crash of the mount
//! process (SQLite, WAL, `synchronous=NORMAL`; `fsync` upgrades one commit to FULL).
//!
//! - `meta`: the branch; the local head the mount shows (`base_commit`, `base_root`); the last
//!   pushed ref and its ETag; and `pending_commit`/`pending_ref`/`pending_seq` while a push's
//!   ref update is in flight;
//! - `pending`: commits made locally and not yet pushed, oldest first (R2);
//! - `inodes`: every changed entry and its ancestors;
//! - `whiteouts`: base entries that were deleted or renamed away;
//! - `extents`: the dirty byte ranges of each file.

use std::collections::HashMap;
use std::ops::Range;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use ctm_core::{CommitKind, Content, DirEntry, Id};

use crate::Result;
use crate::inode::Node;

pub struct WorkDb {
    conn: Connection,
}

/// Everything `load` reads back: inode rows, whiteouts, and extents by inode.
pub type Loaded = (Vec<Row>, Vec<(u64, Vec<u8>)>, HashMap<u64, Vec<Range<u64>>>);

/// A persisted inode row, as loaded on remount.
pub struct Row {
    pub ino: u64,
    pub node: Node,
}

fn content_parts(c: &Content) -> (i64, Vec<u8>) {
    match c {
        Content::Inline(b) => (1, b.clone()),
        Content::Chunk(id) => (2, id.0.to_vec()),
        Content::ChunkList(id) => (3, id.0.to_vec()),
        Content::Dir(id) => (4, id.0.to_vec()),
        Content::Symlink(t) => (5, t.clone()),
    }
}

fn content_from(tag: i64, bytes: Vec<u8>) -> Option<Content> {
    let id = || bytes.as_slice().try_into().ok().map(Id);
    Some(match tag {
        1 => Content::Inline(bytes.clone()),
        2 => Content::Chunk(id()?),
        3 => Content::ChunkList(id()?),
        4 => Content::Dir(id()?),
        5 => Content::Symlink(bytes.clone()),
        _ => return None,
    })
}

impl WorkDb {
    pub fn open(state_dir: &Path) -> Result<WorkDb> {
        std::fs::create_dir_all(state_dir.join("staging"))?;
        let conn = Connection::open(state_dir.join("state.db"))?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS inodes (
                 ino INTEGER PRIMARY KEY, parent INTEGER NOT NULL, name BLOB NOT NULL,
                 mode INTEGER NOT NULL, mtime INTEGER NOT NULL, size INTEGER NOT NULL,
                 content_tag INTEGER NOT NULL, content BLOB NOT NULL,
                 base_len INTEGER NOT NULL, base_visible INTEGER NOT NULL, dirty INTEGER NOT NULL,
                 file_id INTEGER);
             CREATE TABLE IF NOT EXISTS whiteouts (
                 parent INTEGER NOT NULL, name BLOB NOT NULL, PRIMARY KEY (parent, name));
             CREATE TABLE IF NOT EXISTS extents (
                 ino INTEGER NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL,
                 PRIMARY KEY (ino, start));
             CREATE TABLE IF NOT EXISTS pending (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT, commit_id TEXT NOT NULL,
                 kind INTEGER NOT NULL, message TEXT NOT NULL, time_ns INTEGER NOT NULL);",
        )?;
        // Working state written by v0.1 has no file_id column.
        let has_file_id: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('inodes') WHERE name = 'file_id'")?
            .exists([])?;
        if !has_file_id {
            conn.execute_batch("ALTER TABLE inodes ADD COLUMN file_id INTEGER")?;
        }
        Ok(WorkDb { conn })
    }

    /// Whether a state directory holds uncommitted changes, unpushed commits, or a push in
    /// flight.
    pub fn has_changes(state_dir: &Path) -> Result<bool> {
        if !state_dir.join("state.db").exists() {
            return Ok(false);
        }
        let db = WorkDb::open(state_dir)?;
        let count = |sql: &str| -> Result<i64> { Ok(db.conn.query_row(sql, [], |r| r.get(0))?) };
        Ok(count("SELECT COUNT(*) FROM inodes")? > 0
            || count("SELECT COUNT(*) FROM whiteouts")? > 0
            || count("SELECT COUNT(*) FROM pending")? > 0
            || db.meta("pending_commit")?.is_some())
    }

    pub fn meta(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?1", [key], |r| r.get(0))
            .optional()?)
    }

    /// Runs `f` in one transaction.
    pub fn tx<T>(&mut self, f: impl FnOnce(&Transaction<'_>) -> Result<T>) -> Result<T> {
        let tx = self.conn.transaction()?;
        let v = f(&tx)?;
        tx.commit()?;
        Ok(v)
    }

    /// Makes everything written so far durable: one commit with `synchronous=FULL`.
    pub fn sync(&mut self) -> Result<()> {
        self.conn.execute_batch("PRAGMA synchronous = FULL")?;
        let r = self.tx(|t| {
            set_meta(t, "synced", "1")?;
            Ok(())
        });
        self.conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        r
    }

    /// Commits made locally and not yet pushed, oldest first.
    pub fn pending(&self) -> Result<Vec<PendingCommit>> {
        let mut stmt = self
            .conn
            .prepare("SELECT seq, commit_id, kind, message, time_ns FROM pending ORDER BY seq")?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(seq, id, kind, message, time_ns)| {
                let bad = || {
                    crate::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("bad pending commit {seq} in state.db"),
                    ))
                };
                Ok(PendingCommit {
                    seq,
                    commit: id.parse().map_err(|_| bad())?,
                    kind: u8::try_from(kind)
                        .ok()
                        .and_then(CommitKind::from_u8)
                        .ok_or_else(bad)?,
                    message,
                    time_ns,
                })
            })
            .collect()
    }

    pub fn load(&self) -> Result<Loaded> {
        let mut rows = Vec::new();
        let mut stmt = self.conn.prepare(
            "SELECT ino, parent, name, mode, mtime, size, content_tag, content,
                    base_len, base_visible, dirty, file_id FROM inodes",
        )?;
        let mut q = stmt.query([])?;
        while let Some(r) = q.next()? {
            let tag: i64 = r.get(6)?;
            let bytes: Vec<u8> = r.get(7)?;
            let Some(content) = content_from(tag, bytes) else {
                continue;
            };
            let entry = DirEntry {
                name: r.get(2)?,
                mode: r.get::<_, i64>(3)? as u16,
                mtime_ns: r.get(4)?,
                size: r.get::<_, i64>(5)? as u64,
                content,
                btime_ns: None,
                xattrs: None,
                file_id: r.get::<_, Option<i64>>(11)?.map(|v| v as u64),
            };
            let mut node = Node::new(r.get::<_, i64>(1)? as u64, entry);
            node.base_len = r.get::<_, i64>(8)? as u64;
            node.base_visible = r.get::<_, i64>(9)? as u64;
            node.dirty = r.get::<_, i64>(10)? != 0;
            node.changed = true;
            rows.push(Row {
                ino: r.get::<_, i64>(0)? as u64,
                node,
            });
        }
        let mut whiteouts = Vec::new();
        let mut stmt = self.conn.prepare("SELECT parent, name FROM whiteouts")?;
        let mut q = stmt.query([])?;
        while let Some(r) = q.next()? {
            whiteouts.push((r.get::<_, i64>(0)? as u64, r.get(1)?));
        }
        let mut extents: HashMap<u64, Vec<Range<u64>>> = HashMap::new();
        let mut stmt = self
            .conn
            .prepare("SELECT ino, start, end FROM extents ORDER BY ino, start")?;
        let mut q = stmt.query([])?;
        while let Some(r) = q.next()? {
            extents
                .entry(r.get::<_, i64>(0)? as u64)
                .or_default()
                .push(r.get::<_, i64>(1)? as u64..r.get::<_, i64>(2)? as u64);
        }
        Ok((rows, whiteouts, extents))
    }
}

/// A commit made locally, waiting to be pushed.
#[derive(Clone, Debug)]
pub struct PendingCommit {
    pub seq: i64,
    pub commit: Id,
    pub kind: CommitKind,
    pub message: String,
    pub time_ns: i64,
}

pub fn add_pending(
    t: &Transaction<'_>,
    commit: &Id,
    kind: CommitKind,
    message: &str,
    time_ns: i64,
) -> Result<()> {
    t.execute(
        "INSERT INTO pending (commit_id, kind, message, time_ns) VALUES (?1, ?2, ?3, ?4)",
        params![commit.to_hex(), i64::from(kind as u8), message, time_ns],
    )?;
    Ok(())
}

/// Drops the pending commits up to and including `seq`, once they're pushed.
pub fn drop_pending(t: &Transaction<'_>, seq: i64) -> Result<()> {
    t.execute("DELETE FROM pending WHERE seq <= ?1", [seq])?;
    Ok(())
}

pub fn set_meta(t: &Transaction<'_>, key: &str, value: &str) -> Result<()> {
    t.execute(
        "INSERT INTO meta (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = ?2",
        params![key, value],
    )?;
    Ok(())
}

pub fn delete_meta(t: &Transaction<'_>, key: &str) -> Result<()> {
    t.execute("DELETE FROM meta WHERE key = ?1", [key])?;
    Ok(())
}

pub fn save_node(t: &Transaction<'_>, ino: u64, n: &Node) -> Result<()> {
    let (tag, bytes) = content_parts(&n.entry.content);
    t.execute(
        "INSERT OR REPLACE INTO inodes (ino, parent, name, mode, mtime, size, content_tag,
             content, base_len, base_visible, dirty, file_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            ino as i64,
            n.parent as i64,
            n.entry.name,
            i64::from(n.entry.mode),
            n.entry.mtime_ns,
            n.entry.size as i64,
            tag,
            bytes,
            n.base_len as i64,
            n.base_visible as i64,
            i64::from(n.dirty),
            n.entry.file_id.map(|v| v as i64),
        ],
    )?;
    Ok(())
}

pub fn delete_node(t: &Transaction<'_>, ino: u64) -> Result<()> {
    t.execute("DELETE FROM inodes WHERE ino = ?1", [ino as i64])?;
    t.execute("DELETE FROM extents WHERE ino = ?1", [ino as i64])?;
    t.execute("DELETE FROM whiteouts WHERE parent = ?1", [ino as i64])?;
    Ok(())
}

pub fn add_whiteout(t: &Transaction<'_>, parent: u64, name: &[u8]) -> Result<()> {
    t.execute(
        "INSERT OR IGNORE INTO whiteouts (parent, name) VALUES (?1, ?2)",
        params![parent as i64, name],
    )?;
    Ok(())
}

pub fn set_extents(t: &Transaction<'_>, ino: u64, extents: &[Range<u64>]) -> Result<()> {
    t.execute("DELETE FROM extents WHERE ino = ?1", [ino as i64])?;
    for e in extents {
        t.execute(
            "INSERT INTO extents (ino, start, end) VALUES (?1, ?2, ?3)",
            params![ino as i64, e.start as i64, e.end as i64],
        )?;
    }
    Ok(())
}

/// Drops the whole working state (after a successful commit).
pub fn clear_working_state(t: &Transaction<'_>) -> Result<()> {
    t.execute_batch("DELETE FROM inodes; DELETE FROM whiteouts; DELETE FROM extents;")?;
    Ok(())
}

pub fn staging_path(state_dir: &Path, ino: u64) -> PathBuf {
    state_dir.join("staging").join(ino.to_string())
}
