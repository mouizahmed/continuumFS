//! Garbage collection and retention (R6): `ctm gc`.
//!
//! A run takes the GC lease, drops old auto-commits from branch logs, marks everything
//! reachable from branches, snapshots, and live mount records, and lists what isn't in a new
//! dead list (`index/dead/<unix-secs>-<random>`). It then sweeps what earlier dead lists, at
//! least a grace period old, listed and is still unreachable: whole packs are deleted, packs
//! under half live are repacked, loose objects and orphan packs are deleted, and the index is
//! compacted into one segment.
//!
//! Clients never deduplicate against a dead object, and re-upload any dead object a push
//! references before the ref write ([`crate::objects`]), so nothing a new ref needs is ever
//! deleted: a dead object is either still unreachable a grace period later, or reachable again
//! through a copy made meanwhile.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};

use ctm_core::encoding::refs_of;
use ctm_core::pack::{
    DeadList, Entry, Listed, Location, PACK_TARGET, PackBuilder, PackId, decode_index,
    encode_index, pack_key,
};
use ctm_core::{
    ChunkList, ChunkPage, Commit, CommitKind, Content, Encoded, Id, LogEntry, LogSegment, Tree,
};
use ctm_store::PutMode;

use crate::refs::{BranchName, BranchRef, id_hex};
use crate::time::{now_ns, parse_rfc3339, rfc3339};
use crate::{Error, Repo, Result};

/// Trees and lists fetched at once while marking.
const MARK_CONCURRENCY: usize = 32;
/// A branch that keeps moving during retention is left for the next run after this many tries.
const RETENTION_TRIES: usize = 5;

#[derive(Clone, Debug)]
pub struct GcOptions {
    /// Auto-commits older than this leave branch logs (`[retention] auto_days`).
    pub auto_retention: Duration,
    /// How old a dead list must be before what it lists (if still unreachable) is deleted.
    pub grace: Duration,
    /// Mount records seen within this long keep their base commits.
    pub mount_ttl: Duration,
    /// A GC lease older than this belongs to a run that died, and is taken over.
    pub lock_stale: Duration,
    /// Report only; change nothing.
    pub dry_run: bool,
}

impl Default for GcOptions {
    fn default() -> GcOptions {
        GcOptions {
            auto_retention: Duration::from_secs(14 * 86_400),
            grace: Duration::from_secs(86_400),
            mount_ttl: Duration::from_secs(30 * 86_400),
            lock_stale: Duration::from_secs(6 * 3600),
            dry_run: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    /// Auto-commits dropped from branch logs by retention.
    pub dropped_commits: usize,
    pub live_objects: usize,
    /// Unreachable objects listed by this run, for a later run to delete.
    pub newly_dead: usize,
    /// Packs no segment lists, listed by this run.
    pub new_orphans: usize,
    /// Objects deleted (confirmed dead by an earlier list and still unreachable).
    pub deleted_objects: usize,
    pub deleted_packs: usize,
    pub repacked_packs: usize,
    pub deleted_loose: usize,
    pub deleted_orphans: usize,
    /// Pack bytes no longer stored (deleted packs, and what repacking dropped).
    pub freed_bytes: u64,
    /// Index segments replaced by one compacted segment.
    pub compacted_segments: usize,
}

/// `mounts/<machine-id>.<mount-id>`: a mount's pushed base, kept alive while it's recent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MountRecord {
    pub branch: String,
    #[serde(with = "id_hex")]
    pub base_commit: Id,
    pub last_seen: String,
    pub updated_by: String,
}

#[derive(Serialize, Deserialize)]
struct Lease {
    run_id: String,
    started_at: String,
    by: String,
}

const LOCK: &str = "locks/gc";

fn random_hex() -> String {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).expect("the OS random source works");
    hex::encode(b)
}

/// When a dead list was written, from its name (`index/dead/<unix-secs>-<random>`).
fn dead_list_time(key: &str) -> Option<i64> {
    let name = key.strip_prefix("index/dead/")?;
    let secs: i64 = name.split('-').next()?.parse().ok()?;
    Some(secs * 1_000_000_000)
}

impl Repo {
    /// Records that a mount of `mounted` (a branch, or the ref a read-only mount shows), based
    /// on `base_commit` as last pushed, is alive: GC keeps that commit while the record is
    /// younger than `GcOptions::mount_ttl`.
    pub async fn save_mount_record(
        &self,
        mount_id: &str,
        mounted: &str,
        base_commit: Id,
    ) -> Result<()> {
        let record = MountRecord {
            branch: mounted.to_string(),
            base_commit,
            last_seen: rfc3339(now_ns()),
            updated_by: self.identity().updated_by(),
        };
        let body = serde_json::to_vec_pretty(&record).expect("record serializes");
        self.backend()
            .put(
                &self.mount_record_key(mount_id),
                body.into(),
                PutMode::Overwrite,
            )
            .await?;
        Ok(())
    }

    pub async fn delete_mount_record(&self, mount_id: &str) -> Result<()> {
        Ok(self
            .backend()
            .delete(&self.mount_record_key(mount_id))
            .await?)
    }

    fn mount_record_key(&self, mount_id: &str) -> String {
        format!(
            "mounts/{}.{mount_id}",
            hex::encode(self.identity().machine_id)
        )
    }

    /// Runs garbage collection and retention.
    pub async fn gc(&self, opts: &GcOptions) -> Result<GcReport> {
        if opts.dry_run {
            return Gc { repo: self, opts }.run().await;
        }
        let lease = self.take_lease(opts).await?;
        let result = Gc { repo: self, opts }.run().await;
        // Only our own lease: a run that took it over as stale owns it now.
        if let Ok((body, _)) = self.backend().get(LOCK).await
            && serde_json::from_slice::<Lease>(&body).is_ok_and(|l| l.run_id == lease)
        {
            self.backend().delete(LOCK).await?;
        }
        result
    }

    async fn take_lease(&self, opts: &GcOptions) -> Result<String> {
        let run_id = random_hex();
        let lease = Lease {
            run_id: run_id.clone(),
            started_at: rfc3339(now_ns()),
            by: self.identity().updated_by(),
        };
        let body: Bytes = serde_json::to_vec_pretty(&lease)
            .expect("lease serializes")
            .into();
        match self
            .backend()
            .put(LOCK, body.clone(), PutMode::CreateOnly)
            .await
        {
            Ok(_) => return Ok(run_id),
            Err(ctm_store::Error::PreconditionFailed(_)) => {}
            Err(e) => return Err(e.into()),
        }
        let (held, etag) = self.backend().get(LOCK).await?;
        let other: Option<Lease> = serde_json::from_slice(&held).ok();
        let started = other
            .as_ref()
            .and_then(|l| parse_rfc3339(&l.started_at))
            .unwrap_or(0);
        if now_ns() - started < opts.lock_stale.as_nanos() as i64 {
            let by = other.map(|l| format!("{} since {}", l.by, l.started_at));
            return Err(Error::AlreadyExists(format!(
                "a GC run ({}); it is taken over once it's {} h old",
                by.unwrap_or_default(),
                opts.lock_stale.as_secs() / 3600
            )));
        }
        self.backend()
            .put(LOCK, body, PutMode::IfMatch(etag))
            .await
            .map_err(|e| match e {
                ctm_store::Error::PreconditionFailed(_) => {
                    Error::AlreadyExists("a GC run (another one just took over)".into())
                }
                e => e.into(),
            })?;
        Ok(run_id)
    }
}

enum Walk {
    Commit(Id),
    Tree(Id),
    List(Id),
    Page(Id),
}

struct Gc<'a> {
    repo: &'a Repo,
    opts: &'a GcOptions,
}

impl Gc<'_> {
    async fn run(&self) -> Result<GcReport> {
        let now = now_ns();
        let mut report = GcReport::default();
        let mut live: HashSet<Id> = HashSet::new();
        let mut commits: Vec<Id> = Vec::new();

        // Retention, and the roots it leaves.
        for branch in self.repo.list_branches().await? {
            let (segments, kept, dropped) = self.retain(&branch, now).await?;
            report.dropped_commits += dropped;
            live.extend(segments);
            commits.extend(kept);
        }
        for name in self.repo.list_snapshots().await? {
            commits.push(self.repo.read_snapshot(&name).await?.commit);
        }
        for key in self.repo.backend().list("mounts/").await? {
            let Ok((body, _)) = self.repo.backend().get(&key).await else {
                continue;
            };
            if let Ok(record) = serde_json::from_slice::<MountRecord>(&body)
                && parse_rfc3339(&record.last_seen)
                    .is_some_and(|t| now - t < self.opts.mount_ttl.as_nanos() as i64)
            {
                commits.push(record.base_commit);
            }
        }
        self.mark(commits, &mut live).await?;
        report.live_objects = live.len();

        // What's stored: every index entry (with duplicates), packs, loose objects.
        let listed = self.repo.backend().list("index/").await?;
        let (dead_lists, segments): (Vec<String>, Vec<String>) = listed
            .into_iter()
            .partition(|k| k.starts_with("index/dead/"));
        let entries: Vec<Listed> = stream::iter(segments.clone())
            .map(|key| async move {
                let (bytes, _) = self.repo.backend().get(&key).await?;
                decode_index(&bytes).map_err(|e| Error::CorruptJson {
                    what: key,
                    detail: e.to_string(),
                })
            })
            .buffered(16)
            .try_concat()
            .await?;
        let mut packs: HashMap<(PackId, bool), Vec<Listed>> = HashMap::new();
        for e in &entries {
            packs.entry((e.2.pack, e.1.is_data())).or_default().push(*e);
        }
        let mut stored_packs = Vec::new();
        for data in [true, false] {
            let prefix = if data { "packs/data/" } else { "packs/meta/" };
            for key in self.repo.backend().list(prefix).await? {
                if let Ok(id) = key[prefix.len()..].parse::<PackId>() {
                    stored_packs.push((id, data));
                }
            }
        }
        let orphans: Vec<(PackId, bool)> = stored_packs
            .iter()
            .filter(|p| !packs.contains_key(p))
            .copied()
            .collect();
        let mut loose: Vec<(String, Id)> = Vec::new();
        for prefix in ["chunks/", "meta/"] {
            for key in self.repo.backend().list(prefix).await? {
                if let Ok(id) = key[prefix.len()..].parse::<Id>() {
                    loose.push((key, id));
                }
            }
        }

        // This run's dead list.
        let dead_now: HashSet<Id> = entries
            .iter()
            .map(|e| e.0)
            .chain(loose.iter().map(|(_, id)| *id))
            .filter(|id| !live.contains(id))
            .collect();
        report.newly_dead = dead_now.len();
        report.new_orphans = orphans.len();
        if !self.opts.dry_run && (!dead_now.is_empty() || !orphans.is_empty()) {
            let list = DeadList {
                objects: dead_now.into_iter().collect(),
                orphans: orphans.clone(),
            };
            let key = format!("index/dead/{:010}-{}", now / 1_000_000_000, random_hex());
            self.repo
                .backend()
                .put(&key, list.encode().into(), PutMode::CreateOnly)
                .await?;
        }

        // Sweep what earlier lists, old enough, listed and is still unreachable.
        let cutoff = now - self.opts.grace.as_nanos() as i64;
        let old_lists: Vec<String> = dead_lists
            .into_iter()
            .filter(|k| dead_list_time(k).is_some_and(|t| t <= cutoff))
            .collect();
        let mut confirmed: HashSet<Id> = HashSet::new();
        let mut confirmed_orphans: HashSet<(PackId, bool)> = HashSet::new();
        for key in &old_lists {
            let (bytes, _) = self.repo.backend().get(key).await?;
            let list = DeadList::decode(&bytes).map_err(|e| Error::CorruptJson {
                what: key.clone(),
                detail: e.to_string(),
            })?;
            confirmed.extend(list.objects.into_iter().filter(|id| !live.contains(id)));
            confirmed_orphans.extend(list.orphans.into_iter().filter(|o| orphans.contains(o)));
        }
        let mut delete_packs: Vec<(PackId, bool)> = Vec::new();
        let mut repack: Vec<((PackId, bool), Vec<Listed>)> = Vec::new();
        for (pack, listed) in &packs {
            let total: u64 = listed.iter().map(|e| u64::from(e.2.len)).sum();
            let kept: Vec<Listed> = listed
                .iter()
                .filter(|e| !confirmed.contains(&e.0))
                .copied()
                .collect();
            let kept_bytes: u64 = kept.iter().map(|e| u64::from(e.2.len)).sum();
            if kept.is_empty() {
                delete_packs.push(*pack);
                report.deleted_objects += listed.len();
                report.freed_bytes += total;
            } else if kept_bytes * 2 < total {
                report.deleted_objects += listed.len() - kept.len();
                report.freed_bytes += total - kept_bytes;
                repack.push((*pack, kept));
            }
        }
        report.deleted_packs = delete_packs.len();
        report.repacked_packs = repack.len();
        let dead_loose: Vec<&String> = loose
            .iter()
            .filter(|(_, id)| confirmed.contains(id))
            .map(|(k, _)| k)
            .collect();
        report.deleted_loose = dead_loose.len();
        report.deleted_objects += dead_loose.len();
        report.deleted_orphans = confirmed_orphans.len();
        let changed = !delete_packs.is_empty() || !repack.is_empty();
        if segments.len() > 1 || changed {
            report.compacted_segments = segments.len();
        }
        if self.opts.dry_run {
            return Ok(report);
        }

        // Repack, then one segment for everything that stays, then delete.
        let mut moved: HashMap<Id, Location> = HashMap::new();
        let mut new_entries: Vec<Listed> = Vec::new();
        for ((pack, data), kept) in &repack {
            let (bytes, _) = self.repo.backend().get(&pack_key(*pack, *data)).await?;
            new_entries.extend(self.repack(&bytes, kept, *data, &mut moved).await?);
        }
        let gone: HashSet<(PackId, bool)> = delete_packs
            .iter()
            .chain(repack.iter().map(|(p, _)| p))
            .copied()
            .collect();
        if report.compacted_segments > 0 {
            let mut keep: Vec<Listed> = entries
                .iter()
                .filter(|e| !gone.contains(&(e.2.pack, e.1.is_data())))
                .copied()
                .collect();
            keep.extend(new_entries);
            let key = format!("index/{}{}", random_hex(), random_hex());
            self.repo
                .backend()
                .put(&key, encode_index(&keep).into(), PutMode::CreateOnly)
                .await?;
            for seg in &segments {
                self.repo.backend().delete(seg).await?;
            }
        } else if !new_entries.is_empty() {
            let key = format!("index/{}{}", random_hex(), random_hex());
            self.repo
                .backend()
                .put(&key, encode_index(&new_entries).into(), PutMode::CreateOnly)
                .await?;
        }
        for (pack, data) in gone.iter().chain(confirmed_orphans.iter()) {
            self.repo.backend().delete(&pack_key(*pack, *data)).await?;
        }
        for key in dead_loose {
            self.repo.backend().delete(key).await?;
        }
        for key in &old_lists {
            self.repo.backend().delete(key).await?;
        }
        Ok(report)
    }

    /// Drops auto-commits older than the retention period from a branch's log (never its
    /// head), rewriting the log and CASing the ref. Returns the log's segments and commits as
    /// kept, and how many commits were dropped.
    async fn retain(&self, branch: &BranchName, now: i64) -> Result<(Vec<Id>, Vec<Id>, usize)> {
        let cutoff = now - self.opts.auto_retention.as_nanos() as i64;
        for _ in 0..RETENTION_TRIES {
            let (r, etag) = match self.repo.read_ref(branch).await {
                Ok(x) => x,
                Err(Error::UnknownRef(_)) => return Ok((Vec::new(), Vec::new(), 0)),
                Err(e) => return Err(e),
            };
            let (segments, entries) = self.read_log(r.log).await?;
            let keep: Vec<LogEntry> = entries
                .iter()
                .filter(|e| {
                    !(e.kind == CommitKind::Auto && e.time_ns < cutoff && e.commit != r.head)
                })
                .cloned()
                .collect();
            let dropped = entries.len() - keep.len();
            let kept_commits: Vec<Id> = keep.iter().map(|e| e.commit).chain([r.head]).collect();
            if dropped == 0 || self.opts.dry_run {
                return Ok((segments, kept_commits, dropped));
            }
            let (new_segments, log) = self.write_log(keep).await?;
            let next = BranchRef { log, ..r };
            match self.repo.cas_ref(branch, &next, &etag).await {
                Ok(_) => return Ok((new_segments, kept_commits, dropped)),
                Err(Error::Store(ctm_store::Error::PreconditionFailed(_))) => continue,
                Err(e) => return Err(e),
            }
        }
        // It kept moving: leave its log as it is this time.
        let (r, _) = self.repo.read_ref(branch).await?;
        let (segments, entries) = self.read_log(r.log).await?;
        let commits = entries.iter().map(|e| e.commit).chain([r.head]).collect();
        Ok((segments, commits, 0))
    }

    /// A log's segments (newest first) and its entries (oldest first).
    async fn read_log(&self, head: Id) -> Result<(Vec<Id>, Vec<LogEntry>)> {
        let mut segments = Vec::new();
        let mut chunks = Vec::new();
        let mut next = Some(head);
        while let Some(id) = next {
            let seg = self.repo.get::<LogSegment>(&id).await?;
            segments.push(id);
            next = seg.prev;
            chunks.push(seg.entries);
        }
        let entries = chunks.into_iter().rev().flatten().collect();
        Ok((segments, entries))
    }

    /// Writes `entries` (oldest first) as a fresh chain of full segments; returns the segments
    /// and the head.
    async fn write_log(&self, entries: Vec<LogEntry>) -> Result<(Vec<Id>, Id)> {
        let mut prev = None;
        let mut segments = Vec::new();
        let mut encoded = Vec::new();
        for part in entries.chunks(LogSegment::MAX_ENTRIES) {
            let seg = LogSegment {
                prev,
                entries: part.to_vec(),
            };
            let enc = Encoded::new(self.repo.key(), &seg);
            prev = Some(enc.id);
            segments.push(enc.id);
            encoded.push(enc);
        }
        self.repo.put_objects(encoded).await?;
        let head = prev.expect("a log keeps at least its head");
        Ok((segments, head))
    }

    /// Adds everything reachable from `commits` to `live`: commits, trees, chunk lists, pages,
    /// and chunks (which are listed, never fetched).
    async fn mark(&self, commits: Vec<Id>, live: &mut HashSet<Id>) -> Result<()> {
        let mut frontier: Vec<Walk> = commits
            .into_iter()
            .filter(|c| live.insert(*c))
            .map(Walk::Commit)
            .collect();
        while !frontier.is_empty() {
            let batch = std::mem::take(&mut frontier);
            let found: Vec<Vec<(Id, u8)>> = stream::iter(batch)
                .map(|w| self.expand(w))
                .buffer_unordered(MARK_CONCURRENCY)
                .try_collect()
                .await?;
            for (id, kind) in found.into_iter().flatten() {
                if live.insert(id) {
                    match kind {
                        0 => {}
                        1 => frontier.push(Walk::Tree(id)),
                        2 => frontier.push(Walk::List(id)),
                        _ => frontier.push(Walk::Page(id)),
                    }
                }
            }
        }
        Ok(())
    }

    /// The objects one object references, each with what it is: 0 chunk, 1 tree, 2 chunk
    /// list, 3 chunk page.
    async fn expand(&self, w: Walk) -> Result<Vec<(Id, u8)>> {
        Ok(match w {
            Walk::Commit(id) => vec![(self.repo.get::<Commit>(&id).await?.root_tree, 1)],
            Walk::Tree(id) => self
                .repo
                .get::<Tree>(&id)
                .await?
                .entries
                .into_iter()
                .filter_map(|e| match e.content {
                    Content::Dir(t) => Some((t, 1)),
                    Content::ChunkList(l) => Some((l, 2)),
                    Content::Chunk(c) => Some((c, 0)),
                    Content::Inline(_) | Content::Symlink(_) => None,
                })
                .collect(),
            Walk::List(id) => self
                .repo
                .get::<ChunkList>(&id)
                .await?
                .pages
                .into_iter()
                .map(|p| (p.id, 3))
                .collect(),
            Walk::Page(id) => self
                .repo
                .get::<ChunkPage>(&id)
                .await?
                .chunks
                .into_iter()
                .map(|c| (c.id, 0))
                .collect(),
        })
    }

    /// Copies the `kept` entries of a pack into new packs, pointing their hints at children
    /// moved earlier in this run, and returns where each landed.
    async fn repack(
        &self,
        pack: &[u8],
        kept: &[Listed],
        data: bool,
        moved: &mut HashMap<Id, Location>,
    ) -> Result<Vec<Listed>> {
        let mut out = Vec::new();
        let mut builder: Option<PackBuilder> = None;
        let mut kept = kept.to_vec();
        kept.sort_by_key(|e| e.2.offset);
        for (id, ty, loc) in kept {
            let bytes = pack
                .get(loc.range().start as usize..loc.range().end as usize)
                .ok_or_else(|| Error::CorruptJson {
                    what: pack_key(loc.pack, data),
                    detail: "an index entry past the end of its pack".into(),
                })?;
            let entry = Entry::parse(bytes).map_err(|e| Error::CorruptJson {
                what: pack_key(loc.pack, data),
                detail: e.to_string(),
            })?;
            let refs = if ty.is_data() {
                Vec::new()
            } else {
                refs_of(ty, entry.payload, self.repo.params()).map_err(|e| Error::CorruptJson {
                    what: id.to_string(),
                    detail: e.to_string(),
                })?
            };
            let hints: Vec<Option<Location>> = if entry.hints.len() == refs.len() {
                refs.iter()
                    .zip(&entry.hints)
                    .map(|(r, h)| moved.get(r).copied().or(*h))
                    .collect()
            } else {
                refs.iter().map(|r| moved.get(r).copied()).collect()
            };
            let obj = Encoded {
                id,
                ty,
                payload: entry.payload.to_vec(),
                refs,
            };
            let b = builder.get_or_insert_with(|| {
                let mut pid = [0u8; 16];
                getrandom::fill(&mut pid).expect("the OS random source works");
                PackBuilder::new(PackId(pid))
            });
            let new = b.add(&obj, &hints);
            moved.insert(id, new);
            if b.len() >= PACK_TARGET {
                out.extend(
                    self.upload(builder.take().expect("just used"), data)
                        .await?,
                );
            }
        }
        if let Some(b) = builder {
            out.extend(self.upload(b, data).await?);
        }
        Ok(out)
    }

    async fn upload(&self, builder: PackBuilder, data: bool) -> Result<Vec<Listed>> {
        let id = builder.id();
        let (bytes, entries) = builder.finish();
        self.repo
            .backend()
            .put(&pack_key(id, data), bytes.into(), PutMode::Overwrite)
            .await?;
        Ok(entries)
    }
}
