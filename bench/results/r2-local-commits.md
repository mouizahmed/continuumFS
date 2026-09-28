# R2: local commits and background push

A commit is local: dirty files are re-chunked, the new objects go into packs in the mount's
`outgoing/` directory (fsynced), and the commit is queued in `state.db`. The mount pushes queued
commits in the background, and commits on its own after 5 s without writes. Measured with
`bench/baseline.sh` against Cloudflare R2, same machine and connection as the
[v0 baseline](v0-baseline.md), on 2026-09-28, v0.2.0 and then the R2 build in one session:

```sh
CTM_BIN=… bench/baseline.sh writepush   # and append, npm
```

| Benchmark | v0.2.0 (before) | R2 (after) |
|---|---|---|
| Commit 1 GiB of new data (fresh random bytes) | 52.7 s | 16.1 s to commit; pushed 8.9 s later |
| A 256 MiB write started 1 s into that commit | 51.7 s: blocked for the whole upload | 15.1 s: blocked for the local commit only |
| Commit a clone of nest v11.2.6 + `node_modules` (60,459 files) | 18.1 s | 2.9 s; pushed 2.2 s later |
| Append 1 byte to a 20 GiB file, then commit | 1.76 s | 0.41 s |

- **Commits no longer wait for the network,** and writes are paused only while dirty data is
  re-chunked and written locally. Uploads happen afterwards, while the mount keeps working.
- **The local commit is bound by the local disk.** The 1 GiB case writes 1 GiB of staging (the
  test's own write) and then 1 GiB of packs, fsynced, on a DRAM-less SATA SSD whose sustained
  writes are far below its burst speed. The `node_modules` case is mostly many small files.
- **Pushes are faster too:** v0.2 uploaded a file's chunks four at a time as it re-chunked it;
  a push uploads whole 32 MiB packs four at a time.
- Two things found on the way, both fixed before these runs:
  - the chunk cache summed its table on every insert, which made caching a commit's 60k chunks
    quadratic; the total is now kept by SQLite triggers;
  - committed chunks were copied into the chunk cache during the commit; they're copied from the
    pushed packs after the push instead, as the design says, off the commit path.
- The `node_modules` numbers dedup against earlier runs' objects in the bucket (v0.2 uploaded
  52.5 MiB of 357 MiB), so they measure mostly local work plus metadata.
