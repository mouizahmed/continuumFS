# v0 baseline

Measured on 2026-09-27 with `bench/baseline.sh`: Linux 7.2 on x86_64 (12 cores, btrfs), against
Cloudflare R2 over a home connection (about 300 Mbit/s up, 560 Mbit/s down, 60–100 ms per
request). Release build of v0. These are the "before" numbers for the roadmap; they are
published as measured.

| # | Benchmark | Result |
|---|---|---|
| 1 | `ctm fork` of a repo with 1k / 100k / 1M files (best of 5, whole CLI process) | 260 / 284 / 253 ms |
| 2 | Downloaded by `head -c 1M` of a 20 GiB file | 8.0 MiB |
| 3 | Cold sequential read of a 10 GiB file | ctm 40 MB/s · rclone mount 35 MB/s · mountpoint-s3 116 MB/s |
| 4 | `git clone` of nest v11.2.6: mount vs. local btrfs | 1.38 s vs. 0.91 s |
| 4 | `npm ci` (1,972 packages, 60,459 files): mount vs. local btrfs | 23.0 s vs. 6.4 s |
| 5 | Commit that clone + `node_modules` | 110 s; 15,949 objects, 355 MiB uploaded |
| 6 | Append 1 byte to a 20 GiB file, then commit | 1.9 s; 0.3 MiB downloaded, 6 objects (308 KiB) uploaded |
| 7 | Cold `find` over the Linux kernel tree (102,372 entries), empty metadata cache | 20.3 s |
| 7 | Cold `git status` over the Linux kernel tree, empty caches | 5,509 s (92 min); 1.5 GiB downloaded |

Setup steps, for reference:

| Step | Result |
|---|---|
| Import 1k / 100k / 1M tiny files | 1.1 / 3.1 / 19.2 s |
| Import the Linux kernel tree (95,972 files) | 550 s; 53,048 objects, 1.8 GiB uploaded |
| Import two files of 10 and 20 GiB sharing their first 10 GiB | 1,351 s; 28.1 GiB uploaded (shared chunks went up twice; fixed since) |

## Reading the numbers

- **Fork (1)** doesn't depend on repo size: it is one GET and one PUT, plus reading the repo's
  `config` and opening a TLS connection in a fresh process.
- **Lazy reads (2)**: reading 1 MiB of a 20 GiB file downloads the chunk it's in plus readahead.
- **Sequential reads (3)** are well below the link: v0 downloads whole chunks, at most 16 ahead,
  one request each. mountpoint-s3 issues many large parallel range requests. R4 (range reads,
  coalesced readahead) targets this.
- **Small-file writes (4)**: `npm ci` is 3.6× local disk (the full design aims for ≤ 2×). Every
  `write()` is a separate FUSE request and a SQLite transaction.
- **Small-file commits (5)**: every file over 4 KiB is its own object, one HEAD and one PUT each.
  R1 (packfiles) targets this.
- **Partial writes (6)**: appending to a 20 GiB file moves about one chunk each way; the commit
  finds it already fetched, because writes prefetch the base chunks a commit will need.
- **`git status` on a fresh mount (7)** re-reads every file: git's index records inode numbers and
  ctimes, and a new mount has different inode numbers (and reports ctime as mtime), so git
  re-hashes all 96k files, one small read at a time. A read-write mount fixes up the index on
  the first run, but every fresh mount pays this again.
