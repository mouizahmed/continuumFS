# R1: packfiles and a lazy index

Objects are written into 32 MiB packs (chunks and metadata separately) instead of one bucket
object each, with one index segment per push. Measured with `bench/baseline.sh` against
Cloudflare R2, same machine and connection as the [v0 baseline](v0-baseline.md), on 2026-09-27:

```sh
BENCH_RESULTS=… LINUX_BRANCH=linux-r1 bench/baseline.sh linux   # a fresh import into packs
BENCH_RESULTS=… bench/baseline.sh npm
```

| # | Benchmark | v0.1.0 (before) | 0.2.0-dev with R1 (after) |
|---|---|---|---|
| 5 | Commit a clone of nest v11.2.6 + `node_modules` (60,459 files) | 110 s; 15,949 objects PUT, 355 MiB | 14.9 s; 13 objects PUT, 357 MiB |
| — | Import the Linux kernel tree (95,972 files) | 550 s; 53,048 objects PUT, 1.8 GiB | 46.6 s; 60 objects PUT, 1.8 GiB |
| 7 | Cold `find` over the kernel tree (102,372 entries), empty caches | 20.3 s | 12.4 s |
| 4 | `npm ci` in a mount vs. local btrfs (not affected by R1) | 23.0 s vs. 6.4 s | 26.9 s vs. 6.8 s |

- **Commits and imports** now cost a few PUTs per 32 MiB instead of one PUT (and, in v0, one
  HEAD) per file over 4 KiB. The `node_modules` commit is 7.4× faster and now close to the
  upload bandwidth (357 MiB at about 300 Mbit/s is about 10 s).
- The kernel import uploaded everything again rather than deduplicating against the v0 import in
  the same bucket: objects written before R1 are loose and in no index, so the first push that
  needs one writes it into a pack.
- **Cold `find`** reads the same trees as before, each as a ranged GET into a metadata pack, and
  starts with an index sync (the cache, including `index.db`, was empty). It was faster in this
  run; R1 doesn't change the number of requests, so treat part of that as run-to-run variation.
- `npm ci` writes only to local staging; the difference is noise (the local-disk run moved too).
