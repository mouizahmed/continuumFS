# R4: range reads and coalesced readahead

Random reads fetch only the 64 KiB blocks they touch, cached in sparse files; sequential reads
fetch runs of chunks stored next to each other in a pack with one streamed ranged GET per
16 MiB span, verifying and handing over each chunk as it arrives. Measured with
`bench/baseline.sh` against Cloudflare R2, same machine and connection as the
[v0 baseline](v0-baseline.md), on 2026-09-28, reading a 10 GiB file of random data imported
into packs (`SEQ_BRANCH=seq10-v2`):

```sh
SEQ_BRANCH=seq10-v2 SEQREAD_TOOLS="ctm mount-s3" bench/baseline.sh seqread
SEQ_BRANCH=seq10-v2 bench/baseline.sh randread
bench/baseline.sh head
```

| Benchmark | v0.2.0 (before) | R4 (after) |
|---|---|---|
| Cold sequential read of 10 GiB | 40 MB/s | 100 MB/s (mountpoint-s3 in the same session: 115 MB/s) |
| 200 cold random 4 KiB reads: p50 / p90 / p99 | 88 / 123 / 205 ms | 46 / 56 / 98 ms |
| Downloaded by those 200 reads | 311.8 MiB | 13.1 MiB |
| Downloaded by `head -c 1M` of a 20 GiB file (v0.1 data) | 8.0 MiB | 5.1 MiB |

- **Random reads** used to download each touched chunk whole (1.6 MiB on average); now they
  download one or two 64 KiB blocks, which also halves the latency.
- **Sequential reads** went through three limits before reaching the link:
  1. readahead handed out 1–2 chunks per GET, because it dispatched on every 256 KiB read; it
     now waits for whole 16 MiB spans (about 65 MB/s before, with or without more GETs in flight);
  2. a span's chunks were only usable once all 16 MiB had arrived; spans are now streamed;
  3. reads waited on `cache.db` writes, which can stall for seconds behind a checkpoint's fsync
     while the disk absorbs the download (the same read ran at 57–64 MB/s with the cache on disk
     and 110 MB/s on tmpfs). Reads no longer write `cache.db` at all.
- The sequential read downloaded 10,240.2 MiB for 10,240 MiB read: nothing twice.
- `head -c 1M` reads v0.1's loose objects, which have no pack to coalesce; it downloads the
  blocks of the first reads, then whole chunks plus a 2 MiB readahead window once the reads look
  sequential.
- Runs vary by ±15 % on this connection; the tuning runs (2 GiB each) ranged from 82 to 109 MB/s
  for the chosen settings.
