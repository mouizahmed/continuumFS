# R6: garbage collection and retention

`ctm gc` drops auto-commits older than the retention period from branch logs, lists what's
unreachable in a dead list, and deletes what an earlier list (at least a day old) named and is
still unreachable. Measured with `bench/baseline.sh gc` against Cloudflare R2, same machine and
connection as the [v0 baseline](v0-baseline.md), on 2026-09-28, in a repo of its own:

```sh
BENCH_PREFIX=gc-bench bench/baseline.sh gc
```

A 100 MiB file is overwritten with new random data 10 times in a mount; each version is
auto-committed and pushed. Retention (`[retention] auto_days`) and the grace period
(`--grace-secs`, hidden) are set to zero so both runs happen in the benchmark: with the defaults
the first run marks, and a run a day later deletes.

| | Bucket |
|---|---|
| After 10 edit cycles (v0.3 keeps every version) | 1,000.9 MiB |
| After two `ctm gc` runs (8.0 s and 17.3 s) | 100.0 MiB |

- What's left is the current version: the head and the import that created the branch stay in
  the log (retention never drops a branch's head or non-auto commits), and `ctm fsck` finds no
  problems (9.6 s, reading the 100 MiB back).
- The first run drops the 9 older auto-commits and lists their data; the second deletes the 9
  data packs and repacks and compacts the rest.
