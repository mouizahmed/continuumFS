# R3: location hints

Every pack entry carries a hint per reference (where the referenced object's entry is), and refs
and snapshots carry hints for their roots, so a reader goes from a ref to any object with one
ranged GET per step and never needs the index. Measured with `bench/baseline.sh ttfb` against
Cloudflare R2, same machine and connection as the [v0 baseline](v0-baseline.md), on 2026-09-27.

The test is a fresh machine: empty caches and index mirror, then `ctm mount` of the Linux kernel
tree and `head -c 1` of `drivers/gpu/drm/amd/display/dc/core/dc.c` (six directories deep). R1 and
R3 write different formats (format 2 was still unreleased), so each build has its own repo in the
bucket with the same content: the kernel tree imported, then optionally 1,000 more pushes
(`bench/baseline.sh history`: one-file imports, one index segment each), as a repo accumulates
after 1,000 commits.

| Repo history | Build | Mount | First byte | Total (3 runs) |
|---|---|---|---|---|
| 1 push (1 index segment) | 0.2.0-dev with R1 (before) | 1.35–1.83 s | 0.53–0.97 s | 1.89–2.80 s |
| 1 push (1 index segment) | 0.2.0-dev with R3 (after) | 1.01–1.20 s | 0.56–1.08 s | 1.58–2.28 s |
| 1,001 pushes | 0.2.0-dev with R1 (before) | 4.42–7.07 s | 0.30–0.41 s | 4.72–7.48 s |
| 1,001 pushes | 0.2.0-dev with R3 (after) | 0.51–0.57 s | 0.31–0.45 s | 0.84–0.98 s |

- **Before R3**, the first object a fresh machine reads isn't in its (empty) index mirror, so it
  syncs the whole index first: a LIST and one GET per segment. That cost grows with every push
  (4–7 s after 1,000), and with repo size (the kernel's segment alone is 8.7 MB).
- **With R3** the mount reads the ref, then follows hints: no index requests at all, whatever the
  history. The first byte of a file six directories deep is about 7 sequential ranged GETs.
- The one-push R3 runs were slower than the 1,001-push ones on the same build; that's run-to-run
  network variation (they ran about half an hour apart), not history.
