# Continuum

**Git branches for whole workspaces, backed by a bucket. No servers.**

Continuum is a filesystem for Linux that stores everything — file data, directories, history — as
immutable, content-addressed objects in an S3-compatible bucket. You mount a branch, work in it
like any directory, and commit. Forking a branch of any size is a single request, file contents
are downloaded only when something reads them, and editing a huge file uploads only the part
that changed.

- **The bucket is the only source of truth.** No metadata server, no database. Machines
  coordinate through S3 conditional writes.
- **Lazy reads.** `ls` and `stat` download nothing; reading part of a 20 GB file downloads the
  chunks around what's read. Local disk use is capped by a chunk cache (LRU, 20 GiB by default).
- **Partial writes.** Appending one byte to a 20 GB file and committing downloads and uploads
  about one chunk.
- **Branches.** `fork` is one request whatever the size; `diff` skips identical subtrees; every
  commit is a snapshot of the whole tree, sharing unchanged data with the rest of the history.
- **One writer per branch.** If two machines commit to the same branch, the one that loses the
  race is moved to `<branch>.<hostname>` automatically. Nothing is lost and nothing is merged
  behind your back.

It is written in Rust as a single binary (`ctm`), Apache-2.0.

> **Status: v0.** Linux only, for developers comfortable building from source. The storage
> format is versioned but young: repos written by this version stay readable by later ones, not
> the other way round. See [Limits](#limits).

## Quick start

You need Linux, Rust 1.98 or newer, and FUSE 3 (`fusermount3`: the `fuse3` package on most
distributions).

```sh
cargo install --git https://github.com/mouizahmed/continuumFS ctm
```

Try it without any cloud account, with a repo in a local directory:

```sh
$ ctm init file://$HOME/ctm-demo-bucket
Created repo ctm-demo-bucket at file:///home/me/ctm-demo-bucket

$ ctm import ~/some/project --branch main          # commit a directory as branch main
$ mkdir ~/ws && ctm mount main ~/ws                # mount it
$ echo "hello" >> ~/ws/notes.txt                   # edit it like any directory
$ ctm status ~/ws
Branch: main
Base commit: ec2814422370
Changes: 1
$ ctm commit ~/ws -m "first edit"
Committed 21dbac144e8c to main (uploaded 3 objects, 290 B)

$ ctm fork main experiment                         # a new branch: one request
$ mkdir ~/exp && ctm mount experiment ~/exp
$ rm ~/exp/notes.txt && ctm commit ~/exp -m "remove notes"
$ ctm diff main experiment
removed: notes.txt
$ ctm log experiment
$ ctm unmount ~/exp && ctm unmount ~/ws            # unmount commits anything left
```

### With a real bucket

Continuum works with any S3-compatible service that supports conditional writes
(`If-None-Match: *` and `If-Match`); `ctm init` checks and refuses one that doesn't. It is tested
against **Cloudflare R2** (recommended: no egress fees, and lazy reads are egress-heavy) and
RustFS; AWS S3 supports the same conditional writes but hasn't been tested yet.

Credentials come from the environment, and are never stored by Continuum:

```sh
export AWS_ACCESS_KEY_ID=...        # for R2: an R2 API token's S3 access key
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto              # R2; for AWS, the bucket's region

ctm init s3://my-bucket/continuum --endpoint https://<account-id>.r2.cloudflarestorage.com
```

For AWS S3, leave out `--endpoint`. On a second machine, run the same `ctm init`: it connects to
the existing repo instead of creating one.

## Commands

```text
ctm init <url> [--endpoint <url>]            create a repo (s3://bucket/prefix, file:///path), or connect to one
ctm import <dir> --branch <b> [-m msg]       commit a local directory to a branch
ctm export <ref>[:path] <dir>                write a ref (or a path in it) to a local directory
ctm ls <ref>[:path]    ctm cat <ref>:<path>

ctm mount <ref> <dir> [--read-only] [--foreground]
ctm unmount <dir> [--no-commit]              commits first unless --no-commit
ctm commit <dir> [-m msg]
ctm status <dir>
ctm restore <path> --at <ref>                replace a path inside a mount with its version in a ref

ctm fork <from-ref> <new-branch>             ctm branch list
ctm snapshot create <name> [--from <ref>]    ctm snapshot list
ctm log <ref> [-- <path>]                    ctm diff <ref-a> <ref-b> [--stat]
ctm cache stats [--json]
```

A `<ref>` is a branch (`main`), a snapshot (`snap/v1`), or a commit ID prefix of at least 8 hex
characters (`7f3a9c1e`), optionally followed by `:path`. Snapshots and commits always mount
read-only.

**Durability.** A `write` is visible to every process on the machine at once. `fsync` makes it
survive a crash of the machine, and uncommitted work survives the mount process being killed:
the next `ctm mount` of that branch in that directory picks it up. `ctm commit` returns once the
commit is in the bucket.

## How it works

```text
            your apps                               ctm commit / status / restore
               │ POSIX                                      │ control socket
               ▼                                            ▼
        ┌─────────────┐       ┌─────────────────────────────────────────────┐
        │ kernel FUSE │◄─────►│ ctm mount process (one per mount)           │
        └─────────────┘       │   working state: SQLite + sparse staging    │
                              │   caches: metadata + chunks (shared, LRU)   │
                              └──────────────────────┬──────────────────────┘
                                                     │ HTTPS
                              ┌──────────────────────▼──────────────────────┐
                              │ bucket: config · refs/ · meta/ · chunks/    │
                              └─────────────────────────────────────────────┘
```

A commit points to a tree (one object per directory); a directory entry points to its file's
content, stored inline up to 4 KiB, or as content-defined chunks (FastCDC, about 1 MiB each)
listed in pages. Object IDs are keyed BLAKE3 hashes, so identical data is stored once across all
branches and versions. Branch refs are small JSON objects updated with compare-and-swap; a
branch's history is a log the ref points to.

Writes go to sparse local staging files with a map of the dirty byte ranges, so nothing is
downloaded to write. At commit, only the chunks around each change are re-cut, and the result is
exactly what chunking the whole file would give.

## Limits

- **Linux only**, tested on x86_64. macOS is planned; Windows is not.
- **One writer per branch.** Concurrent writers are auto-forked, not merged. `ctm merge` isn't
  there yet.
- **The bucket only grows.** Nothing is deleted yet: every committed version stays, deleting a
  file frees no space, and objects from interrupted commits are never collected. Garbage
  collection and retention are on the roadmap.
- **Many small files mean many requests.** Every file over 4 KiB and every changed directory is
  its own object, so committing a large `node_modules` makes thousands of PUTs. Packfiles are on
  the roadmap.
- **Mounts see only their own writes.** Changes committed elsewhere show up in a new mount, not
  an existing one (`ctm status` says when the branch has moved).
- **Not supported:** hard links (`EPERM`), xattrs, FIFOs, sockets, and device nodes (`ENOTSUP`),
  `RENAME_EXCHANGE`, `fallocate`. Everything appears owned by the mounting user. `mmap` works but
  isn't tested yet.
- **One process per mount** and a control socket each; commands that don't touch a mount talk to
  the bucket directly.
- If a commit's ref update fails and the mount can't even tell whether it landed, writes return
  `EIO` until the next `ctm commit` settles it.

## Benchmarks

v0, against Cloudflare R2 from a home connection (about 300 Mbit/s up, 560 Mbit/s down).
Published as measured, wins and losses; details and a reading of each number are in
[`bench/results/v0-baseline.md`](bench/results/v0-baseline.md), and
[`bench/baseline.sh`](bench/baseline.sh) reproduces them.

| Benchmark | Result |
|---|---|
| `ctm fork` of a 1k / 100k / 1M-file repo | 260 / 284 / 253 ms |
| Downloaded by `head -c 1M` of a 20 GiB file | 8.0 MiB |
| Cold sequential read of 10 GiB | ctm 40 MB/s · rclone mount 35 MB/s · mountpoint-s3 116 MB/s |
| `npm ci` (60k files) in a mount vs. local disk | 23.0 s vs. 6.4 s |
| Commit that `node_modules` | 110 s, 15,949 objects |
| Append 1 byte to a 20 GiB file, then commit | 1.9 s; 0.3 MiB down, 308 KiB up |
| Cold `find` over the Linux kernel tree (102k entries) | 20 s |
| Cold `git status` over the Linux kernel tree | 92 min: git re-reads every file on a fresh mount |

Sequential throughput, small-file commits, and `git status` on a fresh mount are v0's weak spots;
they're what the next versions work on.

## Development

```sh
cargo test --workspace                 # unit, property, and end-to-end tests (FUSE tests skip without /dev/fuse)
docker compose up -d s3 && docker compose run --rm s3-init   # a local S3 server (RustFS) for the S3 tests
CTM_TEST_S3_URL=s3://ctm-test CTM_TEST_S3_ENDPOINT=http://localhost:9000 \
AWS_ACCESS_KEY_ID=ctmtest AWS_SECRET_ACCESS_KEY=ctmtestsecret AWS_REGION=us-east-1 \
cargo test --workspace
CTM_FSX_OPS=1000000 cargo test --release -p ctm --test mount fsx   # the long random-operations run
```

The workspace is five crates: `ctm-core` (object model, encoding, chunking; no I/O),
`ctm-store` (backends and caches), `ctm-repo` (refs, history, import/export), `ctm-fs` (the mount:
working state, reads, commit, FUSE), and `ctm` (the CLI).

## License

Apache-2.0.
