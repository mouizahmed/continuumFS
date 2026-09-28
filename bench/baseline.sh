#!/usr/bin/env bash
# v0 baseline benchmarks: the "before" numbers for the roadmap.
#
#   bench/baseline.sh [all|fork|head|seqread|randread|append|npm|linux|linuxgit|gitremount|history|ttfb|writepush|gc]
#
# BENCH_RESULTS appends to another results file (for a roadmap item's "after" run),
# LINUX_BRANCH imports the kernel into another branch (so a new format is measured from scratch),
# BENCH_PREFIX uses another repo in the same bucket (with its own local state), for builds
# whose formats differ, and SEQ_BRANCH reads the 10 GiB file from another branch (imported on
# first use from a directory holding only that file).
#
# Needs a bucket and an env file (BENCH_ENV, default ~/.config/continuum/bench.env) setting
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, CTM_BENCH_URL (s3://bucket/prefix), and
# CTM_BENCH_ENDPOINT. Needs fusermount3, git, npm, rclone, and mount-s3 (MOUNT_S3 to override
# the path). Data and state live in target/bench; results are appended to bench/results/.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${BENCH_WORK:-$ROOT/target/bench}
RESULTS=${BENCH_RESULTS:-$ROOT/bench/results/v0-baseline.md}
LINUX_BRANCH=${LINUX_BRANCH:-linux}
MOUNT_S3=${MOUNT_S3:-$ROOT/target/bench/tools/mount-s3/bin/mount-s3}
NPM_APP=${NPM_APP:-https://github.com/nestjs/nest}
NPM_APP_REF=${NPM_APP_REF:-v11.2.6}

set -a
# shellcheck source=/dev/null
source "${BENCH_ENV:-$HOME/.config/continuum/bench.env}"
set +a
BUCKET=${CTM_BENCH_URL#s3://}
BUCKET=${BUCKET%%/*}
PREFIX=${CTM_BENCH_URL#s3://$BUCKET/}
STATE=$WORK/home
if [ -n "${BENCH_PREFIX:-}" ]; then
  CTM_BENCH_URL=s3://$BUCKET/$BENCH_PREFIX
  STATE=$WORK/home-$BENCH_PREFIX
fi

# Continuum's own state, isolated from the real home directory.
export XDG_CONFIG_HOME=$STATE/config XDG_DATA_HOME=$STATE/data
export XDG_CACHE_HOME=$STATE/cache XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
# rclone, configured from the same credentials, without a config file.
export RCLONE_CONFIG_R2_TYPE=s3 RCLONE_CONFIG_R2_PROVIDER=Cloudflare
export RCLONE_CONFIG_R2_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export RCLONE_CONFIG_R2_ENDPOINT=$CTM_BENCH_ENDPOINT RCLONE_CONFIG_R2_REGION=$AWS_REGION
# The token is scoped to one bucket, so rclone mustn't try to create it.
export RCLONE_S3_NO_CHECK_BUCKET=true

# CTM_BIN runs another build (for example an installed release) instead of this checkout.
CTM=${CTM_BIN:-$ROOT/target/release/ctm}
MNT=$WORK/mnt
mkdir -p "$WORK" "$MNT" "$(dirname "$RESULTS")"

log() { printf '\n== %s\n' "$*" >&2; }
secs() { awk "BEGIN { printf \"%.2f\", $2 - $1 }"; }
mib() { awk "BEGIN { printf \"%.1f MiB\", $1 / 1048576 }"; }
record() { printf '| %s | %s |\n' "$1" "$2" | tee -a "$RESULTS"; }

# A fresh chunk and metadata cache, so every read below is cold.
cold_cache() { rm -rf "$XDG_CACHE_HOME"; }
fetched() { "$CTM" cache stats --json | sed -E 's/.*"fetched_bytes":([0-9]+).*/\1/'; }
unmount_all() {
  "$CTM" unmount "$MNT" >/dev/null 2>&1 || fusermount3 -u -z "$MNT" 2>/dev/null || true
}
trap unmount_all EXIT

setup() {
  if [ -z "${CTM_BIN:-}" ]; then
    cargo build --release -q -p ctm --manifest-path "$ROOT/Cargo.toml"
  fi
  if [ ! -f "$XDG_CONFIG_HOME/continuum/config.toml" ]; then
    "$CTM" init "$CTM_BENCH_URL" --endpoint "$CTM_BENCH_ENDPOINT"
  fi
  if [ ! -f "$RESULTS" ]; then
    {
      echo "# $(basename "$RESULTS" .md)"
      echo
      echo "Run $(date -u +%Y-%m-%d) on $(uname -srm), $(nproc) cores, against Cloudflare R2 over a home connection (about 300 Mbit/s up, 560 Mbit/s down)."
      echo
      echo "| Benchmark | Result |"
      echo "|---|---|"
    } >"$RESULTS"
  fi
}

# 1. Fork time vs. repo size: fork is one GET and one PUT, whatever the size.
bench_fork() {
  for n in 1000 100000 1000000; do
    local src=$WORK/files-$n
    if [ ! -d "$src" ]; then
      log "creating $n files"
      python3 - "$src" "$n" <<'EOF'
import os, sys
root, n = sys.argv[1], int(sys.argv[2])
per_dir = 1000
for d in range((n + per_dir - 1) // per_dir):
    os.makedirs(f"{root}/d{d}", exist_ok=True)
    for f in range(min(per_dir, n - d * per_dir)):
        with open(f"{root}/d{d}/f{f}", "w") as fh:
            fh.write(f"{d}/{f}\n")
EOF
    fi
    if ! "$CTM" branch list | grep -qx "files-$n"; then
      log "importing $n files"
      local t0=$EPOCHREALTIME
      "$CTM" import "$src" --branch "files-$n" >/dev/null
      record "Import $n tiny files" "$(secs "$t0" "$EPOCHREALTIME") s"
    fi
    local best=999
    for i in 1 2 3 4 5; do
      local t0=$EPOCHREALTIME
      "$CTM" fork "files-$n" "fork-$n-$i-$RANDOM" >/dev/null
      best=$(awk "BEGIN { t = $EPOCHREALTIME - $t0; print (t < $best ? t : $best) }")
    done
    record "1. \`ctm fork\` of a $n-file repo (best of 5, whole CLI process)" \
      "$(awk "BEGIN { printf \"%.0f ms\", $best * 1000 }")"
  done
}

# Data for 2, 3, and 6: a 10 GiB random file, and a 20 GiB one that starts with it, so
# Continuum stores the shared half once.
data() {
  local src=$WORK/data
  if [ ! -f "$src/big.bin" ]; then
    log "generating 10 GiB + 20 GiB of random data"
    mkdir -p "$src"
    head -c 10G /dev/urandom >"$src/seq10.bin"
    { cat "$src/seq10.bin"; head -c 10G /dev/urandom; } >"$src/big.bin"
  fi
  if ! "$CTM" branch list | grep -qx data; then
    log "importing the data files"
    local t0=$EPOCHREALTIME
    local out
    out=$("$CTM" import "$src" --branch data)
    record "Import 30 GiB of files sharing 10 GiB" "$(secs "$t0" "$EPOCHREALTIME") s; ${out#*; }"
  fi
  if ! rclone lsf "r2:$BUCKET/$PREFIX/plain/" 2>/dev/null | grep -qx seq10.bin; then
    log "uploading seq10.bin as a plain object for rclone and mountpoint-s3"
    rclone copyto "$src/seq10.bin" "r2:$BUCKET/$PREFIX/plain/seq10.bin" --s3-upload-concurrency 16
  fi
}

# The branch seqread and randread use: `data`, or SEQ_BRANCH holding only seq10.bin.
SEQ_BRANCH=${SEQ_BRANCH:-data}
seq_branch() {
  data
  [ "$SEQ_BRANCH" = data ] && return
  if ! "$CTM" branch list | grep -qx "$SEQ_BRANCH"; then
    mkdir -p "$WORK/seq10"
    ln -f "$WORK/data/seq10.bin" "$WORK/seq10/seq10.bin"
    log "importing seq10.bin into $SEQ_BRANCH"
    local t0=$EPOCHREALTIME
    local out
    out=$("$CTM" import "$WORK/seq10" --branch "$SEQ_BRANCH")
    record "Import a 10 GiB file into $SEQ_BRANCH" "$(secs "$t0" "$EPOCHREALTIME") s; ${out#*; }"
  fi
}

# 2. Bytes downloaded for `head -c 1M` on a 20 GiB file.
bench_head() {
  data
  cold_cache
  "$CTM" mount data "$MNT" --read-only >/dev/null
  head -c 1M "$MNT/big.bin" >/dev/null
  sleep 2 # let readahead that already started finish, so it's counted
  record "2. Downloaded for \`head -c 1M\` of a 20 GiB file" "$(mib "$(fetched)")"
  unmount_all
}

cold_read() {
  local t0=$EPOCHREALTIME
  dd if="$1" of=/dev/null bs=1M status=none
  awk "BEGIN { printf \"%.0f MB/s (%.1f s)\", 10737.418 / ($EPOCHREALTIME - $t0), $EPOCHREALTIME - $t0 }"
}

# 3. Cold sequential read of 10 GiB: Continuum, rclone mount, mountpoint-s3
# (SEQREAD_TOOLS picks which).
bench_seqread() {
  seq_branch
  for tool in ${SEQREAD_TOOLS:-ctm rclone mount-s3}; do
    case $tool in
      ctm)
        cold_cache
        "$CTM" mount "$SEQ_BRANCH" "$MNT" --read-only >/dev/null
        record "3. Cold sequential read of 10 GiB: ctm ($("$CTM" --version), $SEQ_BRANCH)" \
          "$(cold_read "$MNT/seq10.bin"); downloaded $(mib "$(fetched)")"
        unmount_all
        ;;
      rclone)
        rclone mount "r2:$BUCKET/$PREFIX/plain" "$MNT" --read-only --daemon
        record "3. Cold sequential read of 10 GiB: rclone mount (defaults)" "$(cold_read "$MNT/seq10.bin")"
        fusermount3 -u "$MNT"
        ;;
      mount-s3)
        "$MOUNT_S3" "$BUCKET" "$MNT" --prefix "$PREFIX/plain/" --endpoint-url "$CTM_BENCH_ENDPOINT" \
          --region "$AWS_REGION" --read-only >/dev/null
        record "3. Cold sequential read of 10 GiB: mountpoint-s3" "$(cold_read "$MNT/seq10.bin")"
        fusermount3 -u "$MNT"
        ;;
    esac
  done
}

# R4. Cold random reads: 200 reads of 4 KiB at random offsets of the 10 GiB file, one at a time,
# with empty caches. Latency percentiles and bytes downloaded.
bench_randread() {
  seq_branch
  cold_cache
  "$CTM" mount "$SEQ_BRANCH" "$MNT" --read-only >/dev/null
  local lat
  lat=$(python3 - "$MNT/seq10.bin" <<'EOF'
import os, random, sys, time
fd = os.open(sys.argv[1], os.O_RDONLY)
size = os.fstat(fd).st_size
rng = random.Random(4)
times = []
for _ in range(200):
    off = rng.randrange(0, size - 4096) & ~4095
    t0 = time.perf_counter()
    assert len(os.pread(fd, 4096, off)) == 4096
    times.append(time.perf_counter() - t0)
times.sort()
p = lambda q: times[int(q * (len(times) - 1))] * 1000
print(f"p50 {p(0.5):.0f} ms, p90 {p(0.9):.0f} ms, p99 {p(0.99):.0f} ms")
EOF
)
  sleep 2 # let readahead that already started finish, so it's counted
  record "R4. Cold random 4 KiB reads of a 10 GiB file, 200 reads ($("$CTM" --version), $SEQ_BRANCH)" \
    "$lat; downloaded $(mib "$(fetched)")"
  unmount_all
}

# 6. Append 1 byte to the 20 GiB file and commit.
bench_append() {
  data
  cold_cache
  "$CTM" fork data "append-$RANDOM" >/dev/null
  local branch
  branch=$("$CTM" branch list | grep '^append-' | tail -1)
  "$CTM" mount "$branch" "$MNT" >/dev/null
  printf '!' >>"$MNT/big.bin"
  local t0=$EPOCHREALTIME
  local out
  out=$("$CTM" commit "$MNT" -m append)
  local took
  took=$(secs "$t0" "$EPOCHREALTIME")
  record "6. Append 1 byte to a 20 GiB file, then commit ($("$CTM" --version))" \
    "commit $took s; downloaded $(mib "$(fetched)"); ${out#*(}"
  unmount_all
}

# 4 and 5. git clone + npm ci inside a mount vs. the local disk, then commit node_modules.
bench_npm() {
  local branch=npm-$RANDOM
  mkdir -p "$WORK/empty"
  "$CTM" import "$WORK/empty" --branch "$branch" >/dev/null
  export npm_config_cache=$WORK/npm-cache npm_config_audit=false npm_config_fund=false
  local local_dir=$WORK/npm-local
  rm -rf "$local_dir"
  # Warm npm's cache first, so both timed runs measure the filesystem, not the registry.
  git clone --quiet --depth 1 --branch "$NPM_APP_REF" "$NPM_APP" "$local_dir"
  (cd "$local_dir" && npm ci --ignore-scripts --legacy-peer-deps --silent)
  rm -rf "$local_dir"

  local t0=$EPOCHREALTIME
  git clone --quiet --depth 1 --branch "$NPM_APP_REF" "$NPM_APP" "$local_dir"
  local clone_local
  clone_local=$(secs "$t0" "$EPOCHREALTIME")
  t0=$EPOCHREALTIME
  (cd "$local_dir" && npm ci --ignore-scripts --legacy-peer-deps --silent)
  local npm_local
  npm_local=$(secs "$t0" "$EPOCHREALTIME")

  "$CTM" mount "$branch" "$MNT" >/dev/null
  t0=$EPOCHREALTIME
  git clone --quiet --depth 1 --branch "$NPM_APP_REF" "$NPM_APP" "$MNT/app"
  local clone_mnt
  clone_mnt=$(secs "$t0" "$EPOCHREALTIME")
  t0=$EPOCHREALTIME
  (cd "$MNT/app" && npm ci --ignore-scripts --legacy-peer-deps --silent)
  local npm_mnt
  npm_mnt=$(secs "$t0" "$EPOCHREALTIME")
  local files
  files=$(find "$MNT/app/node_modules" -type f | wc -l)
  record "4. \`git clone\` of ${NPM_APP##*/} $NPM_APP_REF: mount vs. local btrfs" "$clone_mnt s vs. $clone_local s"
  record "4. \`npm ci\` ($files files in node_modules): mount vs. local btrfs" "$npm_mnt s vs. $npm_local s"

  t0=$EPOCHREALTIME
  local out
  out=$("$CTM" commit "$MNT" -m "npm ci")
  record "5. Commit the clone + node_modules ($("$CTM" --version))" "$(secs "$t0" "$EPOCHREALTIME") s; ${out#*(}"
  if has_sync; then
    t0=$EPOCHREALTIME
    "$CTM" sync "$MNT" >/dev/null
    record "5. ... then \`ctm sync\` until it's pushed" "$(secs "$t0" "$EPOCHREALTIME") s"
  fi
  unmount_all
}

# Whether this build commits locally and has `ctm sync` (R2 on).
has_sync() { "$CTM" sync --help >/dev/null 2>&1; }

# R2. Writes while a large commit is pushed: write 1 GiB of fresh random data (nothing in the
# bucket to dedup against), commit, and 1 s into the commit write 256 MiB more (fsync'd). Before
# R2 a commit blocks writes until its upload is done.
bench_writepush() {
  local branch=writepush-$RANDOM
  mkdir -p "$WORK/empty"
  "$CTM" import "$WORK/empty" --branch "$branch" >/dev/null
  "$CTM" mount "$branch" "$MNT" >/dev/null
  head -c 1G /dev/urandom >"$MNT/a.bin"
  local t0=$EPOCHREALTIME
  "$CTM" commit "$MNT" >/dev/null &
  local committing=$!
  sleep 1
  local t1=$EPOCHREALTIME
  head -c 256M /dev/urandom | dd of="$MNT/b.bin" bs=1M iflag=fullblock conv=fsync status=none
  local t2=$EPOCHREALTIME
  wait "$committing"
  local t3=$EPOCHREALTIME
  local pushed=""
  if has_sync; then
    "$CTM" sync "$MNT" >/dev/null
    pushed="; pushed $(secs "$t0" "$EPOCHREALTIME") s after the commit started"
  fi
  record "R2. Commit 1 GiB, and write 256 MiB 1 s into it ($("$CTM" --version))" \
    "commit returned after $(secs "$t0" "$t3") s; the write took $(secs "$t1" "$t2") s$pushed"
  unmount_all
}

linux_branch() {
  local src=$WORK/linux
  if [ ! -d "$src" ]; then
    log "cloning the Linux kernel (shallow)"
    git clone --quiet --depth 1 https://github.com/torvalds/linux "$src"
  fi
  if ! "$CTM" branch list | grep -qx "$LINUX_BRANCH"; then
    log "importing the kernel tree"
    local t0=$EPOCHREALTIME
    local out
    out=$("$CTM" import "$src" --branch "$LINUX_BRANCH")
    record "Import the Linux kernel tree, $(find "$src" -type f | wc -l) files; ${out#*; }" \
      "$(secs "$t0" "$EPOCHREALTIME") s"
  fi
}

# 7. Cold find over the Linux kernel source, with an empty metadata cache.
bench_linux() {
  linux_branch
  cold_cache
  "$CTM" mount "$LINUX_BRANCH" "$MNT" --read-only >/dev/null
  local t0=$EPOCHREALTIME
  local n
  n=$(find "$MNT" | wc -l)
  record "7. Cold \`find\` over the kernel tree ($n entries)" "$(secs "$t0" "$EPOCHREALTIME") s"
  unmount_all
}

# 7. Cold git status over the Linux kernel source, with empty caches.
bench_linuxgit() {
  cold_cache
  "$CTM" mount "$LINUX_BRANCH" "$MNT" --read-only >/dev/null
  local t0=$EPOCHREALTIME
  git -C "$MNT" status --porcelain >/dev/null 2>&1 || true
  record "7. Cold \`git status\` over the kernel tree" \
    "$(secs "$t0" "$EPOCHREALTIME") s; downloaded $(mib "$(fetched)")"
  unmount_all
}

# R3. A fresh machine (empty cache and index mirror): mount the kernel tree and read the first
# byte of a file six directories deep. Three runs.
TTFB_FILE=drivers/gpu/drm/amd/display/dc/core/dc.c
bench_ttfb() {
  linux_branch
  local runs=()
  for i in 1 2 3; do
    cold_cache
    local t0=$EPOCHREALTIME
    "$CTM" mount "$LINUX_BRANCH" "$MNT" --read-only >/dev/null
    local t1=$EPOCHREALTIME
    head -c 1 "$MNT/$TTFB_FILE" >/dev/null
    local t2=$EPOCHREALTIME
    runs+=("mount $(secs "$t0" "$t1") s + first byte $(secs "$t1" "$t2") s")
    unmount_all
  done
  record "Fresh machine: mount the kernel tree, read 1 byte of $TTFB_FILE ($("$CTM" --version))" \
    "$(IFS=';'; echo "${runs[*]}" | sed 's/;/; /g')"
}

# History for ttfb: HISTORY pushes (default 1000), each a one-file import into its own branch,
# so the repo has that many index segments, as it would after that many commits.
bench_history() {
  linux_branch
  local have dir=$STATE/history-src
  have=$("$CTM" branch list | grep -c '^hist-' || true)
  log "pushing $((${HISTORY:-1000} - have)) times"
  mkdir -p "$dir"
  for ((i = have + 1; i <= ${HISTORY:-1000}; i++)); do
    echo "$i" >"$dir/n"
    "$CTM" import "$dir" --branch "hist-$i" >/dev/null
  done
  record "History: $("$CTM" branch list | grep -c '^hist-') pushes" "done"
}

# R6. Storage after N edit cycles: overwrite a 100 MiB file with new data CYCLES times (default
# 10), each version auto-committed and pushed, then run `ctm gc` twice (the second deletes what
# the first listed; retention and the grace period set to zero for the benchmark). Run it with
# BENCH_PREFIX set to a prefix of its own: GC collects the whole repo.
bench_gc() {
  [ -n "${BENCH_PREFIX:-}" ] || { echo "run bench_gc with BENCH_PREFIX set" >&2; exit 2; }
  local cfg=$XDG_CONFIG_HOME/continuum/config.toml
  sed -i -e 's/^quiet_secs = .*/quiet_secs = 1/' -e 's/^auto_days = .*/auto_days = 0/' "$cfg"
  local branch=cycles-$RANDOM
  mkdir -p "$WORK/empty"
  "$CTM" import "$WORK/empty" --branch "$branch" >/dev/null
  "$CTM" mount "$branch" "$MNT" >/dev/null
  local cycles=${CYCLES:-10}
  for ((i = 1; i <= cycles; i++)); do
    head -c 100M /dev/urandom >"$MNT/model.bin"
    until "$CTM" status "$MNT" | grep -q "Changes: 0"; do sleep 0.5; done
    "$CTM" sync "$MNT" >/dev/null
  done
  unmount_all
  local size
  size() { rclone size "r2:$BUCKET/$BENCH_PREFIX" --json | sed -E 's/.*"bytes":([0-9]+).*/\1/'; }
  local before
  before=$(size)
  local t0=$EPOCHREALTIME
  "$CTM" gc --grace-secs 0 >/dev/null
  local t1=$EPOCHREALTIME
  "$CTM" gc --grace-secs 0 >/dev/null
  local t2=$EPOCHREALTIME
  record "R6. Bucket after $cycles edit cycles of a 100 MiB file, then two \`ctm gc\` runs ($("$CTM" --version))" \
    "$(mib "$before") → $(mib "$(size)"); gc runs $(secs "$t0" "$t1") s and $(secs "$t1" "$t2") s"
}

# R9. `git status` on a fresh mount of a repo whose index was last refreshed in a mount.
GIT_REPO=${GIT_REPO:-https://github.com/git/git}
bench_gitremount() {
  local branch
  branch="gitremount-$("$CTM" --version | tr -cd '0-9')-$RANDOM"
  mkdir -p "$WORK/empty"
  "$CTM" import "$WORK/empty" --branch "$branch" >/dev/null
  "$CTM" mount "$branch" "$MNT" >/dev/null
  git clone --quiet --depth 1 "$GIT_REPO" "$MNT/repo"
  git -C "$MNT/repo" status --porcelain >/dev/null
  local files
  files=$(git -C "$MNT/repo" ls-files | wc -l)
  "$CTM" unmount "$MNT" >/dev/null
  cold_cache
  "$CTM" mount "$branch" "$MNT" >/dev/null
  local t0=$EPOCHREALTIME
  git -C "$MNT/repo" status --porcelain >/dev/null
  local took
  took=$(secs "$t0" "$EPOCHREALTIME")
  printf '| %s (%s files) | %s | %s s | %s |\n' "${GIT_REPO#https://github.com/}" "$files" \
    "$("$CTM" --version)" "$took" "$(mib "$(fetched)")" | tee -a "$ROOT/bench/results/r9-stable-inodes.md"
  unmount_all
}

setup
case ${1:-all} in
  all) bench_fork; bench_head; bench_seqread; bench_append; bench_npm; bench_linux; bench_linuxgit ;;
  fork | head | seqread | append | npm | linux | linuxgit | gitremount | history | ttfb | randread | writepush | gc) "bench_$1" ;;
  *) echo "usage: $0 [all|fork|head|seqread|randread|append|npm|linux|linuxgit|gitremount|history|ttfb|writepush|gc]" >&2; exit 2 ;;
esac
