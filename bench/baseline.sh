#!/usr/bin/env bash
# v0 baseline benchmarks: the "before" numbers for the roadmap.
#
#   bench/baseline.sh [all|fork|head|seqread|append|npm|linux|linuxgit|gitremount]
#
# BENCH_RESULTS appends to another results file (for a roadmap item's "after" run), and
# LINUX_BRANCH imports the kernel into another branch (so a new format is measured from scratch).
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

# Continuum's own state, isolated from the real home directory.
export XDG_CONFIG_HOME=$WORK/home/config XDG_DATA_HOME=$WORK/home/data
export XDG_CACHE_HOME=$WORK/home/cache XDG_RUNTIME_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}
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
  data
  for tool in ${SEQREAD_TOOLS:-ctm rclone mount-s3}; do
    case $tool in
      ctm)
        cold_cache
        "$CTM" mount data "$MNT" --read-only >/dev/null
        record "3. Cold sequential read of 10 GiB: ctm" "$(cold_read "$MNT/seq10.bin")"
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
  record "6. Append 1 byte to a 20 GiB file, then commit" \
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
  record "5. Commit the clone + node_modules" "$(secs "$t0" "$EPOCHREALTIME") s; ${out#*(}"
  unmount_all
}

# 7. Cold find over the Linux kernel source, with an empty metadata cache.
bench_linux() {
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
  fork | head | seqread | append | npm | linux | linuxgit | gitremount) "bench_$1" ;;
  *) echo "usage: $0 [all|fork|head|seqread|append|npm|linux|linuxgit|gitremount]" >&2; exit 2 ;;
esac
