#!/usr/bin/env bash
# R11: Continuum against DVC and Git LFS, on the same bucket, the same machine, the same data.
#
#   bench/r11.sh [ctm|dvc|lfs]...      (default: all three, one after the other)
#
# Every "machine" is a fresh container of bench/r11/Dockerfile (the released ctm, DVC, Git LFS),
# so its network counters cover exactly what that tool sent and received, TLS included. Git LFS
# goes through rudolfs, a self-hosted LFS server on the same bucket, running in the same network
# namespace (its traffic to the bucket is counted; git-lfs to rudolfs is loopback). The git
# repos DVC and Git LFS need are bare repos on local disk: hosting code isn't measured.
#
# The data: a 10 GiB model file and 4,000 images of 512 KiB (12 GiB), random so nothing
# compresses. Steps, per tool:
#   1. machine A adds the data and pushes it;
#   2. a fresh machine reads one image (three runs, each a new machine);
#   3. a fresh machine reads everything (16 parallel readers for ctm);
#   4. that machine re-reads the images from local disk, page cache dropped;
#   5. it overwrites 1 MiB in the middle of the model file, commits, and pushes;
#   6. it makes 10 dataset versions (40 images replaced and 40 added in each), then switches back
#      to the first and reads the 80 files that differ.
# Bucket storage is measured after 1, 5, and 6. Needs docker (with --privileged, to drop the
# page cache), rclone, and the env file baseline.sh uses. Each tool gets its own bucket prefix
# (r11/<tool>), emptied at the start of its run.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${BENCH_WORK:-$ROOT/target/bench}/r11
RESULTS=${BENCH_RESULTS:-$ROOT/bench/results/r11-dvc-lfs.md}
IMAGE=ctm-r11
# R11_SMALL=1: a 64 MiB model and 500 images, in a work directory of its own, to try the harness.
if [ -n "${R11_SMALL:-}" ]; then
  WORK=$WORK-small MODEL_MIB=64 IMAGES=500
fi
MODEL_MIB=${MODEL_MIB:-10240} IMAGES=${IMAGES:-4000}
RUDOLFS=jasonwhite0/rudolfs:0.3.8

set -a
# shellcheck source=/dev/null
source "${BENCH_ENV:-$HOME/.config/continuum/bench.env}"
set +a
BUCKET=${CTM_BENCH_URL#s3://}
BUCKET=${BUCKET%%/*}
export RCLONE_CONFIG_R2_TYPE=s3 RCLONE_CONFIG_R2_PROVIDER=Cloudflare
export RCLONE_CONFIG_R2_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export RCLONE_CONFIG_R2_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export RCLONE_CONFIG_R2_ENDPOINT=$CTM_BENCH_ENDPOINT RCLONE_CONFIG_R2_REGION=$AWS_REGION
export RCLONE_S3_NO_CHECK_BUCKET=true

log() { printf '\n== %s\n' "$*" >&2; }
secs() { awk "BEGIN { printf \"%.1f\", $2 - $1 }"; }
size() { awk "BEGIN { b = $1; if (b >= 1073741824) printf \"%.2f GiB\", b / 1073741824; else printf \"%.1f MiB\", b / 1048576 }"; }
record() { printf '| %s | %s | %s |\n' "$1" "$2" "$3" | tee -a "$RESULTS"; }
stored() { rclone size "r2:$BUCKET/r11/$1" --json 2>/dev/null | sed -E 's/.*"bytes":([0-9]+).*/\1/'; }
drop_caches() { docker run --rm --privileged alpine sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'; }

# A fresh machine named $1, with the bucket credentials, the data read-only at /data, and the
# work directory at /work; Git LFS machines get rudolfs too.
machine() {
  local name=r11-$1
  docker rm -f "$name" "$name-lfs" >/dev/null 2>&1 || true
  docker run -d --init --name "$name" --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
    -e AWS_REGION -e AWS_DEFAULT_REGION="$AWS_REGION" -e BUCKET="$BUCKET" \
    -e ENDPOINT="$CTM_BENCH_ENDPOINT" -e XDG_CONFIG_HOME="/work/state-$1/config" \
    -e XDG_DATA_HOME="/work/state-$1/data" -e XDG_CACHE_HOME="/work/state-$1/cache" \
    -v "$WORK/data:/data:ro" -v "$WORK/$TOOL:/work" "$IMAGE" sleep infinity >/dev/null
  if [ "$TOOL" = lfs ]; then
    docker run -d --name "$name-lfs" --network "container:$name" -e AWS_ACCESS_KEY_ID \
      -e AWS_SECRET_ACCESS_KEY -e AWS_DEFAULT_REGION="$AWS_REGION" \
      -e AWS_S3_ENDPOINT="$CTM_BENCH_ENDPOINT" "$RUDOLFS" --host 127.0.0.1:8080 \
      s3 --bucket "$BUCKET" --prefix r11/lfs >/dev/null
    until docker exec "$name" bash -c 'echo >/dev/tcp/127.0.0.1/8080' 2>/dev/null; do sleep 0.2; done
  fi
  CUR=$name
}
done_with() { docker rm -f "r11-$1" "r11-$1-lfs" >/dev/null 2>&1 || true; }
on() { docker exec -w /work "$CUR" bash -ec "$1"; }
counter() { docker exec "$CUR" cat "/sys/class/net/eth0/statistics/$1_bytes"; }

# Runs $1 on the current machine; sets TOOK (seconds), RX and TX (bytes on the wire).
measure() {
  local rx0 tx0 t0
  rx0=$(counter rx) tx0=$(counter tx) t0=$EPOCHREALTIME
  on "$1"
  TOOK=$(secs "$t0" "$EPOCHREALTIME")
  RX=$(($(counter rx) - rx0)) TX=$(($(counter tx) - tx0))
}

# The data, generated once.
data() {
  local d=$WORK/data/v0
  [ -f "$WORK/data/.v0-done" ] && return
  log "generating 10 GiB + 4,000 images of 512 KiB"
  mkdir -p "$d"
  if [ "$MODEL_MIB" = 10240 ] && [ -f "$ROOT/target/bench/data/seq10.bin" ]; then
    ln -f "$ROOT/target/bench/data/seq10.bin" "$d/model.bin"
  else
    head -c "${MODEL_MIB}M" /dev/urandom >"$d/model.bin"
  fi
  python3 - "$d/images" "$IMAGES" <<'EOF'
import os, sys
root = sys.argv[1]
for i in range(int(sys.argv[2])):
    os.makedirs(f"{root}/d{i // 100:02}", exist_ok=True)
    with open(f"{root}/d{i // 100:02}/i{i:04}.jpg", "wb") as f:
        f.write(os.urandom(512 * 1024))
EOF
  touch "$WORK/data/.v0-done"
}

# Shell snippets run on the machines, per tool. $W is the workspace's data directory.
ONE=images/d04/i0471.jpg
EDIT='python3 - "$W/model.bin" <<EOF
import os, sys
with open(sys.argv[1], "r+b") as f:
    f.seek(os.fstat(f.fileno()).st_size // 2)
    f.write(os.urandom(1 << 20))
EOF'
# Version $V: replace images v*40..v*40+39 and add 40 new ones.
VERSION='python3 - "$W/images" "$V" <<EOF
import os, sys
root, v = sys.argv[1], int(sys.argv[2])
for i in range(v * 40, v * 40 + 40):
    with open(f"{root}/d{i // 100:02}/i{i:04}.jpg", "wb") as f:
        f.write(os.urandom(512 * 1024))
    with open(f"{root}/new/v{v:02}-{i:04}.jpg", "wb") as f:
        f.write(os.urandom(512 * 1024))
EOF'
# The 400 images the 10 versions replaced (40..439).
TOUCHED='for i in $(seq 40 439); do cat "$(printf "%s/images/d%02d/i%04d.jpg" "$W" $((i / 100)) "$i")"; done >/dev/null'
NL=$'\n'
READ_ALL='find "$W" -type f -not -path "*/.*" -print0 | xargs -0 -n 16 -P 16 cat >/dev/null'
READ_IMAGES='find "$W/images" -type f -print0 | xargs -0 -n 16 -P 16 cat >/dev/null'

case_ctm() {
  local url='s3://$BUCKET/r11/ctm' join='ctm init s3://$BUCKET/r11/ctm --endpoint $ENDPOINT >/dev/null'
  CLONE_ONE="$join; mkdir -p /mnt/c; ctm mount main /mnt/c --read-only >/dev/null; W=/mnt/c; cat \$W/$ONE >/dev/null"
  CLONE_ALL="$join; mkdir -p /mnt/c; ctm mount main /mnt/c >/dev/null; W=/mnt/c; $READ_ALL"
  ADD="ctm init $url --endpoint \$ENDPOINT >/dev/null; ctm import /data/v0 --branch main"
  W=/mnt/c
  COMMIT='ctm commit /mnt/c -m "$MSG" >/dev/null && ctm sync /mnt/c >/dev/null'
  TAG='ctm snapshot create first --from main >/dev/null'
  SWITCH='ctm unmount /mnt/c >/dev/null; ctm mount snap/first /mnt/c --read-only >/dev/null'
}
case_dvc() {
  ADD='cd /work/a && dvc add -q data && git add data.dvc .gitignore && git commit -qm v0 && dvc push -q && git push -q origin main'
  PREP='git init -q --bare /work/remote.git && mkdir -p /work/a/data && cd /work/a && git init -q &&
    git remote add origin /work/remote.git && dvc init -q && dvc remote add -q -d r2 s3://$BUCKET/r11/dvc &&
    dvc remote modify r2 endpointurl $ENDPOINT && git add .dvc .dvcignore && git commit -qm init &&
    cp -r --reflink=auto /data/v0/. /work/a/data/'
  CLONE_ONE="rm -rf /work/b && git clone -q /work/remote.git /work/b && cd /work/b && dvc pull -q data/$ONE && cat data/$ONE >/dev/null"
  CLONE_ALL="rm -rf /work/b && git clone -q /work/remote.git /work/b && cd /work/b && dvc pull -q"
  W=/work/b/data
  COMMIT='cd /work/b && dvc add -q data && git add data.dvc && git commit -qm "$MSG" && dvc push -q && git push -q origin main'
  TAG='cd /work/b && git tag first'
  SWITCH='cd /work/b && git checkout -q first && dvc checkout -q'
}
case_lfs() {
  local lfs='git config lfs.url http://127.0.0.1:8080/api/r11/data && git config lfs.concurrenttransfers 16'
  PREP="git init -q --bare /work/remote.git && mkdir -p /work/a && cd /work/a && git init -q &&
    git remote add origin /work/remote.git && $lfs &&
    git lfs track 'data/**' >/dev/null && git add .gitattributes && git commit -qm init &&
    mkdir -p data && cp -r --reflink=auto /data/v0/. data/"
  ADD='cd /work/a && git add data && git commit -qm v0 && git push -q origin main'
  CLONE_ONE="rm -rf /work/b && GIT_LFS_SKIP_SMUDGE=1 git clone -q /work/remote.git /work/b && cd /work/b &&
    $lfs && git lfs pull --include data/$ONE && cat data/$ONE >/dev/null"
  CLONE_ALL="rm -rf /work/b && GIT_LFS_SKIP_SMUDGE=1 git clone -q /work/remote.git /work/b && cd /work/b &&
    $lfs && git lfs pull"
  W=/work/b/data
  COMMIT='cd /work/b && git add -A data && git commit -qm "$MSG" && git push -q origin main'
  TAG='cd /work/b && git tag first'
  SWITCH='cd /work/b && git checkout -q first'
}

run() {
  TOOL=$1
  "case_$TOOL"
  log "$TOOL: resetting r11/$TOOL and $WORK/$TOOL"
  rclone purge "r2:$BUCKET/r11/$TOOL" 2>/dev/null || true
  docker run --rm -v "$WORK:/w" alpine rm -rf "/w/$TOOL"
  mkdir -p "$WORK/$TOOL"

  log "$TOOL 1: add and push"
  machine a
  [ -n "${PREP:-}" ] && on "$PREP"
  measure "$ADD"
  record "$TOOL" "1. Add 12 GiB and push" "$TOOK s; sent $(size "$TX"); bucket $(size "$(stored "$TOOL")")"
  done_with a

  log "$TOOL 2: fresh machine, one image"
  local runs=()
  for i in 1 2 3; do
    docker run --rm -v "$WORK/$TOOL:/w" alpine rm -rf /w/b /w/state-b
    machine b
    measure "$CLONE_ONE"
    runs+=("$TOOK s, $(size "$RX")")
    done_with b
  done
  record "$TOOL" "2. Fresh machine: read one 512 KiB image (3 runs: time, received)" "$(IFS=';'; echo "${runs[*]}" | sed 's/;/; /g')"

  log "$TOOL 3: fresh machine, everything"
  docker run --rm -v "$WORK/$TOOL:/w" alpine rm -rf /w/b /w/state-b
  machine b
  measure "$CLONE_ALL"
  record "$TOOL" "3. Fresh machine: read all 12 GiB" "$TOOK s; received $(size "$RX")"

  log "$TOOL 4: local re-read"
  drop_caches
  measure "W=$W; $READ_IMAGES"
  record "$TOOL" "4. Then re-read the 2 GiB of images (page cache dropped)" "$TOOK s; received $(size "$RX")"

  log "$TOOL 5: small edit"
  local before
  before=$(stored "$TOOL")
  measure "W=$W${NL}$EDIT${NL}MSG=edit${NL}$COMMIT"
  record "$TOOL" "5. Overwrite 1 MiB of the 10 GiB file; commit and push" \
    "$TOOK s; sent $(size "$TX"); bucket +$(size $(($(stored "$TOOL") - before)))"

  log "$TOOL 6: 10 versions"
  on "$TAG"
  on "mkdir -p $W/images/new"
  before=$(stored "$TOOL")
  local times=() tx=0
  for v in $(seq 1 10); do
    measure "W=$W${NL}V=$v${NL}$VERSION${NL}MSG=v$v${NL}$COMMIT"
    times+=("$TOOK")
    tx=$((tx + TX))
  done
  record "$TOOL" "6. 10 versions, 80 images changed in each: commit and push (per version)" \
    "$(printf '%s s, ' "${times[@]}" | sed 's/, $//'); sent $(size "$tx") in all; bucket +$(size $(($(stored "$TOOL") - before)))"
  measure "$SWITCH${NL}W=$W${NL}$TOUCHED"
  record "$TOOL" "6. Then switch back to before the 10 versions and read the 400 images they touched" \
    "$TOOK s; received $(size "$RX")"
  record "$TOOL" "Bucket at the end" "$(size "$(stored "$TOOL")")"
  on "ctm unmount /mnt/c >/dev/null 2>&1 || true"
  done_with b
  docker run --rm -v "$WORK:/w" alpine rm -rf "/w/$TOOL"
}

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker build -q -t "$IMAGE" "$ROOT/bench/r11"
mkdir -p "$WORK"
data
if [ ! -f "$RESULTS" ]; then
  {
    echo "# R11: Continuum, DVC, and Git LFS"
    echo
    echo "Run $(date -u +%Y-%m-%d) on $(uname -srm), $(nproc) cores, btrfs, against Cloudflare R2 over a home connection (about 300 Mbit/s up, 560 Mbit/s down)."
    echo "Versions: $(docker run --rm "$IMAGE" bash -c 'echo "$(ctm --version), DVC $(dvc --version), $(git lfs version | cut -d" " -f1), $(git --version)"'), rudolfs ${RUDOLFS##*:}."
    echo
    echo "| Tool | Step | Result |"
    echo "|---|---|---|"
  } >"$RESULTS"
fi
for t in "${@:-ctm dvc lfs}"; do
  for tool in $t; do run "$tool"; done
done
