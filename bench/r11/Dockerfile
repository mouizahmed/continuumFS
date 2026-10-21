# The R11 benchmark client: one "machine" with ctm, DVC, and Git LFS. Each run gets a fresh
# container, so its network counters cover exactly that run.
FROM python:3.13-slim-trixie
RUN apt-get update && apt-get install -y --no-install-recommends git git-lfs fuse3 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "dvc[s3]==3.67.1"
ARG CTM_VERSION=0.5.0
RUN arch=$(uname -m) \
    && curl -fsSL https://github.com/mouizahmed/continuumFS/releases/download/v$CTM_VERSION/ctm-$CTM_VERSION-$arch-unknown-linux-musl.tar.gz \
    | tar -xz --strip-components=1 -C /usr/local/bin ctm-$CTM_VERSION-$arch-unknown-linux-musl/ctm
RUN git config --global user.name bench && git config --global user.email bench@example.com \
    && git lfs install --system --skip-repo \
    && git config --global init.defaultBranch main && git config --global safe.directory '*'
