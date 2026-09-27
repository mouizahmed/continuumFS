# R9: stable inode numbers

`git status` on a fresh mount of a repo whose git index was last refreshed in a Continuum mount
(`bench/baseline.sh gitremount`): clone into a read-write mount, run `git status`, unmount,
clear the cache, mount again, and time `git status`. Against Cloudflare R2, same setup as the
[v0 baseline](v0-baseline.md).

| Repo | Build | `git status` | Downloaded |
|---|---|---|---|
| git/git (4,852 files) | v0.1.0 (before) | 241.8 s | 50.0 MiB |
| git/git (4,852 files) | 0.2.0-dev with R9 (after) | 4.7 s | 2.7 MiB |

Before R9, a fresh mount gave every file a new inode number, so git treated all of them as
possibly changed and re-read the whole tree. With R9 the inode numbers are file IDs stored in
the repo, git's index stays valid, and `git status` reads only git's own metadata.
