# R11: Continuum, DVC, and Git LFS

Run 2026-10-21 on Linux 7.2.9-2-cachyos x86_64, 12 cores, btrfs, against Cloudflare R2 over a home connection (about 300 Mbit/s up, 560 Mbit/s down).
Versions: ctm 0.5.0, DVC 3.67.1, git-lfs/3.6.1, git version 2.47.3, rudolfs 0.3.8.

| Tool | Step | Result |
|---|---|---|
| ctm | 1. Add 12 GiB and push | 446.9 s; sent 11.99 GiB; bucket 11.96 GiB |
| ctm | 2. Fresh machine: read one 512 KiB image (3 runs: time, received) | 2.3 s, 1.4 MiB; 1.8 s, 1.4 MiB; 1.6 s, 1.5 MiB |
| ctm | 3. Fresh machine: read all 12 GiB | 186.5 s; received 12.02 GiB |
| ctm | 4. Then re-read the 2 GiB of images (page cache dropped) | 5.2 s; received 0.0 MiB |
| ctm | 5. Overwrite 1 MiB of the 10 GiB file; commit and push | 1.8 s; sent 3.4 MiB; bucket +3.4 MiB |
| ctm | 6. 10 versions, 80 images changed in each: commit and push (per version) | 7.0 s, 4.6 s, 7.4 s, 7.1 s, 10.0 s, 3.7 s, 7.0 s, 7.0 s, 2.0 s, 7.3 s; sent 402.0 MiB in all; bucket +400.5 MiB |
| ctm | 6. Then switch back to before the 10 versions and read the 400 images they touched | 2.1 s; received 0.0 MiB |
| ctm | Bucket at the end | 12.35 GiB |
| dvc | 1. Add 12 GiB and push | 623.6 s; sent 12.00 GiB; bucket 11.95 GiB |
| dvc | 2. Fresh machine: read one 512 KiB image (3 runs: time, received) | 2.6 s, 2.1 MiB; 2.2 s, 2.1 MiB; 2.5 s, 2.1 MiB |
| dvc | 3. Fresh machine: read all 12 GiB | 238.1 s; received 12.00 GiB |
| dvc | 4. Then re-read the 2 GiB of images (page cache dropped) | 5.2 s; received 0.0 MiB |
| dvc | 5. Overwrite 1 MiB of the 10 GiB file; commit and push | 139.8 s; sent 10.09 GiB; bucket +10.00 GiB |
| dvc | 6. 10 versions, 80 images changed in each: commit and push (per version) | 6.8 s, 6.9 s, 7.4 s, 6.7 s, 8.4 s, 7.6 s, 9.8 s, 7.1 s, 6.8 s, 6.8 s; sent 409.2 MiB in all; bucket +403.2 MiB |
| dvc | 6. Then switch back to before the 10 versions and read the 400 images they touched | 1.7 s; received 0.0 MiB |
| dvc | Bucket at the end | 22.35 GiB |
| lfs | 1. Add 12 GiB and push | 654.8 s; sent 12.01 GiB; bucket 11.95 GiB |
| lfs | 2. Fresh machine: read one 512 KiB image (3 runs: time, received) | 0.9 s, 0.5 MiB; 0.8 s, 0.5 MiB; 0.8 s, 0.5 MiB |
| lfs | 3. Fresh machine: read all 12 GiB | 440.9 s; received 12.00 GiB |
| lfs | 4. Then re-read the 2 GiB of images (page cache dropped) | 5.4 s; received 0.0 MiB |
| lfs | 5. Overwrite 1 MiB of the 10 GiB file; commit and push | 472.1 s; sent 10.03 GiB; bucket +10.00 GiB |
| lfs | 6. 10 versions, 80 images changed in each: commit and push (per version) | 3.8 s, 3.4 s, 3.5 s, 3.4 s, 3.4 s, 6.2 s, 7.3 s, 3.4 s, 3.7 s, 3.3 s; sent 406.0 MiB in all; bucket +400.0 MiB |
| lfs | 6. Then switch back to before the 10 versions and read the 400 images they touched | 1.6 s; received 0.0 MiB |
| lfs | Bucket at the end | 22.34 GiB |

`bench/r11.sh`. Each tool got its own prefix in the same bucket and fresh containers as machines; bytes are what crossed the container's network interface (TLS and requests included), so they're comparable across tools. Defaults throughout, except Git LFS at 16 concurrent transfers (its default is 8). Git LFS went through rudolfs, a self-hosted LFS server on the bucket. One run each, except step 2 (three).

## Summary

| | ctm | DVC | Git LFS |
|---|---|---|---|
| Add 12 GiB and push | **447 s** | 624 s | 655 s |
| Fresh machine, read one image | 1.6–2.3 s, 1.4 MiB | 2.2–2.6 s, 2.1 MiB | **0.8–0.9 s, 0.5 MiB** |
| Fresh machine, read all 12 GiB | **187 s** | 238 s | 441 s |
| Re-read 2 GiB of images locally | 5.2 s | 5.2 s | 5.4 s |
| Edit 1 MiB of a 10 GiB file, push | **1.8 s, 3.4 MiB sent** | 140 s, 10.09 GiB | 472 s, 10.03 GiB |
| New dataset version (80 images), median | 7.0 s | 7.0 s | **3.4 s** |
| Switch back 10 versions, read what changed | 2.1 s | 1.7 s | 1.6 s |
| Bucket at the end | **12.35 GiB** | 22.35 GiB | 22.34 GiB |

## Notes

- **The small edit is where the designs differ.** DVC and Git LFS store whole files, so changing 1 MiB of a 10 GiB file uploads and stores another 10 GiB; Continuum re-chunks around the change and uploads 3.4 MiB, about 3,000× less, in 1.8 s instead of 140 s (DVC) or 472 s (Git LFS). That edit alone is why the bucket ends at 12.35 GiB instead of 22.35 GiB. Expected from the design, but now measured.
- **Whole-file changes cost the same everywhere.** The 10 dataset versions replaced or added whole images: all three stored the same 400 MiB. Git LFS committed each version fastest (3.4 s median against 7.0 s); Continuum's times varied more (2.0–10.0 s).
- **Git LFS wins the single small read.** `git lfs pull --include` of one file took 0.8 s and 0.5 MiB; Continuum needs a mount and its metadata first (1.6–2.3 s, 1.4 MiB), DVC its directory manifest (2.2–2.6 s).
- **Reading everything:** Continuum with 16 parallel readers took 187 s (about 66 MiB/s, near the connection's limit), DVC's pull 238 s, Git LFS's 441 s, through one server process.
- **Once local, all three are the same.** Re-reading the images from a warm Continuum cache through FUSE took 5.2 s, as from local disk for DVC and Git LFS.
- **The initial push:** Continuum 447 s, about 28 MiB/s on a link of about 37 MB/s; DVC and Git LFS took 40–47% longer.
- **Not measured:** Xet (Hugging Face), which chunks like Continuum and would likely match it on the small edit; Git LFS through GitHub's servers.
