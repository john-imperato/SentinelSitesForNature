#!/usr/bin/env python3
"""
Simple ingest prep for a single field import.

Usage (example):
  python ingest.py \
    --input "/Users/johnimperato/Desktop/FieldImport_01_20250905" \
    --reserve R034 --site S005 --deployment 20250905 \
    --staging "/Users/johnimperato/Desktop/staging" \
    --sdsC-root "/expanse/projects/ucnrs-ssn/raw" \
    --mode symlink

Modes:
  symlink  (default): make a staged tree using symlinks to save space/time
  copy                actually copy bytes (uses more space/time)
  plan                make directories + inventories only (no files placed)

Outputs (under the staging target):
  - file_inventory.csv  (one row per file with path, device, size, mtime, sha256)
  - manifest_sha256.txt (sha256  filepath) suitable for 'shasum -c'
  - logs/ingest.log     (basic run info)

Notes:
  - This script does not upload; it prints an rsync command you can run after review.
  - Device folders are inferred from immediate subfolders of --input (e.g., camera_01, ARU_01).
"""

import argparse
import csv
import hashlib
import os
from pathlib import Path
import shutil
import sys
from datetime import datetime, timezone

DEVICE_PREFIXES = ("camera_", "aru_", "Camera", "ARU")  # accepted starts (case-insensitive)
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".cr2", ".nef", ".arw", ".dng"}
AUDIO_EXTS = {".wav", ".flac"}
VIDEO_EXTS = {".mp4", ".mov", ".avi"}

def norm_device_label(name: str) -> str:
    """Normalize device folder names to a safe label (e.g., camera_01 → CAM01, ARU_03 → ARU03)."""
    n = name.strip().replace("-", "_")
    low = n.lower()
    if low.startswith("camera_"):
        tail = low.split("camera_")[1]
        return f"CAM{tail.zfill(2) if tail.isdigit() else tail.upper()}"
    if low.startswith("aru_"):
        tail = low.split("aru_")[1]
        return f"ARU{tail.zfill(2) if tail.isdigit() else tail.upper()}"
    if low.startswith("camera"):
        tail = low.split("camera")[1]
        tail = tail[1:] if tail.startswith("_") else tail
        return f"CAM{tail.zfill(2) if tail.isdigit() else tail.upper()}"
    if low.startswith("aru"):
        tail = low.split("aru")[1]
        tail = tail[1:] if tail.startswith("_") else tail
        return f"ARU{tail.zfill(2) if tail.isdigit() else tail.upper()}"
    # fallback
    return n.replace(" ", "_")

def guess_device_type(label: str) -> str:
    up = label.upper()
    if up.startswith("CAM"): return "camera"
    if up.startswith("ARU"): return "aru"
    return "unknown"

def sha256_file(path: Path, bufsize: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(bufsize), b""):
            h.update(chunk)
    return h.hexdigest()

def discover_devices(input_dir: Path):
    """Return list of (device_path, device_label, device_type).

    These tuples become per-file metadata for `device_label` and `device_type`
    when constructing inventory rows later in `main()`.
    """
    devices = []
    for child in sorted(input_dir.iterdir()):
        if not child.is_dir():
            continue
        name = child.name
        if name.lower().startswith(DEVICE_PREFIXES) or any(name.lower().startswith(p) for p in [p.lower() for p in DEVICE_PREFIXES]):
            label = norm_device_label(name)
            devices.append((child, label, guess_device_type(label)))
        else:
            # allow arbitrary folders; still include but mark type=unknown
            label = norm_device_label(name)
            devices.append((child, label, "unknown"))
    if not devices:
        print(f"[WARN] No device subfolders found under {input_dir}")
    return devices

def make_staging_root(staging_base: Path, sdsC_root: Path, year: str, reserve: str, site: str, deployment: str) -> Path:
    # Matches /<SDSC-root>/<year>/<reserve>/<site>/<Deployment_deployid>/
    dep_dirname = f"Deployment_{deployment}"
    return staging_base.joinpath(sdsC_root, year, reserve, site, dep_dirname)

def safe_relpath(path: Path, start: Path) -> str:
    try:
        return str(path.relative_to(start))
    except Exception:
        return str(path)

def determine_media_class(p: Path) -> str:
    # Classify files into a coarse media type used in metadata
    # (written to `media_class` in the inventory CSV).
    ext = p.suffix.lower()
    if ext in IMAGE_EXTS: return "image"
    if ext in AUDIO_EXTS: return "audio"
    if ext in VIDEO_EXTS: return "video"
    return "other"

def build_argparser():
    ap = argparse.ArgumentParser(description="Prepare a field import for SDSC staging (tree + inventories).")
    ap.add_argument("--input", required=True, help="Path to the import folder containing device subfolders")
    ap.add_argument("--reserve", required=True, help="Reserve ID (e.g., R034)")
    ap.add_argument("--site", required=True, help="Site ID (e.g., S005)")
    ap.add_argument("--deployment", required=True, help="Deployment identifier (e.g., 20250905)")
    ap.add_argument("--staging", required=True, help="Local staging base directory")
    ap.add_argument("--sdsC-root", default="/expanse/projects/ucnrs-ssn/raw", help="SDSC root under which data will live")
    ap.add_argument("--mode", choices=["symlink", "copy", "plan"], default="symlink", help="Place files as symlinks (default), copy, or plan-only")
    ap.add_argument("--compute-hash", action="store_true", help="Compute SHA-256 for all files (slower). If off, writes inventory without checksums.")
    ap.add_argument("--rsync-user", default="your_username", help="Your SDSC login (for rsync cmd hint)")
    ap.add_argument("--rsync-host", default="expanse.sdsc.edu", help="SDSC host (for rsync cmd hint)")
    return ap

def main():
    args = build_argparser().parse_args()
    input_dir = Path(args.input).expanduser().resolve()
    if not input_dir.exists():
        print(f"[ERR] Input not found: {input_dir}")
        sys.exit(1)

    # Derive year safely from deployment if YYYYMMDD
    year = args.deployment[:4] if len(args.deployment) >= 4 and args.deployment[:4].isdigit() else datetime.now().strftime("%Y")

    staging_base = Path(args.staging).expanduser().resolve()
    sdsC_root = Path(args.sdsC_root.strip("/"))  # treat as path segments under staging_base
    staging_root = make_staging_root(staging_base, sdsC_root, year, args.reserve, args.site, args.deployment)

    # Create directories
    staging_root.mkdir(parents=True, exist_ok=True)
    logs_dir = staging_root.joinpath("logs")
    logs_dir.mkdir(parents=True, exist_ok=True)

    devices = discover_devices(input_dir)
    if not devices:
        print("[WARN] No device folders found; exiting.")
        return

    # In-memory collection of per-file metadata rows for the inventory CSV
    inventory_rows = []
    # Lines of the checksum manifest ("<sha256>  ./<relative_path>")
    manifest_lines = []
    dup_check = set()
    problems = 0

    for device_path, device_label, device_type in devices:
        dest_device_dir = staging_root.joinpath(device_label)
        dest_device_dir.mkdir(parents=True, exist_ok=True)

        # Walk files under device and build metadata for each file
        for root, _, files in os.walk(device_path):
            root_p = Path(root)
            for fname in files:
                src = root_p / fname
                if not src.is_file():
                    continue

                # Within device, preserve relative subpath (if any)
                rel_under_device = src.relative_to(device_path)
                dest = dest_device_dir / rel_under_device

                # ensure parent exists
                dest.parent.mkdir(parents=True, exist_ok=True)

                # plan/copy/symlink
                if args.mode == "copy":
                    shutil.copy2(src, dest)
                elif args.mode == "symlink":
                    # If dest exists, remove and relink
                    if dest.exists():
                        dest.unlink()
                    os.symlink(src, dest)
                elif args.mode == "plan":
                    pass  # just record

                # Collect per-file metadata for the inventory
                try:
                    stat = src.stat()
                    size_b = stat.st_size  # file size (bytes)
                    mtime_iso = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()  # last modified (UTC ISO8601)
                except Exception:
                    size_b = ""
                    mtime_iso = ""

                # Stable relative path recorded in metadata and used in manifest
                relpath_from_staging_root = safe_relpath(dest if args.mode != "plan" else dest, staging_root)

                # simple duplicate detection by (device_label, original filename)
                key = (device_label, fname)
                if key in dup_check:
                    print(f"[WARN] Duplicate filename in same device: {fname} (device {device_label})")
                dup_check.add(key)

                # Coarse media type saved as `media_class` in the inventory
                media_class = determine_media_class(src)

                # The inventory row (per-file metadata written to CSV)
                row = {
                    "relative_path": relpath_from_staging_root,  # where the file lands under the staging root
                    "device_label": device_label,                 # normalized device identifier (e.g., CAM01, ARU03)
                    "device_type": device_type,                   # inferred type (camera/aru/unknown)
                    "media_class": media_class,                   # coarse type (image/audio/video/other)
                    "size_bytes": size_b,                         # file size in bytes
                    "mtime_utc": mtime_iso,                       # last modified time in UTC ISO8601
                    "reserve": args.reserve,                      # ingest context: reserve code
                    "site": args.site,                            # ingest context: site code
                    "deployment": args.deployment,                # ingest context: deployment id
                    "source_abspath": str(src),                   # provenance: original absolute source path
                }
                inventory_rows.append(row)

                if args.compute_hash and src.is_file():
                    try:
                        digest = sha256_file(src)  # per-file content hash for integrity verification
                        # Record a manifest entry pairing hash with the staged relative path
                        manifest_lines.append(f"{digest}  ./{relpath_from_staging_root}")
                    except Exception as e:
                        print(f"[ERR] Hash failed for {src}: {e}")
                        problems += 1

    # Write the inventory CSV (one row per file with all metadata fields)
    inv_path = staging_root / "file_inventory.csv"
    inv_fields = [
        "relative_path", "device_label", "device_type", "media_class",
        "size_bytes", "mtime_utc", "reserve", "site", "deployment", "source_abspath"
    ]
    with inv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=inv_fields)
        w.writeheader()
        w.writerows(inventory_rows)

    # Write checksum manifest if requested (for later `shasum -c` verification)
    if args.compute_hash:
        man_path = staging_root / "manifest_sha256.txt"
        with man_path.open("w") as mf:
            mf.write("\n".join(manifest_lines) + ("\n" if manifest_lines else ""))
    else:
        man_path = None

    # Append a run-level metadata summary to the ingest log
    log_path = logs_dir / "ingest.log"
    with log_path.open("a") as lf:
        lf.write(f"[{datetime.now().isoformat()}] input={input_dir} reserve={args.reserve} site={args.site} deployment={args.deployment} mode={args.mode} files={len(inventory_rows)} problems={problems}\n")

    print("\n✅ Staging prepared at:")
    print(f"   {staging_root}")
    print(f"   Inventory: {inv_path}")
    if man_path:
        print(f"   Manifest:  {man_path}")
        print("   Verify later with:  shasum -c manifest_sha256.txt")

    # print suggested rsync command
    remote_path = f"/expanse/projects/ucnrs-ssn/raw/{year}/{args.reserve}/{args.site}/Deployment_{args.deployment}/"
    print("\n📤 Suggested upload command (rsync):")
    print(f'rsync -avh --info=progress2 "{staging_root}/" {args.rsync_user}@{args.rsync_host}:"{remote_path}"')
    print("\n(Review the staged tree first; if you used --mode symlink, rsync will follow symlinks as regular files by default on macOS rsync 2.6.9; if needed, add --copy-links)\n")

if __name__ == "__main__":
    main()
