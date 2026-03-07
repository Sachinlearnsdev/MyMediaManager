#!/usr/bin/env python3
"""
autoharbor.py -- Staging / Extraction / Folder Flattening.

Watches system_drop folders (internal Work pipeline). Files here
have already been stability-checked by AutoMouse, so AutoHarbor
processes them immediately — no stability tracking needed.

Extracts archives (RAR/ZIP/7z), flattens nested folder structures,
and promotes video files to the root for the next pipeline stage.
"""

import os
import sys
import json
import time
import shutil
import signal
import subprocess
import argparse
from pathlib import Path

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, ARCHIVE_EXTS, EXTRACT_TIMEOUT, SCAN_INTERVAL
import common

HAS_UNRAR = shutil.which("unrar") is not None
HAS_7Z = shutil.which("7z") is not None
stop = False

log_obj = None  # Initialized in __main__ with mode

def log(msg, mode):
    log_obj.info(f"[{mode}] {msg}")

def is_video(p: Path):
    return p.suffix.lower() in VIDEO_EXTS

def is_entry_archive(p: Path):
    return p.suffix.lower() in ARCHIVE_EXTS

# Multi-part RAR extensions
_MULTI_PART_EXTS = frozenset(
    f'.r{i:02d}' for i in range(51)
)

def is_archive_part(p: Path):
    """Check if file is an archive or multi-part archive segment."""
    ext = p.suffix.lower()
    return ext in ARCHIVE_EXTS or ext in _MULTI_PART_EXTS


# ================= ARCHIVE HANDLING =================

def find_archive_group(archive: Path, all_items: list[Path]) -> list[Path]:
    """Find all parts of a multi-part archive."""
    import re
    base = re.sub(r'\.part\d+$', '', archive.stem, flags=re.I)
    group = []
    for item in all_items:
        if not is_archive_part(item):
            continue
        item_base = re.sub(r'\.part\d+$', '', item.stem, flags=re.I)
        if item_base == base:
            group.append(item)
    return group if group else [archive]


def extract_archive(p: Path, dest_dir: Path, mode: str):
    """Extract an archive using unrar or 7z."""
    ext = p.suffix.lower()

    # Choose extraction tool
    if ext == '.rar' or ext in _MULTI_PART_EXTS:
        if HAS_UNRAR:
            return _extract_unrar(p, dest_dir, mode)
        elif HAS_7Z:
            return _extract_7z(p, dest_dir, mode)
        else:
            log(f"SKIP extraction (no unrar/7z): {p.name}", mode)
            return False
    elif ext in ('.zip', '.7z'):
        if HAS_7Z:
            return _extract_7z(p, dest_dir, mode)
        elif ext == '.zip':
            return _extract_zipfile(p, dest_dir, mode)
        else:
            log(f"SKIP extraction (no 7z): {p.name}", mode)
            return False
    return False


def _extract_unrar(p: Path, dest_dir: Path, mode: str) -> bool:
    log(f"Extracting (unrar): {p.name}", mode)
    try:
        result = subprocess.run(
            ["unrar", "x", "-o+", str(p), str(dest_dir)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=EXTRACT_TIMEOUT,
        )
        if result.returncode == 0:
            log(f"Extracted OK: {p.name}", mode)
            return True
        else:
            stderr = result.stderr.decode(errors='replace')[:200]
            log(f"Extract failed ({p.name}): {stderr}", mode)
            return False
    except subprocess.TimeoutExpired:
        log(f"Extract timeout: {p.name}", mode)
        return False
    except Exception as e:
        log(f"Extract error ({p.name}): {e}", mode)
        return False


def _extract_7z(p: Path, dest_dir: Path, mode: str) -> bool:
    log(f"Extracting (7z): {p.name}", mode)
    try:
        result = subprocess.run(
            ["7z", "x", "-y", f"-o{dest_dir}", str(p)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=EXTRACT_TIMEOUT,
        )
        if result.returncode == 0:
            log(f"Extracted OK: {p.name}", mode)
            return True
        else:
            stderr = result.stderr.decode(errors='replace')[:200]
            log(f"Extract failed ({p.name}): {stderr}", mode)
            return False
    except subprocess.TimeoutExpired:
        log(f"Extract timeout: {p.name}", mode)
        return False
    except Exception as e:
        log(f"Extract error ({p.name}): {e}", mode)
        return False


def _extract_zipfile(p: Path, dest_dir: Path, mode: str) -> bool:
    """Fallback ZIP extraction using Python stdlib."""
    import zipfile
    log(f"Extracting (zipfile): {p.name}", mode)
    try:
        with zipfile.ZipFile(str(p), 'r') as zf:
            zf.extractall(str(dest_dir))
        log(f"Extracted OK: {p.name}", mode)
        return True
    except Exception as e:
        log(f"Extract error ({p.name}): {e}", mode)
        return False


# ================= CTX / FOLDER PROCESSING =================

def write_ctx(video_dest: Path, source_root, containers, relpath, mode):
    if mode != 'series':
        return
    ctx_path = video_dest.with_suffix(video_dest.suffix + ".ctx.json")
    if ctx_path.exists():
        return
    ctx = {
        "source_root": source_root,
        "source_containers": containers,
        "source_relpath": relpath,
        "pipeline_mode": mode,
        "ingested_at": int(time.time()),
        "notes": "autoharbor extraction"
    }
    try:
        ctx_path.write_text(json.dumps(ctx, indent=2))
        log(f"CTX Created: {ctx_path.name}", mode)
    except Exception as e:
        log(f"CTX Error: {e}", mode)


def process_folder(folder: Path, dest_dir: Path, mode: str):
    log(f"Flattening Folder: {folder.name}", mode)
    for root, _, files in os.walk(folder):
        for name in files:
            src = Path(root) / name
            if is_video(src) or is_entry_archive(src):
                dst = dest_dir / name
                if dst.exists():
                    # Collision: disambiguate using source subfolder path
                    # e.g., "Season 2/Show - 01.mkv" → "Season 2 - Show - 01.mkv"
                    try:
                        rel = src.relative_to(folder)
                        subfolder_parts = list(rel.parts[:-1])
                    except Exception:
                        subfolder_parts = []
                    if subfolder_parts:
                        prefix = " - ".join(subfolder_parts)
                        dst = dest_dir / f"{prefix} - {name}"
                    if dst.exists():
                        # Still collision, use timestamp
                        base_stem = Path(name).stem
                        ext = Path(name).suffix
                        ts = int(time.time())
                        dst = dest_dir / f"{base_stem}_{ts}{ext}"
                    if dst.exists():
                        continue
                    log(f"Collision resolved: {name} -> {dst.name}", mode)
                try:
                    shutil.move(str(src), str(dst))
                    log(f"Promoted: {src.name} -> Root", mode)
                    if is_video(dst):
                        try:
                            rel = src.relative_to(folder)
                            parts = list(rel.parts[:-1])
                            write_ctx(dst, folder.name, parts, str(rel), mode)
                        except Exception as e:
                            log_obj.debug(f"CTX relative path fallback: {e}")
                            write_ctx(dst, folder.name, [], src.name, mode)
                except Exception as e:
                    log(f"Move error: {e}", mode)
    try:
        shutil.rmtree(folder)
        log(f"Removed empty folder: {folder.name}", mode)
    except Exception as e:
        log(f"Folder delete error: {e}", mode)


# ================= SIGNAL HANDLING =================

def shutdown(sig, frm):
    global stop
    stop = True

try:
    signal.signal(signal.SIGTERM, shutdown)
except (OSError, ValueError):
    pass
signal.signal(signal.SIGINT, shutdown)


# ================= MAIN LOOP =================

def main(mode):
    config = common.load_config()

    if mode == 'series':
        pipeline = config['paths']['series_pipeline']
    elif mode == 'movies':
        pipeline = config['paths']['movie_pipeline']
    else:
        sys.exit(1)

    DROP_DIR = Path(pipeline['system_drop'])

    if not DROP_DIR.exists():
        DROP_DIR.mkdir(parents=True, exist_ok=True)

    log(f"Started ({mode}). Watching: {DROP_DIR}", mode)
    if HAS_UNRAR:
        log("Extraction: unrar available", mode)
    if HAS_7Z:
        log("Extraction: 7z available", mode)
    if not HAS_UNRAR and not HAS_7Z:
        log("WARNING: No extraction tools found (unrar, 7z). Archives will be skipped.", mode)

    while not stop:
        try:
            items = sorted(DROP_DIR.iterdir(), key=lambda p: p.stat().st_mtime)
            processed_groups = set()

            for item in items:
                if not item.exists():
                    continue

                # --- Archives: group + extract immediately ---
                if item.is_file() and is_archive_part(item):
                    if str(item) in processed_groups:
                        continue

                    # Find all parts of this archive
                    group = find_archive_group(item, items)

                    # Find the main .rar file in the group
                    main_rar = None
                    for part in group:
                        if part.suffix.lower() == '.rar':
                            main_rar = part
                            break
                    if not main_rar:
                        main_rar = group[0]

                    success = extract_archive(main_rar, DROP_DIR, mode)
                    if success:
                        for part in group:
                            try:
                                part.unlink(missing_ok=True)
                            except Exception:
                                pass
                        log(f"Cleaned archive parts: {len(group)} files", mode)

                    for part in group:
                        processed_groups.add(str(part))

                # --- Folders: flatten ---
                elif item.is_dir():
                    process_folder(item, DROP_DIR, mode)

        except Exception as e:
            log(f"Loop error: {e}", mode)
        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=['series', 'movies'])
    args = parser.parse_args()
    log_obj, _ = common.setup_logger("autoharbor", args.mode)
    main(args.mode)
