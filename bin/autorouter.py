#!/usr/bin/env python3

import os
import sys
import time
import shutil
import argparse
from pathlib import Path
from typing import Dict, Tuple

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, ARCHIVE_EXTS, JUNK_MAP
import common

CHECK_INTERVAL = 2
VIDEO_REQUIRED_MATCHES = 3
seen: Dict[Path, Tuple[int, int, int, int]] = {}

log_obj = None  # Initialized in __main__ with mode

def log(msg, mode):
    log_obj.info(f"[{mode}] {msg}")

def video_stable(path: Path) -> bool:
    try:
        st = path.stat()
    except FileNotFoundError: return False
    size = st.st_size
    mtime_ns = st.st_mtime_ns
    inode = st.st_ino
    last = seen.get(path)
    if not last:
        seen[path] = (size, mtime_ns, inode, 1)
        return False
    last_size, last_mtime, last_inode, matches = last
    if (size, mtime_ns, inode) != (last_size, last_mtime, last_inode):
        seen[path] = (size, mtime_ns, inode, 1)
        return False
    matches += 1
    seen[path] = (size, mtime_ns, inode, matches)
    return matches >= VIDEO_REQUIRED_MATCHES

def get_junk_category(ext: str) -> str:
    for category, exts in JUNK_MAP.items():
        if ext in exts: return category
    return None

def move(src: Path, dest_dir: Path, mode: str) -> None:
    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
        dst_path = dest_dir / src.name
        if dst_path.exists(): return
        shutil.move(str(src), str(dst_path))
        log(f"Routed: {src.name} -> {dest_dir.name}", mode)
    except Exception as e:
        log(f"Move failed: {src.name} | {e}", mode)

def main(mode: str) -> None:
    config = common.load_config()
    TRASH_ROOT = Path(config['paths']['trash_root'])

    if mode == 'series':
        pipeline = config['paths']['series_pipeline']
    elif mode == 'movies':
        pipeline = config['paths']['movie_pipeline']
    else:
        sys.exit(1)

    WATCH_DIR = Path(pipeline['system_drop'])
    DEST_DIR = Path(pipeline['processing'])
    FAILED_DIR = Path(pipeline['failed'])

    for p in [WATCH_DIR, DEST_DIR, FAILED_DIR, TRASH_ROOT]:
        if not p.exists():
            try:
                p.mkdir(parents=True, exist_ok=True)
                log(f"Created dir: {p}", mode)
            except Exception as e:
                log(f"Failed to create {p}: {e}", mode)

    log(f"Started | Watching: {WATCH_DIR} -> Target: {DEST_DIR}", mode)

    while True:
        try:
            items = sorted(WATCH_DIR.iterdir(), key=lambda p: p.stat().st_mtime)
            for item in items:
                if not item.is_file(): continue
                if item.name.startswith("."): continue
                if item.name.endswith(".ctx.json"): continue
                ext = item.suffix.lower()

                if ext in ARCHIVE_EXTS: continue
                if ext in VIDEO_EXTS:
                    if not video_stable(item): continue

                if ext in VIDEO_EXTS:
                    move(item, DEST_DIR, mode)
                    ctx = item.with_suffix(item.suffix + ".ctx.json")
                    if ctx.exists(): move(ctx, DEST_DIR, mode)
                elif (cat := get_junk_category(ext)):
                    trash_dest = TRASH_ROOT / cat
                    move(item, trash_dest, mode)
                else:
                    move(item, FAILED_DIR, mode)
                seen.pop(item, None)
        except Exception as e:
            log(f"Loop error: {e}", mode)
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=['series', 'movies'])
    args = parser.parse_args()
    log_obj, _ = common.setup_logger("autorouter", args.mode)
    main(args.mode)
