#!/usr/bin/env python3
"""
automouse.py -- File Watcher / Gatekeeper.

Monitors input_drop folders and moves stable files into the pipeline.
Uses non-blocking stability tracking (no per-file sleep) and batch
awareness so it waits for entire copy/download operations to finish
before moving anything.
"""

import os
import sys
import time
import shutil
import argparse
from pathlib import Path

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import (
    JUNK_EXTENSIONS, IGNORE_PARTIALS, ARCHIVE_EXTS,
    MIN_SSD_FREE_GB, MOUSE_STABILITY_SCANS, MOUSE_BATCH_SETTLE,
    MOUSE_SCAN_INTERVAL,
)
import common

# Logger initialized in __main__ with mode suffix
logger = None


# ================= HELPERS =================

def get_tree_snapshot(path):
    """Get size + file count + newest mtime for a file or directory tree.
    Returns (total_size, file_count, newest_mtime) or (-1, 0, 0) on error."""
    try:
        if os.path.isfile(path):
            st = os.stat(path)
            return st.st_size, 1, st.st_mtime_ns
        total = 0
        count = 0
        newest = 0
        for dirpath, dirnames, filenames in os.walk(path):
            # Check dir mtime too (changes when files are added/removed)
            try:
                dm = os.stat(dirpath).st_mtime_ns
                if dm > newest:
                    newest = dm
            except OSError:
                pass
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    try:
                        st = os.stat(fp)
                        total += st.st_size
                        count += 1
                        if st.st_mtime_ns > newest:
                            newest = st.st_mtime_ns
                    except OSError:
                        pass
        return total, count, newest
    except OSError:
        return -1, 0, 0


def get_tree_size(path):
    """Recursively gets the size of a directory or file (backwards compat)."""
    size, _, _ = get_tree_snapshot(path)
    return size


def get_free_space_gb(directory):
    try:
        if not directory.exists():
            return 0
        _, _, free = shutil.disk_usage(directory)
        return free / (1024 ** 3)
    except Exception as e:
        logger.warning(f"Could not check free space: {e}")
        return 0


# ================= STABILITY TRACKER =================

class StabilityTracker:
    """Non-blocking file stability tracking.

    Instead of sleeping per-file, records sizes each scan loop and
    marks files stable after N consecutive unchanged readings.
    Also tracks batch settling — if new items keep appearing, it
    holds off on moving anything.
    """

    def __init__(self, required_scans=MOUSE_STABILITY_SCANS, batch_settle=MOUSE_BATCH_SETTLE):
        self.required = required_scans
        self.batch_settle = batch_settle
        self._snapshots = {}     # path_str -> (size, file_count, newest_mtime)
        self._stable_count = {}  # path_str -> consecutive_unchanged_scans
        self._prev_item_set = set()
        self._batch_stable_for = 0

    def scan(self, items: list[Path]):
        """Call once per loop iteration with all items currently in the folder.
        Records size+count+mtime and updates stability counts."""
        current_paths = set()
        any_changed = False

        for item in items:
            p = str(item)
            current_paths.add(p)

            size, count, mtime = get_tree_snapshot(p)
            if size < 0:
                continue

            snapshot = (size, count, mtime)
            prev = self._snapshots.get(p)
            self._snapshots[p] = snapshot

            if prev is not None and snapshot == prev and size > 0:
                self._stable_count[p] = self._stable_count.get(p, 0) + 1
            else:
                self._stable_count[p] = 0
                if prev is not None and snapshot != prev:
                    any_changed = True

        # Batch settling: reset if items changed OR any snapshot changed
        # Tracks size + file count + mtime to catch Samba between-file gaps
        new_items = current_paths - self._prev_item_set
        if new_items or len(current_paths) != len(self._prev_item_set) or any_changed:
            self._batch_stable_for = 0
        else:
            self._batch_stable_for += 1

        self._prev_item_set = current_paths

        # Cleanup entries for items that disappeared
        gone = set(self._snapshots.keys()) - current_paths
        for p in gone:
            self._snapshots.pop(p, None)
            self._stable_count.pop(p, None)

    def is_file_stable(self, item: Path) -> bool:
        """True if this specific file/folder has been unchanged for enough scans."""
        return self._stable_count.get(str(item), 0) >= self.required

    def is_batch_settled(self) -> bool:
        """True if no new items have appeared for enough scans.
        This prevents grabbing file #1 while #2-#50 are still being copied."""
        return self._batch_stable_for >= self.batch_settle

    def all_stable(self, items: list[Path]) -> bool:
        """True if ALL given items are individually stable."""
        return all(self.is_file_stable(i) for i in items)

    def get_stable_items(self, items: list[Path]) -> list[Path]:
        """Return only items that are individually stable."""
        return [i for i in items if self.is_file_stable(i)]

    def status_summary(self, items: list[Path]) -> str:
        """Debug summary: X/Y stable, batch settle progress, item names."""
        stable = sum(1 for i in items if self.is_file_stable(i))
        names = ', '.join(i.name for i in items[:3])
        if len(items) > 3:
            names += f' +{len(items) - 3} more'
        total_gb = sum(get_tree_snapshot(str(i))[0] for i in items) / (1024 ** 3)
        return (f"{names} | {stable}/{len(items)} stable, "
                f"batch: {self._batch_stable_for}/{self.batch_settle}, "
                f"{total_gb:.1f}GB")


# ================= ARCHIVE GROUP DETECTION =================

# Multi-part RAR patterns: .rar, .r00, .r01, .part1.rar, etc.
_ARCHIVE_GROUP_EXTS = frozenset({
    '.r00', '.r01', '.r02', '.r03', '.r04', '.r05', '.r06', '.r07',
    '.r08', '.r09', '.r10', '.r11', '.r12', '.r13', '.r14', '.r15',
    '.r16', '.r17', '.r18', '.r19', '.r20', '.r21', '.r22', '.r23',
    '.r24', '.r25', '.r26', '.r27', '.r28', '.r29', '.r30',
    '.r31', '.r32', '.r33', '.r34', '.r35', '.r36', '.r37', '.r38',
    '.r39', '.r40', '.r41', '.r42', '.r43', '.r44', '.r45', '.r46',
    '.r47', '.r48', '.r49', '.r50',
})


def get_archive_group(item: Path, all_items: list[Path]) -> list[Path]:
    """If item is part of a multi-part archive, return all related parts.
    Returns [item] for standalone files."""
    ext = item.suffix.lower()
    if ext not in ARCHIVE_EXTS and ext not in _ARCHIVE_GROUP_EXTS:
        return [item]

    # Find all items that share the same stem base (before .part1, .r01, etc.)
    base = item.stem
    # Strip .partN suffix if present: "file.part1.rar" -> stem="file.part1" -> base="file"
    import re
    base = re.sub(r'\.part\d+$', '', base, flags=re.I)

    group = []
    for other in all_items:
        other_ext = other.suffix.lower()
        if other_ext not in ARCHIVE_EXTS and other_ext not in _ARCHIVE_GROUP_EXTS:
            continue
        other_base = re.sub(r'\.part\d+$', '', other.stem, flags=re.I)
        if other_base == base:
            group.append(other)

    return group if group else [item]


# ================= MAIN PIPELINE =================

def process_pipeline(mode):
    config = common.load_config()
    tag = mode.upper()  # "SERIES" or "MOVIES"

    def log(msg, level="info"):
        getattr(logger, level)(f"[{tag}] {msg}")

    if mode == 'series':
        pipeline = config['paths']['series_pipeline']
        quota_limit = config['flow_control']['series_quota_gb']
    elif mode == 'movies':
        pipeline = config['paths']['movie_pipeline']
        quota_limit = config['flow_control']['movies_quota_gb']
    else:
        logger.error("Invalid mode. Use 'series' or 'movies'.")
        sys.exit(1)

    INPUT_DROP = Path(pipeline['input_drop'])
    SYSTEM_DROP = Path(pipeline['system_drop'])
    SYSTEM_HOME = Path(pipeline['system_home'])
    TRASH_ROOT = Path(config['paths']['trash_root'])

    for p in [INPUT_DROP, SYSTEM_DROP, SYSTEM_HOME, TRASH_ROOT]:
        if not p.exists():
            try:
                p.mkdir(parents=True, exist_ok=True)
                log(f"Created directory: {p}")
            except Exception as e:
                log(f"Failed to create {p}: {e}", "error")

    log(f"Started Gatekeeper. Watching: {INPUT_DROP}")

    tracker = StabilityTracker()
    last_settling_log = 0

    while True:
        try:
            # Check disk space
            ssd_free = get_free_space_gb(SYSTEM_HOME)
            if ssd_free < MIN_SSD_FREE_GB:
                log(f"Work Drive Full ({ssd_free:.2f}GB free). Pausing.", "warning")
                time.sleep(60)
                continue

            # Get current items
            items = sorted([x for x in INPUT_DROP.iterdir() if x.exists()], key=lambda p: p.stat().st_mtime)

            # Filter out partial downloads early
            items = [x for x in items if x.suffix not in IGNORE_PARTIALS]

            # Update stability tracker (non-blocking — just records sizes)
            tracker.scan(items)

            # Handle junk files immediately (no stability needed)
            real_items = []
            for item in items:
                if item.is_file() and item.suffix.lower() in JUNK_EXTENSIONS:
                    trash_cat = "executables" if item.suffix in ['.exe', '.bat'] else "images"
                    trash_dest = TRASH_ROOT / trash_cat
                    trash_dest.mkdir(parents=True, exist_ok=True)
                    try:
                        shutil.move(str(item), str(trash_dest / item.name))
                        log(f"Junk -> Trash: {item.name}")
                    except Exception as e:
                        log(f"Failed to trash junk {item.name}: {e}", "warning")
                    continue
                real_items.append(item)

            if not real_items:
                time.sleep(MOUSE_SCAN_INTERVAL)
                continue

            # Check batch settling — don't move anything if new files are still appearing
            if not tracker.is_batch_settled():
                now = time.time()
                if now - last_settling_log > 15:
                    log(f"Batch settling... ({tracker.status_summary(real_items)})")
                    last_settling_log = now
                time.sleep(MOUSE_SCAN_INTERVAL)
                continue

            # Process individually stable items
            moved_any = False
            processed_archives = set()  # track archive groups already handled

            for item in real_items:
                if str(item) in processed_archives:
                    continue

                # Archive group handling: wait for ALL parts to be stable
                ext = item.suffix.lower()
                if ext in ARCHIVE_EXTS or ext in _ARCHIVE_GROUP_EXTS:
                    group = get_archive_group(item, real_items)
                    if not tracker.all_stable(group):
                        continue
                    # Move all parts together
                    current_usage = get_tree_size(str(SYSTEM_HOME)) / (1024 ** 3)
                    group_size = sum(get_tree_size(str(g)) for g in group) / (1024 ** 3)
                    if (current_usage + group_size) > quota_limit and current_usage > 0.5:
                        log(f"Quota Full ({current_usage:.1f}/{quota_limit}GB). Waiting on: {item.name}")
                        break
                    for part in group:
                        dest_path = SYSTEM_DROP / part.name
                        if dest_path.exists():
                            continue
                        try:
                            shutil.move(str(part), str(dest_path))
                            log(f"Admitted: {part.name} ({get_tree_size(str(dest_path)) / (1024**3):.2f}GB)")
                            moved_any = True
                        except Exception as e:
                            log(f"Move Failed: {part.name}: {e}", "error")
                        processed_archives.add(str(part))
                    continue

                # Regular file / folder
                if not tracker.is_file_stable(item):
                    continue

                current_usage = get_tree_size(str(SYSTEM_HOME)) / (1024 ** 3)
                item_size = get_tree_size(str(item)) / (1024 ** 3)

                if (current_usage + item_size) > quota_limit and current_usage > 0.5:
                    log(f"Quota Full ({current_usage:.1f}/{quota_limit}GB). Waiting on: {item.name}")
                    break

                dest_path = SYSTEM_DROP / item.name
                if dest_path.exists():
                    continue

                try:
                    shutil.move(str(item), str(dest_path))
                    log(f"Admitted: {item.name} ({item_size:.2f}GB)")
                    moved_any = True
                except Exception as e:
                    log(f"Move Failed: {item.name}: {e}", "error")

        except Exception as e:
            log(f"Main Loop Error: {e}", "error")
            time.sleep(5)

        time.sleep(MOUSE_SCAN_INTERVAL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=['series', 'movies'])
    args = parser.parse_args()
    logger, _ = common.setup_logger("automouse", args.mode)
    process_pipeline(args.mode)
