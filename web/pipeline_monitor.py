#!/usr/bin/env python3
"""
pipeline_monitor.py -- Scans pipeline folders and emits real-time updates via SocketIO.
"""

import json
import threading
import time
from pathlib import Path

try:
    import psutil
except ImportError:
    psutil = None

# Maps pipeline stages to their config paths and labels
SERIES_STAGES = [
    {"id": "drop_shows",        "config_key": ("paths", "series_pipeline", "input_drop"),      "label": "Drop Shows"},
    {"id": "series_system_drop","config_key": ("paths", "series_pipeline", "system_drop"),      "label": "Intake Queue"},
    {"id": "series_processing", "config_key": ("paths", "series_pipeline", "processing"),       "label": "Processing Queue"},
    {"id": "series_identify",   "config_key": ("paths", "series_pipeline", "staged", "identify"), "label": "Identify Queue"},
    {"id": "staged_tv",         "config_key": ("paths", "series_pipeline", "staged", "tv"),     "label": "TV Queue"},
    {"id": "staged_cartoons",   "config_key": ("paths", "series_pipeline", "staged", "cartoons"), "label": "Cartoon Queue"},
    {"id": "staged_anime",      "config_key": ("paths", "series_pipeline", "staged", "anime"),          "label": "Anime Queue"},
    {"id": "staged_reality",    "config_key": ("paths", "series_pipeline", "staged", "reality"),        "label": "Reality Queue"},
    {"id": "staged_talkshow",   "config_key": ("paths", "series_pipeline", "staged", "talkshow"),       "label": "Talk Show Queue"},
    {"id": "staged_docs",       "config_key": ("paths", "series_pipeline", "staged", "documentaries"),  "label": "Docs Queue"},
    {"id": "series_failed",     "config_key": ("paths", "series_pipeline", "failed"),                    "label": "Failed (Shows)"},
]

MOVIE_STAGES = [
    {"id": "drop_movies",       "config_key": ("paths", "movie_pipeline", "input_drop"),       "label": "Drop Movies"},
    {"id": "movies_system_drop","config_key": ("paths", "movie_pipeline", "system_drop"),       "label": "Intake Queue"},
    {"id": "movies_processing", "config_key": ("paths", "movie_pipeline", "processing"),        "label": "Processing Queue"},
    {"id": "movies_staged",     "config_key": ("paths", "movie_pipeline", "staged", "movies"),  "label": "Movie Queue"},
    {"id": "movies_failed",     "config_key": ("paths", "movie_pipeline", "failed"),            "label": "Failed (Movies)"},
]

REVIEW_STAGES = [
    {"id": "review_shows",   "config_key": ("paths", "series_pipeline", "review"),        "label": "Review Shows"},
    {"id": "review_movies",  "config_key": ("paths", "movie_pipeline", "review"),         "label": "Review Movies"},
]

DUPLICATE_STAGES = [
    {"id": "dup_shows",   "config_key": ("paths", "series_pipeline", "duplicates"),  "label": "Duplicate Shows"},
    {"id": "dup_movies",  "config_key": ("paths", "movie_pipeline", "duplicates"),   "label": "Duplicate Movies"},
]

LIBRARY_STAGES = [
    {"id": "library_tv",            "output_key": "tv"},
    {"id": "library_cartoons",      "output_key": "cartoons"},
    {"id": "library_anime",         "output_key": "anime_shows"},
    {"id": "library_movies",        "output_key": "movies"},
    {"id": "library_anime_movies",  "output_key": "anime_movies"},
    {"id": "library_reality",       "output_key": "reality"},
    {"id": "library_talkshow",      "output_key": "talkshow"},
    {"id": "library_docs_series",   "output_key": "documentaries_series"},
    {"id": "library_docs_movies",   "output_key": "documentaries_movies"},
    {"id": "library_standup",       "output_key": "standup"},
]


def _resolve_config_key(config: dict, keys: tuple) -> str:
    """Walk nested dict by key tuple."""
    val = config
    for k in keys:
        if isinstance(val, dict):
            val = val.get(k, '')
        else:
            return ''
    return str(val) if val else ''


def _read_ctx(item: Path) -> str:
    """Read source_root from a CTX sidecar file if it exists."""
    try:
        ctx_path = item.with_suffix(item.suffix + ".ctx.json")
        if not ctx_path.exists():
            ctx_path = item.with_name(item.stem + ".ctx.json")
        if ctx_path.exists():
            data = json.loads(ctx_path.read_text(encoding='utf-8'))
            return data.get("source_root", "")
    except Exception:
        pass
    return ""


def _scan_folder(path: Path) -> dict:
    """Get file list and count for a folder, sorted oldest-first (FIFO)."""
    if not path.exists():
        return {"count": 0, "files": []}
    try:
        files = []
        for item in path.iterdir():
            if item.name.startswith('.'):
                continue
            if item.suffix == '.json' and ('.ctx.' in item.name or '.reason.' in item.name or '.dup.' in item.name):
                continue  # Skip sidecar files from count
            st = item.stat()
            entry = {
                "name": item.name,
                "size_mb": round(st.st_size / (1024 * 1024), 1) if item.is_file() else 0,
                "is_dir": item.is_dir(),
                "mtime": st.st_mtime,
            }
            # Include CTX source_root for tracking correlation
            ctx_source = _read_ctx(item)
            if ctx_source:
                entry["ctx_source"] = ctx_source
            files.append(entry)
        files.sort(key=lambda f: f['mtime'])
        return {"count": len(files), "files": files}
    except OSError:
        return {"count": 0, "files": []}


def _scan_folder_recent(path: Path, max_age_seconds: int = 300) -> dict:
    """Recursively find files modified within max_age_seconds."""
    if not path.exists():
        return {"count": 0, "files": []}
    cutoff = time.time() - max_age_seconds
    files = []
    try:
        for item in path.rglob('*'):
            if item.is_file() and not item.name.startswith('.'):
                try:
                    st = item.stat()
                    if st.st_mtime >= cutoff:
                        files.append({
                            "name": item.name,
                            "size_mb": round(st.st_size / (1024 * 1024), 1),
                            "is_dir": False,
                            "mtime": st.st_mtime,
                        })
                except OSError:
                    continue
        files.sort(key=lambda f: f['mtime'], reverse=True)
        return {"count": len(files), "files": files[:50]}
    except OSError:
        return {"count": 0, "files": []}


def _get_disk_info(config: dict) -> dict:
    """Get disk usage for configured roots."""
    info = {}
    if not psutil:
        return info
    roots = config.get('paths', {}).get('roots', {})
    for name, path_str in roots.items():
        try:
            usage = psutil.disk_usage(path_str)
            info[name] = {
                "total_gb": round(usage.total / (1024**3), 1),
                "free_gb": round(usage.free / (1024**3), 1),
                "used_percent": round(usage.percent, 1),
            }
        except (OSError, FileNotFoundError):
            pass
    return info


class PipelineMonitor:
    def __init__(self, config: dict):
        self.config = config
        self._running = False
        self._thread = None
        self._on_snapshot = None  # Optional callback(snapshot) for auto-management

    def start(self, socketio, on_snapshot=None):
        self._running = True
        self._on_snapshot = on_snapshot
        self._thread = threading.Thread(
            target=self._monitor_loop, args=(socketio,), daemon=True
        )
        self._thread.start()

    def stop(self):
        self._running = False

    def get_snapshot(self) -> dict:
        """Get current pipeline state."""
        data = {"series": {}, "movies": {}, "review": {}, "duplicates": {}, "disk": {}}

        for stage in SERIES_STAGES:
            path_str = _resolve_config_key(self.config, stage['config_key'])
            path = Path(path_str) if path_str else None
            data["series"][stage['id']] = {
                "label": stage['label'],
                **(_scan_folder(path) if path else {"count": 0, "files": []}),
            }

        for stage in MOVIE_STAGES:
            path_str = _resolve_config_key(self.config, stage['config_key'])
            path = Path(path_str) if path_str else None
            data["movies"][stage['id']] = {
                "label": stage['label'],
                **(_scan_folder(path) if path else {"count": 0, "files": []}),
            }

        for stage in REVIEW_STAGES:
            path_str = _resolve_config_key(self.config, stage['config_key'])
            path = Path(path_str) if path_str else None
            data["review"][stage['id']] = {
                "label": stage['label'],
                **(_scan_folder(path) if path else {"count": 0, "files": []}),
            }

        for stage in DUPLICATE_STAGES:
            path_str = _resolve_config_key(self.config, stage['config_key'])
            path = Path(path_str) if path_str else None
            data["duplicates"][stage['id']] = {
                "label": stage['label'],
                **(_scan_folder(path) if path else {"count": 0, "files": []}),
            }

        # Library recent output (for tracking final filenames)
        library_root = self.config.get('paths', {}).get('roots', {}).get('library', '')
        output_paths = self.config.get('paths', {}).get('output', {})
        data["library"] = {}
        for stage in LIBRARY_STAGES:
            rel_path = output_paths.get(stage['output_key'], '')
            if rel_path and library_root:
                full_path = Path(library_root) / rel_path
                data["library"][stage['id']] = _scan_folder_recent(full_path, max_age_seconds=86400)
            else:
                data["library"][stage['id']] = {"count": 0, "files": []}

        data["disk"] = _get_disk_info(self.config)
        return data

    def _monitor_loop(self, socketio):
        while self._running:
            try:
                snapshot = self.get_snapshot()
                socketio.emit('pipeline_update', snapshot, namespace='/')
                if self._on_snapshot:
                    try:
                        self._on_snapshot(snapshot)
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(3)
