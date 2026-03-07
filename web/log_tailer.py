#!/usr/bin/env python3
"""
log_tailer.py -- Reads and streams log files for real-time viewing.

Supports individual service logs AND pipeline-level merged views.
"""

import re
import threading
import time
from pathlib import Path
from datetime import datetime

# Actual log file prefixes (must match what common.setup_logger produces)
# Dual-mode services now produce separate logs: automouse_series, automouse_movies, etc.
SERVICE_LOG_NAMES = [
    "panel",
    "automouse_series", "autoharbor_series", "autorouter_series", "structpilot_series",
    "automouse_movies", "autoharbor_movies", "autorouter_movies", "structpilot_movies",
    "contentclassifier",
    "tvproc", "cartoonsproc", "animeproc",
    "realityproc", "talkshowproc", "documentariesproc",
    "movieproc",
]

# Friendly display names
SERVICE_LABELS = {
    "panel":               "Panel (Lifecycle)",
    "automouse_series":    "Auto Mouse (Series)",
    "autoharbor_series":   "Auto Harbor (Series)",
    "autorouter_series":   "Auto Router (Series)",
    "structpilot_series":  "Struct Pilot (Series)",
    "automouse_movies":    "Auto Mouse (Movies)",
    "autoharbor_movies":   "Auto Harbor (Movies)",
    "autorouter_movies":   "Auto Router (Movies)",
    "structpilot_movies":  "Struct Pilot (Movies)",
    "contentclassifier":   "Content Classifier",
    "tvproc":              "TV Processor",
    "cartoonsproc":        "Cartoons Processor",
    "animeproc":           "Anime Processor",
    "realityproc":         "Reality Processor",
    "talkshowproc":        "Talk Show Processor",
    "documentariesproc":   "Docs Processor",
    "movieproc":           "Movie Processor",
}

# Pipeline groups — merged views showing the complete flow
PIPELINE_GROUPS = {
    "pipeline_all": {
        "label": "All Services (combined)",
        "services": [
            "panel",
            "automouse_series", "autoharbor_series", "autorouter_series", "structpilot_series",
            "automouse_movies", "autoharbor_movies", "autorouter_movies", "structpilot_movies",
            "contentclassifier", "tvproc", "cartoonsproc", "animeproc",
            "realityproc", "talkshowproc", "documentariesproc",
            "movieproc",
        ],
    },
    "pipeline_series": {
        "label": "Series Pipeline (full flow)",
        "services": [
            "panel",
            "automouse_series", "autoharbor_series", "autorouter_series", "structpilot_series",
            "contentclassifier", "tvproc", "cartoonsproc", "animeproc",
            "realityproc", "talkshowproc", "documentariesproc",
        ],
    },
    "pipeline_movies": {
        "label": "Movie Pipeline (full flow)",
        "services": [
            "panel",
            "automouse_movies", "autoharbor_movies", "autorouter_movies", "structpilot_movies",
            "movieproc",
        ],
    },
}

# Regex to parse timestamp from log lines: "2026-02-15 02:04:32 | INFO  | ..."
_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')

# Regex to parse timestamp from log filenames: "animeproc_2026-02-15_02-22-23.log"
_FILENAME_TS_RE = re.compile(r'_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.log$')

# Session clustering window (seconds) — logs started within this window = same session
_SESSION_WINDOW = 120


class LogTailer:
    def __init__(self, log_dir: Path):
        self.log_dir = Path(log_dir)
        self._running = False
        self._thread = None
        self._positions = {}  # service -> file position

    def get_available_logs(self) -> dict:
        """Get ordered dict of available log sources (services + pipelines)."""
        logs = {}
        if not self.log_dir.exists():
            return logs

        # Find which services actually have log files
        active_services = set()
        for f in self.log_dir.iterdir():
            if not f.name.endswith('.log'):
                continue
            for known in SERVICE_LOG_NAMES:
                if f.name.startswith(known + '_'):
                    active_services.add(known)
                    break

        # Build ordered result: pipelines first, then individual services
        for pid, pinfo in PIPELINE_GROUPS.items():
            # Only show pipeline if at least some of its services have logs
            if active_services & set(pinfo["services"]):
                logs[pid] = {"label": pinfo["label"], "type": "pipeline"}

        for svc in SERVICE_LOG_NAMES:
            if svc in active_services:
                logs[svc] = {
                    "label": SERVICE_LABELS.get(svc, svc),
                    "type": "service",
                }

        return logs

    def get_latest_log(self, service_name: str) -> Path | None:
        """Find the most recent log file for a service."""
        if not self.log_dir.exists():
            return None
        matches = sorted(
            [f for f in self.log_dir.iterdir()
             if f.name.startswith(service_name + '_') and f.name.endswith('.log')],
            key=lambda x: x.name,
            reverse=True
        )
        return matches[0] if matches else None

    def read_tail(self, service_name: str, lines: int = 5000) -> list:
        """Read the last N lines from the latest log file for a single service."""
        log_file = self.get_latest_log(service_name)
        if not log_file or not log_file.exists():
            return []
        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                all_lines = f.readlines()
            return [line.rstrip('\n') for line in all_lines[-lines:]]
        except OSError:
            return []

    def read_pipeline_tail(self, pipeline_id: str, lines: int = 10000) -> list:
        """Read merged + sorted log lines from all services in a pipeline."""
        pinfo = PIPELINE_GROUPS.get(pipeline_id)
        if not pinfo:
            return []

        tagged_lines = []
        for svc in pinfo["services"]:
            svc_lines = self.read_tail(svc, lines=2000)
            short_label = svc
            for raw_line in svc_lines:
                tagged_lines.append((raw_line, short_label))

        # Sort by timestamp (lines without timestamps go to end)
        def sort_key(item):
            m = _TS_RE.match(item[0])
            return m.group(1) if m else "9999"

        tagged_lines.sort(key=sort_key)

        # Format: prepend service tag to each line
        result = []
        for raw_line, svc in tagged_lines[-lines:]:
            # Insert service tag after timestamp: "2026... | INFO | [svc] message"
            m = _TS_RE.match(raw_line)
            if m:
                ts_end = m.end()
                result.append(f"{raw_line[:ts_end]} [{svc}] {raw_line[ts_end:].lstrip(' |')}")
            else:
                result.append(f"[{svc}] {raw_line}")

        return result

    def read_logs_for(self, name: str, lines: int = 10000) -> list:
        """Read logs — auto-detects pipeline vs single service."""
        if name in PIPELINE_GROUPS:
            return self.read_pipeline_tail(name, lines=lines)
        return self.read_tail(name, lines=lines)

    # ─── SESSION HISTORY ───

    def _parse_log_files(self) -> list[dict]:
        """Parse all log files into (service, datetime, path) entries."""
        if not self.log_dir.exists():
            return []
        entries = []
        for f in self.log_dir.iterdir():
            if not f.name.endswith('.log'):
                continue
            m = _FILENAME_TS_RE.search(f.name)
            if not m:
                continue
            # Identify service
            svc = None
            for known in SERVICE_LOG_NAMES:
                if f.name.startswith(known + '_'):
                    svc = known
                    break
            if not svc:
                continue
            # Parse datetime
            date_str = m.group(1)
            time_str = m.group(2).replace('-', ':')
            try:
                dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            entries.append({"service": svc, "dt": dt, "path": f, "size": f.stat().st_size})
        return entries

    def get_sessions(self) -> list[dict]:
        """Detect sessions by clustering log files by creation time.
        Returns list of sessions, newest first."""
        entries = self._parse_log_files()
        if not entries:
            return []

        # Sort by datetime
        entries.sort(key=lambda e: e["dt"])

        # Cluster: group files within _SESSION_WINDOW seconds
        sessions = []
        current_cluster = [entries[0]]

        for entry in entries[1:]:
            # Compare to the earliest file in current cluster
            delta = (entry["dt"] - current_cluster[0]["dt"]).total_seconds()
            if delta <= _SESSION_WINDOW:
                current_cluster.append(entry)
            else:
                sessions.append(current_cluster)
                current_cluster = [entry]
        sessions.append(current_cluster)

        # Build session summaries
        result = []
        for i, cluster in enumerate(reversed(sessions)):
            services = sorted(set(e["service"] for e in cluster))
            start_dt = min(e["dt"] for e in cluster)
            total_size = sum(e["size"] for e in cluster)
            total_lines = 0
            for e in cluster:
                try:
                    with open(e["path"], 'r', encoding='utf-8', errors='replace') as f:
                        total_lines += sum(1 for _ in f)
                except OSError:
                    pass

            # Session ID = timestamp of earliest file
            session_id = start_dt.strftime("%Y-%m-%d_%H-%M-%S")

            # Activity summary: count key events by scanning log content
            imports = 0
            errors = 0
            classified = 0
            last_activity = start_dt
            for e in cluster:
                try:
                    with open(e["path"], 'r', encoding='utf-8', errors='replace') as f:
                        for line in f:
                            if 'Library Import:' in line:
                                imports += 1
                            elif 'ERROR' in line:
                                errors += 1
                            elif 'Routed:' in line or 'Cache hit:' in line:
                                classified += 1
                            # Track last timestamp for duration
                            tm = _TS_RE.match(line)
                            if tm:
                                try:
                                    ldt = datetime.strptime(tm.group(1), "%Y-%m-%d %H:%M:%S")
                                    if ldt > last_activity:
                                        last_activity = ldt
                                except ValueError:
                                    pass
                except OSError:
                    pass

            duration_min = max(1, int((last_activity - start_dt).total_seconds() / 60))

            result.append({
                "id": session_id,
                "started": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "services": services,
                "service_labels": [SERVICE_LABELS.get(s, s) for s in services],
                "file_count": len(cluster),
                "total_lines": total_lines,
                "size_kb": round(total_size / 1024, 1),
                "is_latest": (i == 0),
                "imports": imports,
                "errors": errors,
                "classified": classified,
                "duration_min": duration_min,
            })
        return result

    def read_session(self, session_id: str, lines: int = 15000) -> list:
        """Read merged logs for a specific session."""
        entries = self._parse_log_files()
        if not entries:
            return []

        # Parse session_id back to datetime
        try:
            session_dt = datetime.strptime(session_id, "%Y-%m-%d_%H-%M-%S")
        except ValueError:
            return []

        # Find files in this session (within clustering window)
        session_files = []
        for e in entries:
            delta = abs((e["dt"] - session_dt).total_seconds())
            if delta <= _SESSION_WINDOW:
                session_files.append(e)

        if not session_files:
            return []

        # Read and merge all files with service tags
        tagged_lines = []
        for entry in session_files:
            svc = entry["service"]
            try:
                with open(entry["path"], 'r', encoding='utf-8', errors='replace') as f:
                    for line in f:
                        tagged_lines.append((line.rstrip('\n'), svc))
            except OSError:
                continue

        # Sort by timestamp
        def sort_key(item):
            m = _TS_RE.match(item[0])
            return m.group(1) if m else "9999"

        tagged_lines.sort(key=sort_key)

        # Format with service tags + idle gap markers
        result = []
        prev_dt = None
        for raw_line, svc in tagged_lines[-lines:]:
            m = _TS_RE.match(raw_line)
            if m:
                ts_end = m.end()
                # Insert idle gap marker if > 2 minutes between entries
                try:
                    cur_dt = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
                    if prev_dt and (cur_dt - prev_dt).total_seconds() > 120:
                        gap_min = int((cur_dt - prev_dt).total_seconds() / 60)
                        result.append(f"──── idle {gap_min}min ────")
                    prev_dt = cur_dt
                except ValueError:
                    pass
                result.append(f"{raw_line[:ts_end]} [{svc}] {raw_line[ts_end:].lstrip(' |')}")
            else:
                result.append(f"[{svc}] {raw_line}")

        return result

    def start_streaming(self, socketio):
        """Start background thread that tails all active log files."""
        self._running = True
        self._thread = threading.Thread(
            target=self._stream_loop, args=(socketio,), daemon=True
        )
        self._thread.start()

    def stop_streaming(self):
        self._running = False

    def _stream_loop(self, socketio):
        while self._running:
            for svc_name in SERVICE_LOG_NAMES:
                log_file = self.get_latest_log(svc_name)
                if not log_file or not log_file.exists():
                    continue
                try:
                    size = log_file.stat().st_size
                    last_pos = self._positions.get(svc_name, 0)

                    # If file rotated (smaller than last position), reset
                    if size < last_pos:
                        last_pos = 0

                    if size > last_pos:
                        with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                            f.seek(last_pos)
                            new_lines = f.readlines()
                            self._positions[svc_name] = f.tell()

                        if new_lines:
                            clean = [l.rstrip('\n') for l in new_lines]

                            # Emit for the individual service
                            socketio.emit('log_lines', {
                                'service': svc_name,
                                'lines': clean,
                            }, namespace='/')

                            # Also emit for any pipeline that includes this service
                            for pid, pinfo in PIPELINE_GROUPS.items():
                                if svc_name in pinfo["services"]:
                                    tagged = []
                                    for line in clean:
                                        m = _TS_RE.match(line)
                                        if m:
                                            ts_end = m.end()
                                            tagged.append(f"{line[:ts_end]} [{svc_name}] {line[ts_end:].lstrip(' |')}")
                                        else:
                                            tagged.append(f"[{svc_name}] {line}")
                                    socketio.emit('log_lines', {
                                        'service': pid,
                                        'lines': tagged,
                                    }, namespace='/')
                except OSError:
                    pass
            time.sleep(1)
