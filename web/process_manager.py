#!/usr/bin/env python3
"""
process_manager.py -- Service lifecycle management (start/stop/restart).
Replaces mymediamanager.sh when using the web panel.
"""

import os
import sys
import signal
import subprocess
import threading
import time
import logging
import glob
from pathlib import Path
from datetime import datetime

try:
    import psutil
except ImportError:
    psutil = None


# Project root (MyMediaManager/)
ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "bin"


def _setup_panel_logger(log_dir: Path, retention: int = 5) -> logging.Logger:
    """Create a panel logger that writes lifecycle events to logs/panel_<ts>.log."""
    log_dir.mkdir(parents=True, exist_ok=True)

    # Clean up old panel logs
    pattern = str(log_dir / "panel_*.log")
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    while len(files) > retention:
        try:
            os.remove(files.pop(0))
        except OSError:
            pass

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"panel_{timestamp}.log"

    logger = logging.getLogger("mmm_panel")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    # Remove old handlers if reinitializing
    for h in logger.handlers[:]:
        logger.removeHandler(h)

    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-5s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("--- SESSION START: panel ---")
    logger.info("Web panel lifecycle logger initialized")
    return logger

SERVICE_REGISTRY = [
    # Series Pipeline
    {"id": "automouse_series",         "script": "automouse.py",         "args": ["--mode", "series"],     "group": "series",  "label": "AutoMouse (Series)"},
    {"id": "autoharbor_series",        "script": "autoharbor.py",        "args": ["--mode", "series"],     "group": "series",  "label": "AutoHarbor (Series)"},
    {"id": "autorouter_series",        "script": "autorouter.py",        "args": ["--mode", "series"],     "group": "series",  "label": "AutoRouter (Series)"},
    {"id": "structpilot_series",       "script": "structpilot.py",       "args": ["--mode", "series"],     "group": "series",  "label": "StructPilot (Series)"},
    {"id": "contentclassifier",        "script": "contentclassifier.py", "args": ["--mode", "series"],     "group": "series",  "label": "ContentClassifier"},
    # Movie Pipeline
    {"id": "automouse_movies",         "script": "automouse.py",         "args": ["--mode", "movies"],     "group": "movies",  "label": "AutoMouse (Movies)"},
    {"id": "autoharbor_movies",        "script": "autoharbor.py",        "args": ["--mode", "movies"],     "group": "movies",  "label": "AutoHarbor (Movies)"},
    {"id": "autorouter_movies",        "script": "autorouter.py",        "args": ["--mode", "movies"],     "group": "movies",  "label": "AutoRouter (Movies)"},
    {"id": "structpilot_movies",       "script": "structpilot.py",       "args": ["--mode", "movies"],     "group": "movies",  "label": "StructPilot (Movies)"},
    {"id": "movieprocessor",           "script": "movieprocessor.py",    "args": ["--mode", "movies"],     "group": "movies",  "label": "MovieProcessor"},
    # Final Processors (part of series pipeline)
    {"id": "seriesprocessor_tv",       "script": "seriesprocessor.py",   "args": ["--type", "tv"],         "group": "series",  "label": "TV Processor"},
    {"id": "seriesprocessor_cartoons", "script": "seriesprocessor.py",   "args": ["--type", "cartoons"],   "group": "series",  "label": "Cartoon Processor"},
    {"id": "animeprocessor",           "script": "animeprocessor.py",    "args": [],                       "group": "series",  "label": "Anime Processor"},
    {"id": "seriesprocessor_reality",       "script": "seriesprocessor.py",   "args": ["--type", "reality"],       "group": "series",  "label": "Reality Processor"},
    {"id": "seriesprocessor_talkshow",      "script": "seriesprocessor.py",   "args": ["--type", "talkshow"],      "group": "series",  "label": "Talk Show Processor"},
    {"id": "seriesprocessor_documentaries", "script": "seriesprocessor.py",   "args": ["--type", "documentaries"], "group": "series",  "label": "Docs Processor"},
]


# Required API keys per service (services not listed need no API keys)
SERVICE_API_REQUIREMENTS = {
    "contentclassifier":        ["tvdb", "mal"],
    "seriesprocessor_tv":       ["tvdb"],
    "seriesprocessor_cartoons": ["tvdb"],
    "animeprocessor":           ["mal"],
    "movieprocessor":           ["tmdb"],
    "seriesprocessor_reality":       ["tvdb"],
    "seriesprocessor_talkshow":      ["tvdb"],
    "seriesprocessor_documentaries": ["tvdb"],
}


class ProcessManager:
    def __init__(self, config, config_mgr=None):
        self.config = config
        self.config_mgr = config_mgr
        self._processes = {}  # service_id -> {process, started_at, ...}
        self._lock = threading.Lock()
        self._monitor_thread = None
        self._running = False
        # Panel logger for lifecycle events
        log_dir = Path(config.get('logging', {}).get('path', str(ROOT / 'logs')))
        retention = config.get('logging', {}).get('retention_sessions', 5)
        self._log = _setup_panel_logger(log_dir, retention)

    def _fresh_config(self):
        """Re-read config from disk so we pick up Settings changes without restart."""
        if self.config_mgr:
            return self.config_mgr.read()
        return self.config

    def init_infrastructure(self):
        """Create all required folders. Returns list of errors (empty = success)."""
        cfg = self._fresh_config()
        paths = cfg.get('paths', {})
        roots = paths.get('roots', {})
        manager_root = Path(roots.get('manager', str(ROOT)))
        data_root_str = roots.get('data', '')
        library_root_str = roots.get('library', '')

        # Check if roots are configured
        errors = []
        if not data_root_str:
            errors.append("Data root path is not configured. Go to Settings > Paths and set it.")
        if not library_root_str:
            errors.append("Library root path is not configured. Go to Settings > Paths and set it.")
        if errors:
            return errors

        data_root = Path(data_root_str)
        library_root = Path(library_root_str)

        # Check if root dirs exist and are writable
        import tempfile
        for label, root in [("Data root", data_root), ("Library root", library_root)]:
            if not root.exists():
                errors.append(f"{label} ({root}) does not exist.")
            else:
                # Actually try writing instead of os.access (which ignores supplementary groups)
                try:
                    fd, tmp = tempfile.mkstemp(dir=str(root), prefix='.mmm_check_')
                    os.close(fd)
                    os.unlink(tmp)
                except OSError:
                    errors.append(f"{label} ({root}) is not writable.")
        if errors:
            errors.append("Restart the container to fix permissions: docker compose restart")
            return errors

        dirs = [
            # Input drops (data root -- same drive as pipeline)
            data_root / "Drop_Shows",
            data_root / "Drop_Movies",
            data_root / "Trash",
            data_root / "Review" / "Shows",
            data_root / "Review" / "Movies",
            data_root / "Duplicates" / "Shows",
            data_root / "Duplicates" / "Movies",
            # Series pipeline (data root - hidden with dot prefix)
            data_root / ".Work" / "Shows" / "Intake",
            data_root / ".Work" / "Shows" / "Processing",
            data_root / ".Work" / "Shows" / "Failed",
            data_root / ".Work" / "Shows" / "Staged" / "Identify",
            data_root / ".Work" / "Shows" / "Staged" / "TV_Shows",
            data_root / ".Work" / "Shows" / "Staged" / "Cartoons",
            data_root / ".Work" / "Shows" / "Staged" / "Anime",
            data_root / ".Work" / "Shows" / "Staged" / "Reality",
            data_root / ".Work" / "Shows" / "Staged" / "TalkShows",
            data_root / ".Work" / "Shows" / "Staged" / "Documentaries",
            # Movie pipeline (data root)
            data_root / ".Work" / "Movies" / "Intake",
            data_root / ".Work" / "Movies" / "Processing",
            data_root / ".Work" / "Movies" / "Failed",
            data_root / ".Work" / "Movies" / "Staged",
            # Libraries (library root -- same drive as data root)
            library_root / "TV Shows",
            library_root / "Movies",
            library_root / "Anime" / "Shows",
            library_root / "Anime" / "Movies",
            library_root / "Cartoons",
            library_root / "Reality TV",
            library_root / "Talk Shows",
            library_root / "Documentaries" / "Series",
            library_root / "Documentaries" / "Movies",
            library_root / "Stand-Up",
            # System
            ROOT / "logs",
            ROOT / "cache" / "tv",
            ROOT / "cache" / "anime",
            ROOT / "cache" / "movies",
            ROOT / "cache" / "cartoons",
            ROOT / "cache" / "classifier",
            ROOT / "cache" / "reality",
            ROOT / "cache" / "talkshow",
            ROOT / "cache" / "documentaries",
        ]
        for d in dirs:
            try:
                d.mkdir(parents=True, exist_ok=True)
            except PermissionError:
                errors.append(f"Cannot create {d}")
        if errors:
            errors.append("Restart the container to fix permissions: docker compose restart")
        return errors

    def log(self, message: str, level: str = "info"):
        """Write a lifecycle event to the panel log."""
        getattr(self._log, level, self._log.info)(message)

    def _label(self, service_id: str) -> str:
        """Get friendly label for a service ID."""
        svc = self._find_service(service_id)
        return svc['label'] if svc else service_id

    def start(self, service_id: str) -> dict:
        """Start a single service. Returns status dict."""
        svc = self._find_service(service_id)
        if not svc:
            return {"error": f"Unknown service: {service_id}"}

        with self._lock:
            if service_id in self._processes:
                info = self._processes[service_id]
                if info['process'].poll() is None:
                    return {"status": "already_running", "pid": info['process'].pid}

        script_path = BIN / svc['script']
        if not script_path.exists():
            self._log.error(f"Script not found: {script_path}")
            return {"error": f"Script not found: {script_path}"}

        python_exe = sys.executable
        cmd = [python_exe, str(script_path)] + svc['args']

        try:
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except Exception as e:
            self._log.error(f"Failed to start {svc['label']}: {e}")
            return {"error": str(e)}

        with self._lock:
            self._processes[service_id] = {
                'process': proc,
                'started_at': datetime.now().isoformat(),
                'service': svc,
            }
        self._log.info(f"Started {svc['label']} (PID {proc.pid})")
        return {"status": "started", "pid": proc.pid}

    def stop(self, service_id: str) -> dict:
        """Stop a single service gracefully."""
        label = self._label(service_id)
        with self._lock:
            info = self._processes.get(service_id)
            if not info:
                return {"status": "not_running"}
            proc = info['process']

        pid = proc.pid
        if proc.poll() is not None:
            with self._lock:
                self._processes.pop(service_id, None)
            return {"status": "already_stopped"}

        # Graceful: SIGTERM (or terminate on Windows)
        try:
            if sys.platform == 'win32':
                proc.terminate()
            else:
                proc.send_signal(signal.SIGTERM)
        except OSError:
            pass

        # Wait up to 5 seconds
        forced = False
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=3)
            forced = True

        with self._lock:
            self._processes.pop(service_id, None)
        if forced:
            self._log.warning(f"Force-killed {label} (PID {pid}) after timeout")
        else:
            self._log.info(f"Stopped {label} (PID {pid})")
        return {"status": "stopped"}

    def restart(self, service_id: str) -> dict:
        label = self._label(service_id)
        self._log.info(f"Restarting {label}...")
        self.stop(service_id)
        time.sleep(0.5)
        return self.start(service_id)

    def start_all(self) -> list:
        self._log.info("=== Starting all services ===")
        self.init_infrastructure()
        results = []
        for svc in SERVICE_REGISTRY:
            result = self.start(svc['id'])
            result['service_id'] = svc['id']
            results.append(result)
            time.sleep(0.3)
        started = sum(1 for r in results if r.get('status') == 'started')
        self._log.info(f"=== Start all complete: {started}/{len(results)} services started ===")
        return results

    def stop_all(self) -> list:
        self._log.info("=== Stopping all services ===")
        results = []
        for svc in SERVICE_REGISTRY:
            result = self.stop(svc['id'])
            result['service_id'] = svc['id']
            results.append(result)
        stopped = sum(1 for r in results if r.get('status') in ('stopped', 'not_running', 'already_stopped'))
        self._log.info(f"=== Stop all complete: {stopped}/{len(results)} services stopped ===")
        return results

    def restart_all(self) -> list:
        self._log.info("=== Restarting all services ===")
        self.stop_all()
        time.sleep(1)
        return self.start_all()

    def start_group(self, group: str) -> list:
        self._log.info(f"Starting {group} group...")
        results = []
        for svc in SERVICE_REGISTRY:
            if svc['group'] == group:
                result = self.start(svc['id'])
                result['service_id'] = svc['id']
                results.append(result)
                time.sleep(0.3)
        return results

    def stop_group(self, group: str) -> list:
        self._log.info(f"Stopping {group} group...")
        results = []
        for svc in SERVICE_REGISTRY:
            if svc['group'] == group:
                result = self.stop(svc['id'])
                result['service_id'] = svc['id']
                results.append(result)
        return results

    def get_status(self) -> list:
        """Get status of all services."""
        statuses = []
        for svc in SERVICE_REGISTRY:
            info = self._get_service_info(svc['id'])
            info['id'] = svc['id']
            info['label'] = svc['label']
            info['group'] = svc['group']
            statuses.append(info)
        return statuses

    def get_service_status(self, service_id: str) -> dict:
        info = self._get_service_info(service_id)
        svc = self._find_service(service_id)
        if svc:
            info['id'] = svc['id']
            info['label'] = svc['label']
            info['group'] = svc['group']
        return info

    def _get_service_info(self, service_id: str) -> dict:
        with self._lock:
            info = self._processes.get(service_id)

        if not info:
            return {"status": "stopped", "pid": None}

        proc = info['process']
        if proc.poll() is not None:
            exit_code = proc.returncode
            with self._lock:
                self._processes.pop(service_id, None)
            label = self._label(service_id)
            self._log.warning(f"{label} crashed (exit code {exit_code})")
            return {"status": "crashed", "pid": None, "exit_code": exit_code}

        result = {
            "status": "running",
            "pid": proc.pid,
            "started_at": info['started_at'],
        }

        # Enrich with psutil data if available
        if psutil:
            try:
                p = psutil.Process(proc.pid)
                result["cpu_percent"] = p.cpu_percent(interval=0)
                result["memory_mb"] = round(p.memory_info().rss / (1024 * 1024), 1)
                result["uptime_seconds"] = int(time.time() - p.create_time())
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return result

    def _find_service(self, service_id: str) -> dict | None:
        for svc in SERVICE_REGISTRY:
            if svc['id'] == service_id:
                return svc
        return None

    def start_monitor(self, socketio):
        """Start background thread that monitors process health."""
        self._running = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, args=(socketio,), daemon=True
        )
        self._monitor_thread.start()

    def stop_monitor(self):
        self._running = False

    # ── Auto start/stop ──
    _auto_idle_since = None  # Timestamp when pipeline first became idle

    def auto_manage(self, snapshot: dict):
        """Auto-start services when files appear, auto-stop when idle.

        Called every 3s by pipeline monitor. Reads auto_services config.
        """
        cfg = self._fresh_config()
        auto_cfg = cfg.get('web_panel', {}).get('auto_services', {})
        if not auto_cfg.get('enabled', False):
            self._auto_idle_since = None
            return

        idle_minutes = auto_cfg.get('idle_minutes', 5)

        # Check if any pipeline working dirs have files (excluding drop, review, duplicates, library)
        pipeline_stages = []
        for sid, data in (snapshot.get('series', {}) | snapshot.get('movies', {})).items():
            if sid.startswith('drop_'):
                continue
            pipeline_stages.append(data)

        pipeline_busy = any(s.get('count', 0) > 0 for s in pipeline_stages)

        # Check if drop folders have files
        series_drop = snapshot.get('series', {}).get('drop_shows', {})
        movies_drop = snapshot.get('movies', {}).get('drop_movies', {})
        has_drops = (series_drop.get('count', 0) > 0 or movies_drop.get('count', 0) > 0)

        # Determine which groups have running services
        any_running = any(
            sid in self._processes and self._processes[sid]['process'].poll() is None
            for sid in [s['id'] for s in SERVICE_REGISTRY]
        )

        # Auto-start: drop folders have files but services stopped
        if has_drops and not any_running:
            self._log.info("[Auto] Files detected in drop folders, starting services...")
            # Start the relevant group(s)
            if series_drop.get('count', 0) > 0:
                self.init_infrastructure()
                self.start_group('series')
            if movies_drop.get('count', 0) > 0:
                self.init_infrastructure()
                self.start_group('movies')
            self._auto_idle_since = None
            return

        # Auto-stop: all pipeline dirs empty + no drops for idle_minutes
        if any_running and not pipeline_busy and not has_drops:
            if self._auto_idle_since is None:
                self._auto_idle_since = time.time()
            elif time.time() - self._auto_idle_since >= idle_minutes * 60:
                self._log.info(f"[Auto] Pipeline idle for {idle_minutes}min, stopping services...")
                self.stop_all()
                self._auto_idle_since = None
        else:
            self._auto_idle_since = None

    def _monitor_loop(self, socketio):
        while self._running:
            statuses = self.get_status()
            socketio.emit('service_status', statuses, namespace='/')
            time.sleep(3)

    def shutdown(self):
        """Clean shutdown: stop monitor, stop all services."""
        self.stop_monitor()
        self.stop_all()
