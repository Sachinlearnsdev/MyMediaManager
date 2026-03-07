#!/usr/bin/env python3
"""
Web Control Panel for MyMediaManager.
Launch: python webpanel.py
"""

import sys
import os
import signal
import atexit
from pathlib import Path

# Ensure project root is in path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# Lockfile to prevent multiple instances
LOCK_FILE = ROOT / "cache" / ".panel_lock"


def acquire_lock():
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            import psutil
            if psutil.pid_exists(pid) and pid != os.getpid():
                print(f"[ERROR] Another instance is running (PID {pid})")
                print(f"  If this is incorrect, delete: {LOCK_FILE}")
                sys.exit(1)
        except (ValueError, ImportError):
            pass
    LOCK_FILE.write_text(str(os.getpid()))


def release_lock():
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def cleanup_shutdown():
    """Stop all pipeline services and release resources on exit."""
    try:
        from web.app import pm
        if pm:
            print("[MyMediaManager] Stopping all services...")
            pm.stop_all()
    except Exception:
        pass
    release_lock()


def main():
    acquire_lock()

    atexit.register(cleanup_shutdown)

    def signal_handler(signum, frame):
        print(f"\n[MyMediaManager] Received signal {signum}, shutting down...")
        cleanup_shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        from web.app import create_app, socketio

        app = create_app()
        cfg = app.config.get('MMM_PANEL', {})
        host = cfg.get('host', '0.0.0.0')
        port = cfg.get('port', 8888)

        print(f"[MyMediaManager] Web Panel starting on http://{host}:{port}")
        socketio.run(app, host=host, port=port, debug=False)
    except OSError as e:
        if 'Address already in use' in str(e) or 'address already in use' in str(e):
            print(f"[ERROR] Port {port} is already in use.")
            print(f"  Find what's using it: sudo lsof -i :{port}")
            print(f"  Kill it:              sudo kill $(sudo lsof -t -i:{port})")
            sys.exit(1)
        raise
    except KeyboardInterrupt:
        print("\n[MyMediaManager] Shutting down...")
    finally:
        cleanup_shutdown()


if __name__ == "__main__":
    main()
