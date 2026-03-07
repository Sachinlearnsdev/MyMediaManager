#!/usr/bin/env python3
"""
app.py -- Flask application factory for MyMediaManager Web Control Panel.
"""

from gevent import monkey
monkey.patch_all()

import os
import sys
import json
import shutil
import atexit
from pathlib import Path
import time as time_mod
from datetime import timedelta

from flask import Flask, jsonify, request, render_template, redirect, url_for, send_file
from flask_socketio import SocketIO

# Ensure project root is on path so we can import bin.common
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bin.common import load_config, BASE_DIR, CONFIG_PATH
from web.auth import auth_bp, login_required
from web.process_manager import ProcessManager
from web.pipeline_monitor import PipelineMonitor
from web.log_tailer import LogTailer
from web.config_manager import ConfigManager
from web.recovery import RecoveryManager
from web.api_stats import APIStats

socketio = SocketIO()

# Globals initialized in create_app
pm: ProcessManager = None
pipeline_mon: PipelineMonitor = None
log_tailer: LogTailer = None
config_mgr: ConfigManager = None
recovery_mgr: RecoveryManager = None
api_stats: APIStats = None


def create_app() -> Flask:
    global pm, pipeline_mon, log_tailer, config_mgr, recovery_mgr, api_stats

    app = Flask(
        __name__,
        template_folder=str(ROOT / 'web' / 'templates'),
        static_folder=str(ROOT / 'web' / 'static'),
    )

    # Load config
    cfg = load_config()
    panel_cfg = cfg.get('web_panel', {})

    app.secret_key = panel_cfg.get('secret_key', 'mmm-panel-secret-change-me')
    app.permanent_session_lifetime = timedelta(
        hours=panel_cfg.get('session_timeout_hours', 24)
    )
    app.config['MMM_CONFIG'] = cfg
    app.config['MMM_PANEL'] = panel_cfg
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.jinja_env.auto_reload = True

    # Initialize components
    config_mgr = ConfigManager(CONFIG_PATH)
    pm = ProcessManager(cfg, config_mgr)
    pipeline_mon = PipelineMonitor(cfg)
    log_dir = Path(cfg.get('logging', {}).get('path', str(BASE_DIR / 'logs')))
    log_tailer = LogTailer(log_dir)
    recovery_mgr = RecoveryManager(cfg, pm)
    api_stats = APIStats(cfg)

    # Register blueprints
    app.register_blueprint(auth_bp)

    # Make auth_enabled available in all templates
    @app.context_processor
    def inject_auth_status():
        password_hash = panel_cfg.get('password_hash', '').strip()
        return {'auth_enabled': bool(password_hash)}

    # Initialize SocketIO
    socketio.init_app(app, async_mode='gevent')

    # Suppress noisy Werkzeug request logs (SocketIO polling, static files, source maps)
    import logging

    class _QuietFilter(logging.Filter):
        def filter(self, record):
            msg = record.getMessage()
            if '/socket.io/' in msg:
                return False
            if '/static/' in msg:
                return False
            if '.map ' in msg:
                return False
            return True

    logging.getLogger('werkzeug').addFilter(_QuietFilter())

    # ──────────────────────────────────────────────
    # Page Routes
    # ──────────────────────────────────────────────

    @app.route('/')
    @login_required
    def index():
        return redirect(url_for('main.dashboard'))

    # Use a blueprint for main pages
    from flask import Blueprint
    main_bp = Blueprint('main', __name__)

    @main_bp.route('/dashboard')
    @login_required
    def dashboard():
        return render_template('dashboard.html',
                               services=pm.get_status(),
                               pipeline=pipeline_mon.get_snapshot())

    @main_bp.route('/logs')
    @login_required
    def logs():
        available = log_tailer.get_available_logs()
        retention = cfg.get('logging', {}).get('retention_sessions', 5)
        return render_template('logs.html', available_logs=available, retention=retention)

    @main_bp.route('/settings')
    @login_required
    def settings():
        raw_cfg = config_mgr.read()
        return render_template('settings.html', config=raw_cfg)

    @main_bp.route('/stats')
    @login_required
    def stats():
        return render_template('stats.html', stats=api_stats.get_stats(), config=cfg)

    @main_bp.route('/recovery')
    @login_required
    def recovery():
        stuck = recovery_mgr.get_stuck_files()
        cfg = load_config()
        return render_template('recovery.html', stuck=stuck, config=cfg)

    @main_bp.route('/library')
    @login_required
    def library():
        return render_template('library.html', config=cfg)

    @main_bp.route('/guide')
    @login_required
    def guide():
        return render_template('guide.html')

    app.register_blueprint(main_bp)

    # ──────────────────────────────────────────────
    # API Routes
    # ──────────────────────────────────────────────

    def _check_service_keys(service_id):
        """Check if required API keys are configured for a service."""
        from web.process_manager import SERVICE_API_REQUIREMENTS
        required = SERVICE_API_REQUIREMENTS.get(service_id, [])
        if not required:
            return {"ok": True, "missing": []}
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})
        missing = [k for k in required if not keys.get(k, '').strip()]
        return {"ok": len(missing) == 0, "missing": missing}

    @app.route('/api/services', methods=['GET'])
    @login_required
    def api_services():
        return jsonify(pm.get_status())

    @app.route('/api/services/<service_id>/start', methods=['POST'])
    @login_required
    def api_start_service(service_id):
        key_check = _check_service_keys(service_id)
        if not key_check['ok']:
            missing = ', '.join(k.upper() for k in key_check['missing'])
            return jsonify({"error": f"Missing required API keys: {missing}. Add them in Settings > API Keys."}), 400
        return jsonify(pm.start(service_id))

    @app.route('/api/services/<service_id>/stop', methods=['POST'])
    @login_required
    def api_stop_service(service_id):
        return jsonify(pm.stop(service_id))

    @app.route('/api/services/<service_id>/restart', methods=['POST'])
    @login_required
    def api_restart_service(service_id):
        key_check = _check_service_keys(service_id)
        if not key_check['ok']:
            missing = ', '.join(k.upper() for k in key_check['missing'])
            return jsonify({"error": f"Missing required API keys: {missing}. Add them in Settings > API Keys."}), 400
        return jsonify(pm.restart(service_id))

    @app.route('/api/services/start-all', methods=['POST'])
    @login_required
    def api_start_all():
        from web.process_manager import SERVICE_REGISTRY, SERVICE_API_REQUIREMENTS
        pm.log("=== Starting all services ===")
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})
        infra_errors = pm.init_infrastructure()
        if infra_errors:
            pm.log(f"Infrastructure errors: {len(infra_errors)} issues", "error")
            return jsonify({"error": "infrastructure", "issues": infra_errors}), 400
        results = []
        for svc in SERVICE_REGISTRY:
            required = SERVICE_API_REQUIREMENTS.get(svc['id'], [])
            missing = [k for k in required if not keys.get(k, '').strip()]
            if missing:
                pm.log(f"Blocked {svc['label']}: missing API keys ({', '.join(k.upper() for k in missing)})", "warning")
                results.append({
                    "service_id": svc['id'],
                    "status": "blocked",
                    "error": f"Missing: {', '.join(k.upper() for k in missing)}"
                })
            else:
                result = pm.start(svc['id'])
                result['service_id'] = svc['id']
                results.append(result)
                time_mod.sleep(0.3)
        started = sum(1 for r in results if r.get('status') == 'started')
        blocked = sum(1 for r in results if r.get('status') == 'blocked')
        pm.log(f"=== Start all complete: {started} started, {blocked} blocked ===")
        return jsonify(results)

    @app.route('/api/services/stop-all', methods=['POST'])
    @login_required
    def api_stop_all():
        return jsonify(pm.stop_all())

    @app.route('/api/services/restart-all', methods=['POST'])
    @login_required
    def api_restart_all():
        from web.process_manager import SERVICE_REGISTRY, SERVICE_API_REQUIREMENTS
        pm.log("=== Restarting all services ===")
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})
        pm.stop_all()
        time_mod.sleep(1)
        infra_errors = pm.init_infrastructure()
        if infra_errors:
            pm.log(f"Infrastructure errors: {len(infra_errors)} issues", "error")
            return jsonify({"error": "infrastructure", "issues": infra_errors}), 400
        results = []
        for svc in SERVICE_REGISTRY:
            required = SERVICE_API_REQUIREMENTS.get(svc['id'], [])
            missing = [k for k in required if not keys.get(k, '').strip()]
            if missing:
                pm.log(f"Blocked {svc['label']}: missing API keys ({', '.join(k.upper() for k in missing)})", "warning")
                results.append({
                    "service_id": svc['id'],
                    "status": "blocked",
                    "error": f"Missing: {', '.join(k.upper() for k in missing)}"
                })
            else:
                result = pm.start(svc['id'])
                result['service_id'] = svc['id']
                results.append(result)
                time_mod.sleep(0.3)
        started = sum(1 for r in results if r.get('status') == 'started')
        pm.log(f"=== Restart all complete: {started}/{len(results)} services started ===")
        return jsonify(results)

    @app.route('/api/services/group/<group>/start', methods=['POST'])
    @login_required
    def api_start_group(group):
        from web.process_manager import SERVICE_REGISTRY, SERVICE_API_REQUIREMENTS
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})
        results = []
        for svc in SERVICE_REGISTRY:
            if svc['group'] != group:
                continue
            required = SERVICE_API_REQUIREMENTS.get(svc['id'], [])
            missing = [k for k in required if not keys.get(k, '').strip()]
            if missing:
                results.append({
                    "service_id": svc['id'],
                    "status": "blocked",
                    "error": f"Missing: {', '.join(k.upper() for k in missing)}"
                })
            else:
                result = pm.start(svc['id'])
                result['service_id'] = svc['id']
                results.append(result)
                time_mod.sleep(0.3)
        return jsonify(results)

    @app.route('/api/services/group/<group>/stop', methods=['POST'])
    @login_required
    def api_stop_group(group):
        return jsonify(pm.stop_group(group))

    @app.route('/api/pipeline', methods=['GET'])
    @login_required
    def api_pipeline():
        return jsonify(pipeline_mon.get_snapshot())

    @app.route('/api/logs/<service_name>', methods=['GET'])
    @login_required
    def api_logs(service_name):
        lines = request.args.get('lines', 200, type=int)
        return jsonify({"lines": log_tailer.read_logs_for(service_name, lines)})

    @app.route('/api/logs', methods=['GET'])
    @login_required
    def api_logs_available():
        return jsonify(log_tailer.get_available_logs())

    @app.route('/api/sessions', methods=['GET'])
    @login_required
    def api_sessions():
        return jsonify(log_tailer.get_sessions())

    @app.route('/api/sessions/<session_id>', methods=['GET'])
    @login_required
    def api_session_logs(session_id):
        lines = request.args.get('lines', 15000, type=int)
        return jsonify({"lines": log_tailer.read_session(session_id, lines)})

    @app.route('/api/config', methods=['GET'])
    @login_required
    def api_config_get():
        return jsonify(config_mgr.read())

    @app.route('/api/config', methods=['POST'])
    @login_required
    def api_config_save():
        try:
            new_cfg = request.get_json()
            config_mgr.write(new_cfg)
            return jsonify({"status": "saved"})
        except Exception as e:
            return jsonify({"error": str(e)}), 400

    @app.route('/api/restart-panel', methods=['POST'])
    @login_required
    def api_restart_panel():
        """Restart the web panel: update firewall rules for new port, then restart."""
        import subprocess as _sp
        try:
            cfg = config_mgr.read()
            new_port = cfg.get('web_panel', {}).get('port', 8888)
            old_port = request.host.split(':')[-1] if ':' in request.host else '8888'

            # Update ufw firewall rules (Linux only, ignore errors on non-ufw systems)
            try:
                _sp.run(['ufw', 'allow', f'{new_port}/tcp'], capture_output=True, timeout=5)
                if str(old_port) != str(new_port):
                    _sp.run(['ufw', 'delete', 'allow', f'{old_port}/tcp'], capture_output=True, timeout=5)
            except (FileNotFoundError, _sp.TimeoutExpired):
                pass  # ufw not installed or not active

            pm.log(f"Panel restart requested: port {old_port} -> {new_port}")

            # Schedule restart after response is sent
            import threading
            def _do_restart():
                import time, os, signal
                time.sleep(1.5)
                os.kill(os.getpid(), signal.SIGTERM)
            threading.Thread(target=_do_restart, daemon=True).start()

            return jsonify({"status": "restarting", "port": new_port})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/api/stats', methods=['GET'])
    @login_required
    def api_stats_get():
        return jsonify(api_stats.get_stats())

    @app.route('/api/stats/cache-details', methods=['GET'])
    @login_required
    def api_cache_details():
        return jsonify(api_stats.get_cache_details())

    @app.route('/api/recovery/flush', methods=['POST'])
    @login_required
    def api_recovery_flush():
        pipeline = request.json.get('pipeline', 'both') if request.json else 'both'
        return jsonify(recovery_mgr.flush_stuck(pipeline))

    @app.route('/api/recovery/retry-failed', methods=['POST'])
    @login_required
    def api_recovery_retry_failed():
        pipeline = request.json.get('pipeline', 'both') if request.json else 'both'
        return jsonify(recovery_mgr.retry_failed(pipeline))

    @app.route('/api/recovery/retry-review', methods=['POST'])
    @login_required
    def api_recovery_retry_review():
        pipeline = request.json.get('pipeline', 'both') if request.json else 'both'
        return jsonify(recovery_mgr.retry_review(pipeline))

    @app.route('/api/recovery/nuclear', methods=['POST'])
    @login_required
    def api_recovery_nuclear():
        return jsonify(recovery_mgr.nuclear_reset())

    @app.route('/api/recovery/clear-cache', methods=['POST'])
    @login_required
    def api_recovery_clear_cache():
        cache_type = request.json.get('type', 'all') if request.json else 'all'
        return jsonify(recovery_mgr.clear_cache(cache_type))

    @app.route('/api/recovery/stuck', methods=['GET'])
    @login_required
    def api_recovery_stuck():
        return jsonify(recovery_mgr.get_stuck_files())

    @app.route('/api/recovery/clean-trash', methods=['POST'])
    @login_required
    def api_recovery_clean_trash():
        return jsonify(recovery_mgr.clean_trash())

    @app.route('/api/recovery/clean-review', methods=['POST'])
    @login_required
    def api_recovery_clean_review():
        return jsonify(recovery_mgr.clean_review())

    # ──────────────────────────────────────────────
    # Review File Manager API
    # ──────────────────────────────────────────────

    @app.route('/api/review', methods=['GET'])
    @login_required
    def api_review_list():
        """List files in Review folders with their reason sidecars."""
        import json as json_mod
        paths_cfg = cfg.get('paths', {})
        review_dirs = {
            'shows': Path(paths_cfg.get('series_pipeline', {}).get('review', '')),
            'movies': Path(paths_cfg.get('movie_pipeline', {}).get('review', '')),
        }
        files = []
        for pipeline, rdir in review_dirs.items():
            if not rdir.exists():
                continue
            for f in sorted(rdir.iterdir()):
                if not f.is_file() or f.name.endswith('.reason.json') or f.name.endswith('.ctx.json'):
                    continue
                entry = {
                    'name': f.name,
                    'pipeline': pipeline,
                    'size_mb': round(f.stat().st_size / (1024 * 1024), 1),
                    'modified': f.stat().st_mtime,
                    'reason': None,
                }
                reason_path = f.with_name(f.name + '.reason.json')
                if reason_path.exists():
                    try:
                        entry['reason'] = json_mod.loads(reason_path.read_text(encoding='utf-8'))
                    except Exception:
                        pass
                files.append(entry)
        return jsonify({'files': files})

    @app.route('/api/review/rename', methods=['POST'])
    @login_required
    def api_review_rename():
        """Rename a file in Review."""
        data = request.get_json() or {}
        old_name = data.get('old_name', '')
        new_name = data.get('new_name', '')
        pipeline = data.get('pipeline', 'shows')
        if not old_name or not new_name:
            return jsonify({'error': 'Missing old_name or new_name'}), 400
        # Prevent path traversal
        if '/' in new_name or '\\' in new_name or '..' in new_name:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        rdir = Path(paths_cfg.get(pipe_key, {}).get('review', ''))
        old_path = rdir / old_name
        new_path = rdir / new_name
        if not old_path.exists():
            return jsonify({'error': 'File not found'}), 404
        if new_path.exists():
            return jsonify({'error': 'Target name already exists'}), 409
        old_path.rename(new_path)
        # Move reason sidecar too
        old_reason = old_path.with_name(old_name + '.reason.json')
        if old_reason.exists():
            old_reason.rename(new_path.with_name(new_name + '.reason.json'))
        # Move ctx sidecar too
        old_ctx = old_path.with_name(old_name + '.ctx.json')
        if old_ctx.exists():
            old_ctx.rename(new_path.with_name(new_name + '.ctx.json'))
        return jsonify({'status': 'renamed', 'new_name': new_name})

    @app.route('/api/review/retry', methods=['POST'])
    @login_required
    def api_review_retry():
        """Retry a single file by moving it back to Processing."""
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename:
            return jsonify({'error': 'Missing name'}), 400
        paths_cfg = cfg.get('paths', {})
        if pipeline == 'shows':
            rdir = Path(paths_cfg.get('series_pipeline', {}).get('review', ''))
            dest = Path(paths_cfg.get('series_pipeline', {}).get('processing', ''))
        else:
            rdir = Path(paths_cfg.get('movie_pipeline', {}).get('review', ''))
            dest = Path(paths_cfg.get('movie_pipeline', {}).get('processing', ''))
        src = rdir / filename
        if not src.exists():
            return jsonify({'error': 'File not found'}), 404
        dest.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dest / filename))
        # Remove reason sidecar (no longer relevant after retry)
        reason = rdir / (filename + '.reason.json')
        if reason.exists():
            reason.unlink()
        # Move ctx sidecar with the file
        ctx = rdir / (filename + '.ctx.json')
        if ctx.exists():
            shutil.move(str(ctx), str(dest / (filename + '.ctx.json')))
        return jsonify({'status': 'retried', 'destination': str(dest)})

    @app.route('/api/review/trash', methods=['POST'])
    @login_required
    def api_review_trash():
        """Move a single file from Review to Trash."""
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename:
            return jsonify({'error': 'Missing name'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        rdir = Path(paths_cfg.get(pipe_key, {}).get('review', ''))
        trash = Path(paths_cfg.get('trash_root', 'Trash'))
        src = rdir / filename
        if not src.exists():
            return jsonify({'error': 'File not found'}), 404
        trash.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(trash / filename))
        # Clean up sidecars
        for sidecar in [filename + '.reason.json', filename + '.ctx.json']:
            sc = rdir / sidecar
            if sc.exists():
                sc.unlink()
        return jsonify({'status': 'trashed'})

    @app.route('/api/review/stream', methods=['GET'])
    @login_required
    def api_review_stream():
        """Stream a video file from Review for preview playback."""
        filename = request.args.get('name', '')
        pipeline = request.args.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        rdir = Path(paths_cfg.get(pipe_key, {}).get('review', ''))
        file_path = rdir / filename
        if not file_path.exists():
            return jsonify({'error': 'File not found'}), 404
        mime_map = {
            '.mp4': 'video/mp4', '.mkv': 'video/x-matroska', '.avi': 'video/x-msvideo',
            '.webm': 'video/webm', '.mov': 'video/quicktime', '.wmv': 'video/x-ms-wmv',
            '.flv': 'video/x-flv', '.m4v': 'video/mp4',
        }
        mime = mime_map.get(file_path.suffix.lower(), 'application/octet-stream')
        return send_file(str(file_path), mimetype=mime, conditional=True)

    @app.route('/api/review/open-folder', methods=['POST'])
    @login_required
    def api_review_open_folder():
        """Open the Review folder in the OS file manager with the file selected."""
        import subprocess
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        rdir = Path(paths_cfg.get(pipe_key, {}).get('review', ''))
        file_path = rdir / filename
        if not file_path.exists():
            # Fall back to opening just the folder
            file_path = rdir
        try:
            if os.name == 'nt':
                subprocess.Popen(['explorer', '/select,', str(file_path)])
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', '-R', str(file_path)])
            else:
                subprocess.Popen(['xdg-open', str(file_path.parent)])
            return jsonify({'status': 'opened'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ── Review Approve API ──────────────────────────────────
    @app.route('/api/review/approve', methods=['POST'])
    @login_required
    def api_review_approve():
        """Approve a review file: move it to its final library destination using reason.json match data."""
        import re as re_mod
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400

        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        rdir = Path(paths_cfg.get(pipe_key, {}).get('review', ''))
        src = rdir / filename
        if not src.exists():
            return jsonify({'error': 'File not found'}), 404

        # Read reason sidecar for match data
        reason_path = rdir / (filename + '.reason.json')
        if not reason_path.exists():
            return jsonify({'error': 'No reason data — cannot determine destination'}), 400
        try:
            reason_data = json.loads(reason_path.read_text(encoding='utf-8'))
        except Exception:
            return jsonify({'error': 'Could not read reason data'}), 400

        match_data = reason_data.get('match_data')
        best_match = reason_data.get('best_match')

        if not match_data and not best_match:
            return jsonify({'error': 'No match data in reason — use Retry instead'}), 400

        from bin.central_logic import CentralLogic
        brain = CentralLogic()

        try:
            if pipeline == 'movies':
                # Movie approval: build Title (Year).ext
                title = (match_data or {}).get('title') or best_match
                safe_title = brain.sanitize_for_filesystem(title)

                # Extract year from match_data, then filename fallback
                year = (match_data or {}).get('year')
                if not year:
                    _, file_year, _ = brain.get_title_candidate(filename)
                    year = file_year or '0000'
                year = str(year)[:4]

                final_name = f"{safe_title} ({year}){src.suffix}"
                output = paths_cfg.get('output', {})
                dest_root = Path(output.get('movies', 'Movies'))

                # If we have TMDB ID, fetch details for routing
                tmdb_id = (match_data or {}).get('source_id')
                if tmdb_id and (match_data or {}).get('source') == 'tmdb':
                    try:
                        from bin.media_sources import SourcePool
                        cache_root = Path(cfg.get('cache', {}).get('root', str(ROOT / 'cache')))
                        pool = SourcePool(cfg, cache_root / 'movies')
                        details = pool.get_details_for('tmdb', int(tmdb_id))
                        if details:
                            genres = [g['id'] for g in details.get('genres', [])]
                            countries = [c['iso_3166_1'] for c in details.get('production_countries', [])]
                            if 16 in genres and countries == ['JP']:
                                dest_root = Path(output.get('anime_movies', 'Anime/Movies'))
                                collection = details.get('belongs_to_collection')
                                if collection:
                                    col_name = brain.sanitize_for_filesystem(collection['name'])
                                    dest_root = dest_root / col_name
                            elif 35 in genres:
                                kw_data = details.get('keywords', {})
                                keywords = [k.get('name', '').lower() for k in kw_data.get('keywords', [])]
                                is_standup = any(
                                    any(t in kw for t in ('stand-up', 'standup', 'stand up', 'comedy special',
                                                          'comedy concert', 'comedian', 'one-man show',
                                                          'one man show', 'live comedy', 'comedy act', 'netflix special'))
                                    for kw in keywords
                                )
                                if not is_standup:
                                    runtime = details.get('runtime', 0) or 0
                                    if genres == [35] and 0 < runtime <= 80:
                                        is_standup = True
                                if is_standup:
                                    dest_root = Path(output.get('standup', 'Stand-Up'))
                            elif genres and genres[0] == 99:
                                dest_root = Path(output.get('documentaries_movies', 'Documentaries/Movies'))
                    except Exception:
                        pass  # Fallback to default Movies/

                dest_root.mkdir(parents=True, exist_ok=True)
                target = dest_root / final_name
            else:
                # Series approval: file has SxxEyy, use match title to build ShowName/Season XX/ShowName - SxxEyy.ext
                title = (match_data or {}).get('title') or best_match
                safe_title = brain.sanitize_for_filesystem(title)
                media_type = (match_data or {}).get('media_type', 'tv')

                # Parse SxxEyy from filename
                m = re_mod.match(r'^.*?[Ss](\d{1,2})[Ee](\d{1,4})', filename)
                if m:
                    season_num = int(m.group(1))
                    ep_num = int(m.group(2))
                    season_str = f"Season {season_num:02d}"
                    ep_str = f"S{season_num:02d}E{ep_num:02d}"
                    new_filename = f"{safe_title} {ep_str}{src.suffix}"
                else:
                    # No SxxEyy found, just use safe title + original name
                    season_str = "Season 01"
                    new_filename = f"{safe_title} {src.stem}{src.suffix}"

                output = paths_cfg.get('output', {})
                dest_root = Path(output.get(media_type, output.get('tv', 'TV Shows')))
                dest_root = dest_root / safe_title / season_str
                dest_root.mkdir(parents=True, exist_ok=True)
                target = dest_root / new_filename

            if target.exists():
                return jsonify({'error': f'Target already exists: {target.name}'}), 409

            shutil.move(str(src), str(target))

            # Clean up sidecars
            for sidecar_ext in ['.reason.json', '.ctx.json', '.meta.json']:
                sc = rdir / (filename + sidecar_ext)
                if sc.exists():
                    sc.unlink()

            return jsonify({
                'status': 'approved',
                'destination': str(target),
                'final_name': target.name,
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # ── Duplicates API ────────────────────────────────────────
    @app.route('/api/duplicates', methods=['GET'])
    @login_required
    def api_duplicates_list():
        """List all files in Duplicates folders with their .dup.json metadata."""
        paths_cfg = cfg.get('paths', {})
        dup_dirs = {
            'shows': Path(paths_cfg.get('series_pipeline', {}).get('duplicates', '')),
            'movies': Path(paths_cfg.get('movie_pipeline', {}).get('duplicates', '')),
        }
        files = []
        for pipeline, ddir in dup_dirs.items():
            if not ddir.exists():
                continue
            for item in sorted(ddir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
                if item.is_file() and not item.name.endswith('.dup.json'):
                    dup_meta = {}
                    dup_sidecar = ddir / (item.name + '.dup.json')
                    if dup_sidecar.exists():
                        try:
                            dup_meta = json.loads(dup_sidecar.read_text(encoding='utf-8'))
                        except Exception:
                            pass
                    files.append({
                        'name': item.name,
                        'pipeline': pipeline,
                        'size_mb': round(item.stat().st_size / (1024*1024), 1),
                        'final_name': dup_meta.get('final_name', ''),
                        'existing_path': dup_meta.get('existing_path', ''),
                        'existing_size_mb': dup_meta.get('existing_size_mb', 0),
                        'new_size_mb': dup_meta.get('new_size_mb', 0),
                        'source': dup_meta.get('source', ''),
                        'timestamp': dup_meta.get('timestamp', ''),
                    })
        return jsonify(files)

    @app.route('/api/duplicates/replace', methods=['POST'])
    @login_required
    def api_duplicates_replace():
        """Replace the existing library file with the duplicate."""
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        ddir = Path(paths_cfg.get(pipe_key, {}).get('duplicates', ''))
        src = ddir / filename
        if not src.exists():
            return jsonify({'error': 'File not found'}), 404
        # Read dup sidecar to find target
        dup_sidecar = ddir / (filename + '.dup.json')
        if not dup_sidecar.exists():
            return jsonify({'error': 'No duplicate metadata found'}), 400
        try:
            dup_meta = json.loads(dup_sidecar.read_text(encoding='utf-8'))
        except Exception:
            return jsonify({'error': 'Invalid duplicate metadata'}), 400
        existing_path = Path(dup_meta.get('existing_path', ''))
        final_name = dup_meta.get('final_name', '')
        if not existing_path or not final_name:
            return jsonify({'error': 'Incomplete duplicate metadata'}), 400
        # Replace: move dup to the library location with the correct final name
        target = existing_path.parent / final_name
        target.parent.mkdir(parents=True, exist_ok=True)
        if existing_path.exists():
            existing_path.unlink()
        shutil.move(str(src), str(target))
        dup_sidecar.unlink(missing_ok=True)
        return jsonify({'status': 'replaced', 'target': str(target)})

    @app.route('/api/duplicates/delete', methods=['POST'])
    @login_required
    def api_duplicates_delete():
        """Delete (trash) a duplicate file."""
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        ddir = Path(paths_cfg.get(pipe_key, {}).get('duplicates', ''))
        src = ddir / filename
        if not src.exists():
            return jsonify({'error': 'File not found'}), 404
        trash = Path(paths_cfg.get('trash_root', 'Trash'))
        trash.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(trash / filename))
        # Clean up sidecar
        dup_sidecar = ddir / (filename + '.dup.json')
        if dup_sidecar.exists():
            dup_sidecar.unlink()
        return jsonify({'status': 'deleted'})

    @app.route('/api/duplicates/open-folder', methods=['POST'])
    @login_required
    def api_duplicates_open_folder():
        """Open a duplicate or its existing library file in OS file manager."""
        import subprocess
        data = request.get_json() or {}
        filename = data.get('name', '')
        pipeline = data.get('pipeline', 'shows')
        target = data.get('target', 'dup')  # 'dup' or 'existing'
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        ddir = Path(paths_cfg.get(pipe_key, {}).get('duplicates', ''))
        if target == 'existing':
            dup_sidecar = ddir / (filename + '.dup.json')
            if dup_sidecar.exists():
                try:
                    dup_meta = json.loads(dup_sidecar.read_text(encoding='utf-8'))
                    file_path = Path(dup_meta.get('existing_path', ''))
                except Exception:
                    return jsonify({'error': 'Cannot read metadata'}), 400
            else:
                return jsonify({'error': 'No metadata'}), 404
        else:
            file_path = ddir / filename
        if not file_path.exists():
            file_path = file_path.parent
        try:
            if os.name == 'nt':
                subprocess.Popen(['explorer', '/select,', str(file_path)])
            elif sys.platform == 'darwin':
                subprocess.Popen(['open', '-R', str(file_path)])
            else:
                subprocess.Popen(['xdg-open', str(file_path.parent)])
            return jsonify({'status': 'opened'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    @app.route('/api/duplicates/stream', methods=['GET'])
    @login_required
    def api_duplicates_stream():
        """Stream a duplicate file for video preview."""
        filename = request.args.get('name', '')
        pipeline = request.args.get('pipeline', 'shows')
        if not filename or '/' in filename or '\\' in filename or '..' in filename:
            return jsonify({'error': 'Invalid'}), 400
        paths_cfg = cfg.get('paths', {})
        pipe_key = 'series_pipeline' if pipeline == 'shows' else 'movie_pipeline'
        ddir = Path(paths_cfg.get(pipe_key, {}).get('duplicates', ''))
        file_path = ddir / filename
        if not file_path.exists():
            return jsonify({'error': 'Not found'}), 404
        return send_file(str(file_path), conditional=True)

    @app.route('/api/browse', methods=['GET'])
    @login_required
    def api_browse():
        """List directories for the path picker."""
        req_path = request.args.get('path', '')
        if not req_path:
            # Default: show filesystem roots
            if os.name == 'nt':
                # Windows: list drive letters
                import string
                drives = []
                for letter in string.ascii_uppercase:
                    dp = f"{letter}:\\"
                    if os.path.isdir(dp):
                        drives.append({"name": f"{letter}:", "path": dp, "type": "drive"})
                return jsonify({"current": "", "parent": "", "dirs": drives})
            else:
                req_path = '/'

        target = Path(req_path).resolve()
        if not target.is_dir():
            return jsonify({"error": "Not a directory"}), 400

        dirs = []
        try:
            for entry in sorted(target.iterdir()):
                if entry.is_dir() and not entry.name.startswith('.'):
                    dirs.append({
                        "name": entry.name,
                        "path": str(entry),
                        "type": "dir"
                    })
        except PermissionError:
            pass

        parent = str(target.parent) if str(target) != str(target.parent) else ""
        return jsonify({
            "current": str(target),
            "parent": parent,
            "dirs": dirs
        })

    @app.route('/api/key-status', methods=['GET'])
    @login_required
    def api_key_status():
        """Check which API keys are configured and which services are blocked."""
        from web.process_manager import SERVICE_REGISTRY, SERVICE_API_REQUIREMENTS
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})

        required_keys = set()
        for reqs in SERVICE_API_REQUIREMENTS.values():
            required_keys.update(reqs)

        key_status = {}
        for k in required_keys:
            key_status[k] = bool(keys.get(k, '').strip())

        missing = [k.upper() for k, v in key_status.items() if not v]

        blocked_services = []
        for svc in SERVICE_REGISTRY:
            required = SERVICE_API_REQUIREMENTS.get(svc['id'], [])
            svc_missing = [k for k in required if not keys.get(k, '').strip()]
            if svc_missing:
                blocked_services.append({
                    "id": svc['id'],
                    "label": svc['label'],
                    "missing": [k.upper() for k in svc_missing]
                })

        return jsonify({
            "keys": key_status,
            "missing": missing,
            "blocked_services": blocked_services,
            "all_ready": len(missing) == 0
        })

    @app.route('/api/health/paths', methods=['GET'])
    @login_required
    def api_health_paths():
        """Validate that all configured paths exist and are writable."""
        cfg_data = config_mgr.read()
        paths_cfg = cfg_data.get('paths', {})
        roots = paths_cfg.get('roots', {})

        checks = []
        # Check root paths
        for name, path_str in roots.items():
            if not path_str:
                checks.append({"name": f"roots.{name}", "path": "", "exists": False, "writable": False, "error": "not configured"})
                continue
            p = Path(path_str)
            exists = p.exists()
            writable = os.access(str(p), os.W_OK) if exists else False
            checks.append({"name": f"roots.{name}", "path": path_str, "exists": exists, "writable": writable})

        # Check critical pipeline dirs
        critical_dirs = []
        for key in ['input_drop', 'system_drop', 'processing', 'failed', 'review', 'duplicates']:
            for pipe_name, pipe_key in [('series', 'series_pipeline'), ('movies', 'movie_pipeline')]:
                pipe = paths_cfg.get(pipe_key, {})
                val = pipe.get(key, '')
                if val:
                    critical_dirs.append((f"{pipe_name}.{key}", val))
        # Staged dirs
        for pipe_name, pipe_key in [('series', 'series_pipeline'), ('movies', 'movie_pipeline')]:
            staged = paths_cfg.get(pipe_key, {}).get('staged', {})
            for sk, sv in staged.items():
                if sv:
                    critical_dirs.append((f"{pipe_name}.staged.{sk}", sv))

        for name, path_str in critical_dirs:
            p = Path(path_str)
            exists = p.exists()
            writable = os.access(str(p), os.W_OK) if exists else False
            checks.append({"name": name, "path": path_str, "exists": exists, "writable": writable})

        all_ok = all(c['exists'] and c['writable'] for c in checks if c.get('path'))
        return jsonify({"checks": checks, "all_ok": all_ok})

    @app.route('/api/health/setup-status', methods=['GET'])
    @login_required
    def api_setup_status():
        """Auto-detect setup completion status for the guide page."""
        import hashlib
        from web.process_manager import SERVICE_API_REQUIREMENTS
        cfg_data = config_mgr.read()
        keys = cfg_data.get('api_keys', {})
        roots = cfg_data.get('paths', {}).get('roots', {})
        web_panel = cfg_data.get('web_panel', {})

        # 1. Password changed from default
        pw_hash = web_panel.get('password_hash', '')
        default_hash = hashlib.sha256(b'admin').hexdigest()
        password_changed = bool(pw_hash) and pw_hash != default_hash

        # 2. Data root configured and exists
        data_root = roots.get('data', '')
        data_ok = bool(data_root) and Path(data_root).exists()

        # 3. Library root configured and exists
        library_root = roots.get('library', '')
        library_ok = bool(library_root) and Path(library_root).exists()

        # 4. API keys - check essential ones
        essential_keys = {'tvdb', 'tmdb', 'mal'}
        keys_configured = {k: bool(keys.get(k, '').strip()) for k in essential_keys}
        keys_ok = all(keys_configured.values())

        # 5. Pipeline dirs exist
        dirs_ok = True
        for pipe_key in ['series_pipeline', 'movie_pipeline']:
            pipe = cfg_data.get('paths', {}).get(pipe_key, {})
            for key in ['input_drop', 'system_drop', 'processing']:
                val = pipe.get(key, '')
                if val and not Path(val).exists():
                    dirs_ok = False
                    break

        # 6. All services can start (no missing keys)
        all_required = set()
        for reqs in SERVICE_API_REQUIREMENTS.values():
            all_required.update(reqs)
        services_ready = all(bool(keys.get(k, '').strip()) for k in all_required)

        steps = [
            {"id": "password", "label": "Change default password", "ok": password_changed,
             "hint": "Go to Settings > Web Panel > Password"},
            {"id": "data_root", "label": "Configure data root path", "ok": data_ok,
             "hint": "Go to Settings > Paths > Data Root"},
            {"id": "library_root", "label": "Configure library root path", "ok": library_ok,
             "hint": "Go to Settings > Paths > Library Root"},
            {"id": "api_keys", "label": "Add API keys (TVDB, TMDB, MAL)", "ok": keys_ok,
             "hint": "Go to Settings > API Keys", "detail": keys_configured},
            {"id": "directories", "label": "Pipeline directories created", "ok": dirs_ok,
             "hint": "Start All services once to auto-create directories"},
            {"id": "services", "label": "All services ready to start", "ok": services_ready,
             "hint": "Add remaining API keys for blocked services"},
        ]

        completed = sum(1 for s in steps if s['ok'])
        return jsonify({
            "steps": steps,
            "completed": completed,
            "total": len(steps),
            "all_done": completed == len(steps),
            "first_run": not password_changed,
        })

    @app.route('/api/test-key', methods=['POST'])
    @login_required
    def api_test_key():
        """Test if an API key is valid by making a lightweight request."""
        import urllib.request
        import urllib.error
        import urllib.parse

        data = request.get_json() or {}
        api_name = data.get('name', '').lower()
        api_key = data.get('key', '')
        api_secret = data.get('secret', '')
        test_url = data.get('test_url', '')

        if not api_name or not api_key:
            return jsonify({"valid": False, "error": "Missing name or key"}), 400

        try:
            if api_name == 'tvdb':
                # TVDB: POST /login with apikey
                import json as json_mod
                req = urllib.request.Request(
                    'https://api4.thetvdb.com/v4/login',
                    data=json_mod.dumps({"apikey": api_key}).encode('utf-8'),
                    headers={'Content-Type': 'application/json'},
                    method='POST'
                )
                resp = urllib.request.urlopen(req, timeout=10)
                result = resp.read()
                return jsonify({"valid": True, "message": "TVDB key is valid"})

            elif api_name == 'tmdb':
                # TMDB: GET /search/movie with api_key param
                url = f'https://api.themoviedb.org/3/search/movie?api_key={api_key}&query=test'
                req = urllib.request.Request(url)
                resp = urllib.request.urlopen(req, timeout=10)
                return jsonify({"valid": True, "message": "TMDB key is valid"})

            elif api_name == 'mal':
                # MAL: GET /anime with X-MAL-CLIENT-ID header
                url = 'https://api.myanimelist.net/v2/anime?q=test&limit=1'
                req = urllib.request.Request(url, headers={'X-MAL-CLIENT-ID': api_key})
                resp = urllib.request.urlopen(req, timeout=10)
                return jsonify({"valid": True, "message": "MAL key is valid"})

            elif api_name == 'trakt':
                # Trakt: GET /shows/trending with client_id header
                url = 'https://api.trakt.tv/shows/trending?limit=1'
                req = urllib.request.Request(url, headers={
                    'trakt-api-key': api_key,
                    'trakt-api-version': '2',
                    'Content-Type': 'application/json',
                    'User-Agent': 'MyMediaManager/1.0',
                })
                resp = urllib.request.urlopen(req, timeout=10)
                return jsonify({"valid": True, "message": "Trakt key is valid"})

            elif api_name == 'omdb':
                # OMDb: GET with apikey query param — returns JSON with Response field
                import json as json_mod
                url = f'http://www.omdbapi.com/?apikey={api_key}&t=test'
                req = urllib.request.Request(url, headers={'User-Agent': 'MyMediaManager/1.0'})
                resp = urllib.request.urlopen(req, timeout=10)
                result = json_mod.loads(resp.read().decode())
                if result.get('Response') == 'False':
                    return jsonify({"valid": False, "error": result.get('Error', 'Invalid key')})
                return jsonify({"valid": True, "message": "OMDb key is valid"})

            elif api_name == 'fanart':
                # Fanart.tv: GET with api_key query param — 401 if invalid
                url = f'https://webservice.fanart.tv/v3/movies/550?api_key={api_key}'
                req = urllib.request.Request(url, headers={'User-Agent': 'MyMediaManager/1.0'})
                resp = urllib.request.urlopen(req, timeout=10)
                return jsonify({"valid": True, "message": "Fanart.tv key is valid"})

            elif api_name == 'youtube':
                # YouTube Data API v3: GET search with key param
                url = f'https://www.googleapis.com/youtube/v3/search?part=snippet&q=test&key={api_key}&maxResults=1'
                req = urllib.request.Request(url, headers={'User-Agent': 'MyMediaManager/1.0'})
                resp = urllib.request.urlopen(req, timeout=10)
                return jsonify({"valid": True, "message": "YouTube API key is valid"})

            elif api_name == 'igdb':
                # IGDB: Twitch OAuth — POST client_credentials to get token
                import json as json_mod
                if not api_secret:
                    return jsonify({"valid": False, "error": "IGDB requires both Client ID and Client Secret"})
                token_data = urllib.parse.urlencode({
                    'client_id': api_key,
                    'client_secret': api_secret,
                    'grant_type': 'client_credentials',
                }).encode('utf-8')
                req = urllib.request.Request(
                    'https://id.twitch.tv/oauth2/token',
                    data=token_data,
                    headers={'User-Agent': 'MyMediaManager/1.0'},
                    method='POST'
                )
                resp = urllib.request.urlopen(req, timeout=10)
                result = json_mod.loads(resp.read().decode())
                if 'access_token' in result:
                    return jsonify({"valid": True, "message": "IGDB credentials are valid"})
                return jsonify({"valid": False, "error": "No access token received"})

            else:
                # Custom API: try test URL if provided
                if test_url:
                    # Try key in common header formats
                    for header_name in ['Authorization', 'X-Api-Key', 'api-key']:
                        try:
                            header_val = f'Bearer {api_key}' if header_name == 'Authorization' else api_key
                            req = urllib.request.Request(test_url, headers={
                                header_name: header_val,
                                'User-Agent': 'MyMediaManager/1.0',
                            })
                            resp = urllib.request.urlopen(req, timeout=10)
                            if resp.status < 400:
                                return jsonify({"valid": True, "message": f"Key verified via {test_url}"})
                        except urllib.error.HTTPError as e:
                            if e.code in (401, 403):
                                continue  # Try next header format
                            # Other errors (404, 500) — endpoint might just not exist
                            continue
                        except Exception:
                            continue
                    # Also try as query param
                    try:
                        sep = '&' if '?' in test_url else '?'
                        param_url = f'{test_url}{sep}api_key={api_key}'
                        req = urllib.request.Request(param_url, headers={'User-Agent': 'MyMediaManager/1.0'})
                        resp = urllib.request.urlopen(req, timeout=10)
                        if resp.status < 400:
                            return jsonify({"valid": True, "message": f"Key verified via {test_url}"})
                    except Exception:
                        pass
                    return jsonify({"valid": False, "error": f"Key rejected by {test_url} (tried common auth formats)"})
                return jsonify({"valid": False, "error": f"No automated test for '{api_name}'"})

        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                return jsonify({"valid": False, "error": f"Invalid key (HTTP {e.code})"})
            return jsonify({"valid": False, "error": f"API error (HTTP {e.code})"})
        except urllib.error.URLError as e:
            return jsonify({"valid": False, "error": f"Connection failed: {str(e.reason)}"})
        except Exception as e:
            return jsonify({"valid": False, "error": str(e)})

    @app.route('/api/library', methods=['GET'])
    @login_required
    def api_library():
        """Scan library folders and return structure with file metadata."""
        import json as json_mod
        roots = cfg.get('paths', {}).get('roots', {})
        output = cfg.get('paths', {}).get('output', {})
        library_root = Path(roots.get('library', str(ROOT / 'Library')))

        # Determine "new" threshold: files modified within last N hours
        new_hours = request.args.get('new_hours', 24, type=int)
        now = time_mod.time()
        new_threshold = now - (new_hours * 3600)

        # Load show_cache for extra metadata
        cache_cfg = cfg.get('cache', {})
        show_cache_path = Path(cache_cfg.get('show_cache_file', 'cache/show_cache.json'))
        if not show_cache_path.is_absolute():
            show_cache_path = ROOT / show_cache_path
        recently_cached = set()
        try:
            if show_cache_path.exists():
                with open(show_cache_path, 'r', encoding='utf-8') as f:
                    sc_data = json_mod.load(f)
                for slug, entry in sc_data.items():
                    last_seen = entry.get('last_seen', '')
                    if last_seen:
                        recently_cached.add(entry.get('canonical_name', ''))
        except Exception:
            pass

        categories = {
            'tv': {'label': 'TV Shows', 'path': output.get('tv', 'TV Shows'), 'color': '#4fc3f7'},
            'movies': {'label': 'Movies', 'path': output.get('movies', 'Movies'), 'color': '#ffb74d'},
            'anime_shows': {'label': 'Anime Shows', 'path': output.get('anime_shows', 'Anime/Shows'), 'color': '#6366f1'},
            'anime_movies': {'label': 'Anime Movies', 'path': output.get('anime_movies', 'Anime/Movies'), 'color': '#ce93d8'},
            'cartoons': {'label': 'Cartoons', 'path': output.get('cartoons', 'Cartoons'), 'color': '#81c784'},
            'reality': {'label': 'Reality TV', 'path': output.get('reality', 'Reality TV'), 'color': '#ff7043'},
            'talkshow': {'label': 'Talk Shows', 'path': output.get('talkshow', 'Talk Shows'), 'color': '#26c6da'},
            'docs_series': {'label': 'Doc Series', 'path': output.get('documentaries_series', 'Documentaries/Series'), 'color': '#8d6e63'},
            'docs_movies': {'label': 'Doc Movies', 'path': output.get('documentaries_movies', 'Documentaries/Movies'), 'color': '#8d6e63'},
            'standup': {'label': 'Stand-Up', 'path': output.get('standup', 'Stand-Up'), 'color': '#ab47bc'},
        }

        result = {}
        total_files = 0
        new_files = 0

        for cat_id, cat_info in categories.items():
            cat_path = library_root / cat_info['path']
            entries = []
            cat_total = 0
            cat_new = 0

            if cat_path.exists():
                # For shows: list top-level folders (show names) with subfolders (seasons)
                # For movies: list files directly
                for item in sorted(cat_path.iterdir()):
                    if item.name.startswith('.'):
                        continue
                    if item.is_dir():
                        # Show/collection folder — list subfolders and direct files
                        subfolders = []
                        direct_files = []
                        folder_total = 0
                        newest_mtime = 0

                        for child in sorted(item.iterdir()):
                            if child.name.startswith('.'):
                                continue
                            if child.is_dir():
                                # Season/subfolder
                                sf_files = []
                                sf_newest = 0
                                for f in sorted(child.iterdir()):
                                    if f.is_file() and not f.name.startswith('.'):
                                        mt = f.stat().st_mtime
                                        is_new = mt > new_threshold
                                        sf_files.append({
                                            'name': f.name,
                                            'rel_path': str(f.relative_to(cat_path)),
                                            'size_mb': round(f.stat().st_size / (1024*1024), 1),
                                            'modified': mt,
                                            'is_new': is_new,
                                        })
                                        if mt > sf_newest:
                                            sf_newest = mt
                                        folder_total += 1
                                        if is_new:
                                            cat_new += 1
                                subfolders.append({
                                    'name': child.name,
                                    'files': sf_files,
                                    'file_count': len(sf_files),
                                    'newest_mtime': sf_newest,
                                    'has_new': sf_newest > new_threshold,
                                })
                                if sf_newest > newest_mtime:
                                    newest_mtime = sf_newest
                            elif child.is_file():
                                # Direct file in show folder (not in season subfolder)
                                mt = child.stat().st_mtime
                                is_new = mt > new_threshold
                                direct_files.append({
                                    'name': child.name,
                                    'rel_path': str(child.relative_to(cat_path)),
                                    'size_mb': round(child.stat().st_size / (1024*1024), 1),
                                    'modified': mt,
                                    'is_new': is_new,
                                })
                                if mt > newest_mtime:
                                    newest_mtime = mt
                                folder_total += 1
                                if is_new:
                                    cat_new += 1

                        entries.append({
                            'name': item.name,
                            'type': 'folder',
                            'file_count': folder_total,
                            'subfolders': subfolders,
                            'files': direct_files,
                            'newest_mtime': newest_mtime,
                            'has_new': newest_mtime > new_threshold,
                        })
                        cat_total += folder_total
                    elif item.is_file():
                        mt = item.stat().st_mtime
                        is_new = mt > new_threshold
                        entries.append({
                            'name': item.name,
                            'type': 'file',
                            'size_mb': round(item.stat().st_size / (1024*1024), 1),
                            'modified': mt,
                            'is_new': is_new,
                        })
                        cat_total += 1
                        if is_new:
                            cat_new += 1

            total_files += cat_total
            new_files += cat_new
            result[cat_id] = {
                'label': cat_info['label'],
                'color': cat_info['color'],
                'path': str(cat_path),
                'exists': cat_path.exists(),
                'entries': entries,
                'total_files': cat_total,
                'new_files': cat_new,
            }

        return jsonify({
            'categories': result,
            'library_root': str(library_root),
            'total_files': total_files,
            'new_files': new_files,
            'new_hours': new_hours,
        })

    @app.route('/api/dry-run', methods=['POST'])
    @login_required
    def api_dry_run():
        """Classify a filename without moving files. Returns predicted result."""
        data = request.get_json() or {}
        filename = data.get('filename', '').strip()
        if not filename:
            return jsonify({"error": "filename required"}), 400

        try:
            from bin.central_logic import CentralLogic
            from bin.media_sources import SourcePool
            brain = CentralLogic()

            # Detect if series (has SxxEyy) or movie
            import re
            sxxeyy = re.search(r'[Ss](\d{1,2})[Ee](\d{1,4})', filename)

            if sxxeyy:
                # Series: run through classifier logic
                raw_name = re.match(r'^(?P<name>.+?)\s*[Ss]\d', filename)
                raw = raw_name.group('name').replace('.', ' ').strip() if raw_name else filename
                candidate, _, _ = brain.get_title_candidate(filename)
                queries = brain.generate_query_matrix(candidate)

                # Load config for API access
                fresh_cfg = config_mgr.read()
                cache_root = Path(fresh_cfg.get('cache', {}).get('root', str(ROOT / 'cache')))
                pool = SourcePool(fresh_cfg, cache_root / "classifier")

                classifier_cfg = fresh_cfg.get('api_config', {}).get('classifier', {})
                anime_order = classifier_cfg.get('anime_check', ['mal', 'jikan'])
                tv_order = classifier_cfg.get('tv_check', ['tvdb'])

                best_result = None
                best_score = 0

                for q in queries:
                    if len(q) < 2:
                        continue

                    # Anime check
                    anime_results = pool.search_chain(q, anime_order, media_type="anime")
                    for a in anime_results:
                        a_type = (a.get('media_type') or '').upper()
                        if a_type not in ('TV', 'OVA', 'ONA'):
                            continue
                        for key in ['title_english', 'title']:
                            api_title = a.get(key)
                            if not api_title:
                                continue
                            score = brain.calculate_confidence(raw, api_title)
                            if score > best_score:
                                best_score = score
                                best_result = {
                                    "title": a.get('title_english') or a.get('title'),
                                    "media_type": "anime",
                                    "source": a.get('source'),
                                    "source_id": a.get('source_id'),
                                }

                    # TV check
                    tv_results = pool.search_chain(q, tv_order, media_type="tv")
                    tv_results = brain.tvmaze_spinoff_fallback(tv_results, q, pool, 'tv')
                    for t in tv_results:
                        score = brain.calculate_confidence(raw, t.get('title'))
                        matched = t.get('title')
                        for alias in (t.get('aliases') or []):
                            a_score = brain.calculate_confidence(raw, alias)
                            if a_score > score:
                                score = a_score
                                matched = alias
                        if score > best_score:
                            best_score = score
                            src = pool.get(t['source'])
                            details = src.get_details(t['source_id']) if src else None
                            genres = details.get('genres', []) if details else []
                            media_type = "tv"
                            if "anime" in genres:
                                media_type = "anime"
                            elif "animation" in genres:
                                media_type = "cartoons"
                            elif "documentary" in genres:
                                media_type = "documentaries"
                            elif "reality" in genres:
                                media_type = "reality"
                            elif "talk-show" in genres:
                                media_type = "talkshow"
                            best_result = {
                                "title": matched,
                                "media_type": media_type,
                                "source": t.get('source'),
                                "source_id": t.get('source_id'),
                                "genres": genres,
                            }

                    if best_score >= 95:
                        break

                season = int(sxxeyy.group(1))
                episode = int(sxxeyy.group(2))
                safe_title = brain.sanitize_for_filesystem(best_result['title']) if best_result else candidate

                return jsonify({
                    "input": filename,
                    "pipeline": "series",
                    "candidate": candidate,
                    "season": season,
                    "episode": episode,
                    "confidence": best_score,
                    "match": best_result,
                    "predicted_name": f"{safe_title} - S{season:02}E{episode:02}{Path(filename).suffix}" if best_result else None,
                    "predicted_path": f"{best_result['media_type'].replace('_', ' ').title()}/{safe_title}/Season {season:02}/" if best_result else None,
                })
            else:
                # Movie
                candidate, year, _ = brain.get_title_candidate(filename)
                queries = brain.generate_query_matrix(candidate)

                fresh_cfg = config_mgr.read()
                cache_root = Path(fresh_cfg.get('cache', {}).get('root', str(ROOT / 'cache')))
                pool = SourcePool(fresh_cfg, cache_root / "movies")

                movies_cfg = fresh_cfg.get('api_config', {}).get('movies', {})
                search_order = movies_cfg.get('search', ['tmdb'])

                best_result = None
                best_score = 0

                for q in queries:
                    if len(q) < 2:
                        continue
                    results = pool.search_chain(q, search_order, media_type="movie")
                    for r in results:
                        score = brain.calculate_confidence(
                            filename, r.get('title'),
                            api_year=r.get('year'), file_year=year,
                            year_mode="tiebreaker"
                        )
                        if score > best_score:
                            best_score = score
                            best_result = {
                                "title": r.get('title'),
                                "year": r.get('year'),
                                "source": r.get('source'),
                                "source_id": r.get('source_id'),
                                "media_type": "movie",
                            }
                    if best_score >= 95:
                        break

                safe_title = brain.sanitize_for_filesystem(best_result['title']) if best_result else candidate
                result_year = best_result['year'] if best_result else year

                return jsonify({
                    "input": filename,
                    "pipeline": "movies",
                    "candidate": candidate,
                    "file_year": year,
                    "confidence": best_score,
                    "match": best_result,
                    "predicted_name": f"{safe_title} ({result_year}){Path(filename).suffix}" if best_result and result_year else None,
                    "predicted_path": f"Movies/" if best_result else None,
                })

        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/api/auth/verify', methods=['POST'])
    @login_required
    def api_auth_verify():
        """Verify password for sensitive actions (e.g., revealing API keys)."""
        from web.auth import _check_password, _auth_enabled
        if not _auth_enabled():
            return jsonify({"verified": True})
        data = request.get_json() or {}
        password = data.get('password', '')
        if _check_password(password):
            return jsonify({"verified": True})
        return jsonify({"verified": False, "error": "Incorrect password"}), 403

    # ──────────────────────────────────────────────
    # SocketIO Events
    # ──────────────────────────────────────────────

    @socketio.on('connect')
    def handle_connect():
        # Send initial state
        socketio.emit('service_status', pm.get_status())
        socketio.emit('pipeline_update', pipeline_mon.get_snapshot())

    @socketio.on('request_logs')
    def handle_request_logs(data):
        service = data.get('service', '')
        lines = log_tailer.read_logs_for(service, 10000)
        socketio.emit('log_history', {'service': service, 'lines': lines})

    # Start background threads
    pm.start_monitor(socketio)
    pipeline_mon.start(socketio, on_snapshot=pm.auto_manage)
    log_tailer.start_streaming(socketio)

    # Scheduled auto-cleanup (runs every 6 hours)
    import threading
    _cleanup_stop = threading.Event()

    def _scheduled_cleanup():
        while not _cleanup_stop.wait(6 * 3600):
            try:
                recovery_mgr.auto_cleanup()
            except Exception:
                pass

    _cleanup_thread = threading.Thread(target=_scheduled_cleanup, daemon=True)
    _cleanup_thread.start()

    # Cleanup on exit
    def cleanup():
        _cleanup_stop.set()
        pm.shutdown()
        pipeline_mon.stop()
        log_tailer.stop_streaming()
    atexit.register(cleanup)

    return app
