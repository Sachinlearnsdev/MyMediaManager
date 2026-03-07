#!/usr/bin/env python3
"""
auth.py -- Pi-hole style authentication for the web panel.

- Default account: admin / admin
- First login forces password change
- Password stored as SHA-256 hash in config
- Empty password_hash = no auth required
"""

import hashlib
import secrets
from functools import wraps
from flask import Blueprint, request, session, redirect, url_for, render_template, flash, current_app, jsonify

auth_bp = Blueprint('auth', __name__)

DEFAULT_PASSWORD_HASH = hashlib.sha256('admin'.encode('utf-8')).hexdigest()


def _hash_password(password: str) -> str:
    """SHA-256 hash a password."""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def _get_panel_cfg() -> dict:
    return current_app.config.get('MMM_PANEL', {})


def _auth_enabled() -> bool:
    """Check if authentication is enabled (password is set)."""
    return bool(_get_panel_cfg().get('password_hash', '').strip())


def _is_first_login() -> bool:
    """Check if user still has the default password (needs to change it)."""
    stored = _get_panel_cfg().get('password_hash', '').strip()
    return stored == DEFAULT_PASSWORD_HASH


def _check_password(password: str) -> bool:
    """Verify password against stored hash."""
    stored = _get_panel_cfg().get('password_hash', '').strip()
    if not stored:
        return True
    return _hash_password(password) == stored


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _auth_enabled():
            return f(*args, **kwargs)
        if session.get('authenticated'):
            # If first login, force password change (except on the setup page itself)
            if _is_first_login() and request.endpoint not in ('auth.setup', 'auth.change_password', 'auth.logout', 'static'):
                return redirect(url_for('auth.setup'))
            return f(*args, **kwargs)
        # API key check
        api_key = request.headers.get('X-API-Key') or request.args.get('apikey')
        if api_key:
            stored_key = _get_panel_cfg().get('api_key', '')
            if api_key and api_key == stored_key:
                return f(*args, **kwargs)
        # Return JSON for API endpoints instead of redirect
        if request.path.startswith('/api/'):
            from flask import jsonify
            return jsonify({"error": "Session expired. Please refresh the page and log in again."}), 401
        return redirect(url_for('auth.login'))
    return decorated


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    if not _auth_enabled():
        return redirect(url_for('main.dashboard'))

    if session.get('authenticated'):
        if _is_first_login():
            return redirect(url_for('auth.setup'))
        return redirect(url_for('main.dashboard'))

    if request.method == 'POST':
        password = request.form.get('password', '')
        if _check_password(password):
            session['authenticated'] = True
            session.permanent = True
            if _is_first_login():
                return redirect(url_for('auth.setup'))
            return redirect(url_for('main.dashboard'))
        flash('Invalid password', 'error')

    return render_template('login.html')


@auth_bp.route('/setup', methods=['GET', 'POST'])
def setup():
    """First-login password change page."""
    if not session.get('authenticated'):
        return redirect(url_for('auth.login'))
    if not _is_first_login():
        return redirect(url_for('main.dashboard'))

    if request.method == 'POST':
        new_pw = request.form.get('new_password', '')
        confirm_pw = request.form.get('confirm_password', '')

        if len(new_pw) < 4:
            flash('Password must be at least 4 characters', 'error')
        elif new_pw != confirm_pw:
            flash('Passwords do not match', 'error')
        elif new_pw == 'admin':
            flash('Please choose a different password', 'error')
        else:
            try:
                _save_password(new_pw)
                flash('Password updated successfully', 'success')
                return redirect(url_for('main.dashboard'))
            except Exception as e:
                flash(f'Error: {e}', 'error')

    return render_template('setup.html')


@auth_bp.route('/logout')
def logout():
    session.clear()
    if not _auth_enabled():
        return redirect(url_for('main.dashboard'))
    return redirect(url_for('auth.login'))


@auth_bp.route('/api/auth/password', methods=['POST'])
def change_password():
    """Change password via API (from settings page)."""
    if _auth_enabled() and not session.get('authenticated'):
        return jsonify({"error": "Not authenticated"}), 401

    data = request.get_json() or {}
    current_pw = data.get('current', '')
    new_pw = data.get('new', '')

    if _auth_enabled() and not _check_password(current_pw):
        return jsonify({"error": "Current password is incorrect"}), 403

    if new_pw and len(new_pw) < 4:
        return jsonify({"error": "Password must be at least 4 characters"}), 400

    try:
        _save_password(new_pw if new_pw else None)
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _save_password(new_password):
    """Save new password hash to config. None = disable auth."""
    from web.config_manager import ConfigManager
    from bin.common import CONFIG_PATH

    config_mgr = ConfigManager(CONFIG_PATH)
    cfg = config_mgr.read()

    if new_password:
        cfg['web_panel']['password_hash'] = _hash_password(new_password)
    else:
        cfg['web_panel']['password_hash'] = ''

    if 'api_key' not in cfg['web_panel'] or not cfg['web_panel']['api_key']:
        cfg['web_panel']['api_key'] = secrets.token_hex(16)

    config_mgr.write(cfg)
    current_app.config['MMM_PANEL'] = cfg['web_panel']
