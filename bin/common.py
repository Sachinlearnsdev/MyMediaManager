#!/usr/bin/env python3
"""
common.py -- Single gateway for configuration, logging, and path resolution.
All services import this module. No other module should read config files directly.
"""

import json
import logging
import sys
import os
import glob
from pathlib import Path
from datetime import datetime

# Derive BASE_DIR from this file's location (no hardcoded path)
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config" / "config.json"
SECRETS_PATH = BASE_DIR / "config" / "secrets.json"
ENV_PATH = BASE_DIR / ".env"

# .env variable name → api_keys key name
_ENV_KEY_MAP = {
    'MMM_TVDB_KEY': 'tvdb',
    'MMM_TMDB_KEY': 'tmdb',
    'MMM_MAL_KEY': 'mal',
    'MMM_TRAKT_KEY': 'trakt',
    'MMM_OMDB_KEY': 'omdb',
    'MMM_FANART_KEY': 'fanart',
    'MMM_IGDB_KEY': 'igdb',
    'MMM_YOUTUBE_KEY': 'youtube',
}


def _deep_merge(base: dict, override: dict) -> dict:
    """Merge override into base recursively."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def hydrate_paths(cfg):
    """
    Recursively updates relative paths in the config to be absolute
    by joining them with their defined roots.
    """
    try:
        paths = cfg.get('paths', {})
        roots = paths.get('roots', {})

        manager_root = Path(roots.get('manager', str(BASE_DIR)))
        # Data root: where all file I/O happens (Drop, Work, Trash, Review).
        # Defaults to manager root for backwards compatibility.
        # On multi-drive setups, point this to the HDD so all moves
        # stay on the same filesystem (instant rename vs slow copy+delete).
        data_root = Path(roots.get('data', str(manager_root)))
        library_root = Path(roots.get('library', str(data_root / 'Library')))

        # 1. Hydrate Trash (data root -- file I/O)
        if 'trash_root' in paths and not os.path.isabs(str(paths['trash_root'])):
            paths['trash_root'] = str(data_root / paths['trash_root'])

        # 2. Hydrate Output (Library root)
        if 'output' in paths:
            for key, val in paths['output'].items():
                if not os.path.isabs(str(val)):
                    paths['output'][key] = str(library_root / val)

        # 3. Hydrate Pipelines (data root -- file I/O)
        for pipeline in ['series_pipeline', 'movie_pipeline']:
            if pipeline in paths:
                section = paths[pipeline]
                for key, val in section.items():
                    if isinstance(val, dict):
                        for subkey, subval in val.items():
                            if isinstance(subval, str) and not os.path.isabs(subval):
                                section[key][subkey] = str(data_root / subval)
                    elif isinstance(val, str) and not os.path.isabs(val):
                        section[key] = str(data_root / val)

        # 4. Hydrate Cache paths
        if 'cache' in cfg:
            for key in ['root', 'show_cache_file', 'noise_learned_file']:
                val = cfg['cache'].get(key, '')
                if val and not os.path.isabs(str(val)):
                    cfg['cache'][key] = str(manager_root / val)

        # 5. Hydrate Logging path
        if 'logging' in cfg:
            val = cfg['logging'].get('path', '')
            if val and not os.path.isabs(str(val)):
                cfg['logging']['path'] = str(manager_root / val)

        return cfg
    except Exception as e:
        print(f"[CRITICAL] Path hydration failed: {e}")
        sys.exit(1)


def _load_env_keys() -> dict:
    """Read API keys from .env file. Returns {api_name: key_value}."""
    keys = {}
    if not ENV_PATH.exists():
        return keys
    try:
        for line in ENV_PATH.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            var, _, val = line.partition('=')
            var, val = var.strip(), val.strip()
            if not val:
                continue
            if var in _ENV_KEY_MAP:
                keys[_ENV_KEY_MAP[var]] = val
            elif var.startswith('MMM_') and var.endswith('_KEY'):
                # Custom key: MMM_SOMETHING_KEY → something
                api_name = var[4:-4].lower()
                keys[api_name] = val
    except Exception as e:
        print(f"[WARNING] Could not load .env: {e}")
    return keys


def load_config():
    """Loads config.json, merges secrets.json, overlays .env keys, hydrates paths."""
    if not CONFIG_PATH.exists():
        print(f"[CRITICAL] Config missing at {CONFIG_PATH}")
        sys.exit(1)

    try:
        txt = CONFIG_PATH.read_text()
        raw_config = json.loads(txt)
    except Exception as e:
        print(f"[CRITICAL] Config invalid: {e}")
        sys.exit(1)

    # Merge secrets if present (legacy support)
    if SECRETS_PATH.exists():
        try:
            secrets = json.loads(SECRETS_PATH.read_text())
            raw_config = _deep_merge(raw_config, secrets)
        except Exception as e:
            print(f"[WARNING] Could not load secrets: {e}")

    # Overlay API keys from .env
    env_keys = _load_env_keys()
    if env_keys:
        existing = raw_config.get('api_keys', {})
        existing.update(env_keys)
        raw_config['api_keys'] = existing

    return hydrate_paths(raw_config)


def cleanup_old_logs(log_dir, service_name, retention_count):
    pattern = str(log_dir / f"{service_name}_*.log")
    files = sorted(glob.glob(pattern), key=os.path.getmtime)
    while len(files) > retention_count:
        oldest = files.pop(0)
        try:
            os.remove(oldest)
        except Exception as e:
            print(f"[WARNING] Could not remove old log {oldest}: {e}")


def setup_logger(service_name, mode=None):
    """Returns (logger, config). Config is fully hydrated.
    If mode is provided (e.g. 'series'/'movies'), creates a separate log file
    like automouse_series_2026-03-03_12-00-00.log for cleaner log splitting."""
    cfg = load_config()

    effective_name = f"{service_name}_{mode}" if mode else service_name

    log_cfg = cfg.get("logging", {})
    default_log_path = BASE_DIR / "logs"
    log_dir = Path(log_cfg.get("path", str(default_log_path)))
    retention = log_cfg.get("retention_sessions", 5)
    level_str = log_cfg.get("level", "INFO").upper()

    log_dir.mkdir(parents=True, exist_ok=True)
    cleanup_old_logs(log_dir, effective_name, retention)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = log_dir / f"{effective_name}_{timestamp}.log"

    logger = logging.getLogger(effective_name)
    logger.setLevel(getattr(logging, level_str, logging.INFO))
    logger.propagate = False

    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-5s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"--- SESSION START: {service_name} ---")
    logger.info(f"Log file: {log_file}")

    return logger, cfg
