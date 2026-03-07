#!/usr/bin/env python3
"""
config_manager.py -- Safe atomic config read/write for the web panel.
API keys are stored in .env (gitignored), everything else in config.json.
"""

import json
import os
import tempfile
from pathlib import Path

# .env key name → config.json api_keys name
ENV_KEY_MAP = {
    'MMM_TVDB_KEY': 'tvdb',
    'MMM_TMDB_KEY': 'tmdb',
    'MMM_MAL_KEY': 'mal',
    'MMM_TRAKT_KEY': 'trakt',
    'MMM_OMDB_KEY': 'omdb',
    'MMM_FANART_KEY': 'fanart',
    'MMM_IGDB_KEY': 'igdb',
    'MMM_YOUTUBE_KEY': 'youtube',
}

# Reverse: api_keys name → .env key name
_REVERSE_MAP = {v: k for k, v in ENV_KEY_MAP.items()}


def _env_path():
    return Path(__file__).resolve().parent.parent / '.env'


def read_env_keys() -> dict:
    """Read API keys from .env file. Returns {api_name: key_value}."""
    env_file = _env_path()
    keys = {}
    if not env_file.exists():
        return keys
    for line in env_file.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        var, _, val = line.partition('=')
        var = var.strip()
        val = val.strip()
        if not val:
            continue
        if var in ENV_KEY_MAP:
            keys[ENV_KEY_MAP[var]] = val
        elif var.startswith('MMM_') and var.endswith('_KEY'):
            # Custom key: MMM_SOMETHING_KEY → something
            api_name = var[4:-4].lower()
            keys[api_name] = val
    return keys


def write_env_keys(api_keys: dict):
    """Write API keys to .env file. Preserves comments and unknown vars."""
    env_file = _env_path()
    # Read existing lines to preserve comments and ordering
    existing_lines = []
    if env_file.exists():
        existing_lines = env_file.read_text(encoding='utf-8').splitlines()

    # Track which env vars we've already written
    written = set()
    new_lines = []

    for line in existing_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith('#') and '=' in stripped:
            var = stripped.split('=', 1)[0].strip()
            if var in ENV_KEY_MAP:
                api_name = ENV_KEY_MAP[var]
                if api_name in api_keys and api_keys[api_name].strip():
                    new_lines.append(f'{var}={api_keys[api_name]}')
                    written.add(api_name)
                # Skip if key was deleted (don't write empty)
                continue
        new_lines.append(line)

    # Append any new keys not in the original file
    for api_name, value in api_keys.items():
        if api_name not in written and value.strip():
            env_var = _REVERSE_MAP.get(api_name)
            if env_var:
                new_lines.append(f'{env_var}={value}')
            else:
                # Custom key not in map - create env var name
                env_var = f'MMM_{api_name.upper()}_KEY'
                new_lines.append(f'{env_var}={value}')

    # Atomic write
    fd, temp_path = tempfile.mkstemp(dir=str(env_file.parent), suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_lines))
            f.write('\n')
        os.replace(temp_path, str(env_file))
    except Exception:
        try:
            os.unlink(temp_path)
        except OSError:
            pass
        raise


class ConfigManager:
    def __init__(self, config_path: Path):
        self.config_path = Path(config_path)

    def read(self) -> dict:
        """Read config.json and overlay API keys from .env."""
        cfg = json.loads(self.config_path.read_text(encoding='utf-8'))
        env_keys = read_env_keys()
        if env_keys:
            existing = cfg.get('api_keys', {})
            existing.update(env_keys)
            cfg['api_keys'] = existing
        return cfg

    def read_raw(self) -> dict:
        """Read config.json without .env overlay (for internal use)."""
        return json.loads(self.config_path.read_text(encoding='utf-8'))

    def write(self, new_config: dict):
        """Write config. API keys go to .env, everything else to config.json."""
        self._validate(new_config)

        # Extract API keys → .env
        api_keys = new_config.pop('api_keys', {})
        if api_keys:
            write_env_keys(api_keys)

        # Store empty placeholders in config.json so the structure is preserved
        new_config['api_keys'] = {}

        # Atomic write config.json
        fd, temp_path = tempfile.mkstemp(
            dir=str(self.config_path.parent),
            suffix='.tmp'
        )
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(new_config, f, indent=2, ensure_ascii=False)
                f.write('\n')
            os.replace(temp_path, str(self.config_path))
        except Exception:
            try:
                os.unlink(temp_path)
            except OSError:
                pass
            raise

    def update_section(self, section: str, data: dict):
        cfg = self.read()
        if section in cfg and isinstance(cfg[section], dict):
            cfg[section].update(data)
        else:
            cfg[section] = data
        self.write(cfg)

    def _validate(self, config: dict):
        required = ['paths', 'cache', 'logging']
        for key in required:
            if key not in config:
                raise ValueError(f"Missing required config section: {key}")
        if 'roots' not in config.get('paths', {}):
            raise ValueError("Missing paths.roots in config")
