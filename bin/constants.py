#!/usr/bin/env python3
"""
constants.py -- Single source of truth for all shared constants.
Imported by every service in the pipeline.

Values in the TIMING & THRESHOLDS section can be overridden via
the "tuning" section in config/config.json. This allows users to
adjust behaviour from the web panel without touching code.
"""

import json as _json
from pathlib import Path as _Path


def _load_tuning() -> dict:
    """Load user overrides from config.json tuning section at import time."""
    try:
        cfg_path = _Path(__file__).resolve().parent.parent / "config" / "config.json"
        if cfg_path.exists():
            cfg = _json.loads(cfg_path.read_text(encoding='utf-8'))
            return cfg.get('tuning', {})
    except Exception:
        pass
    return {}


_tuning = _load_tuning()

# ============================================================
# FILE TYPE SETS
# ============================================================

VIDEO_EXTS = frozenset({
    ".mkv", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v"
})

ARCHIVE_EXTS = frozenset({".rar", ".zip", ".7z"})

JUNK_EXTENSIONS = frozenset({
    '.txt', '.nfo', '.exe', '.bat', '.sh', '.msi', '.jpg', '.png',
    '.jpeg', '.bmp', '.gif', '.url', '.lnk', '.sfv', '.md5'
})

IGNORE_PARTIALS = frozenset({'.part', '.crdownload', '.tmp', '.download'})

JUNK_MAP = {
    "documents": frozenset({".pdf", ".txt", ".doc", ".docx", ".rtf", ".nfo", ".md"}),
    "images": frozenset({".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}),
    "executables": frozenset({".exe", ".msi", ".sh", ".bat", ".cmd", ".bin", ".url", ".lnk"}),
}

# ============================================================
# API ENDPOINTS
# ============================================================

JIKAN_BASE_URL = "https://api.jikan.moe/v4"
ANILIST_API_URL = "https://graphql.anilist.co"
MAL_OFFICIAL_BASE_URL = "https://api.myanimelist.net/v2"
TVDB_API_BASE = "https://api4.thetvdb.com/v4"
TMDB_API_BASE = "https://api.themoviedb.org/3"
TRAKT_API_BASE = "https://api.trakt.tv"
TVMAZE_API_BASE = "https://api.tvmaze.com"
KITSU_API_BASE = "https://kitsu.io"
OMDB_API_BASE = "http://www.omdbapi.com"

# ============================================================
# TIMING & THRESHOLDS
# ============================================================

SCAN_INTERVAL = _tuning.get('scan_interval', 5)
EXTRACT_TIMEOUT = _tuning.get('extract_timeout', 300)
MIN_SSD_FREE_GB = _tuning.get('min_ssd_free_gb', 10)

# AutoMouse — non-blocking file stability for input drop folders
MOUSE_SCAN_INTERVAL = _tuning.get('mouse_scan_interval', 2)    # Loop interval (seconds)
MOUSE_STABILITY_SCANS = _tuning.get('mouse_stability_scans', 4)  # Consecutive unchanged scans before stable
MOUSE_BATCH_SETTLE = _tuning.get('mouse_batch_settle', 15)     # Scans with no new/changed items before batch moves (30s at 2s interval)

# Trash / Review auto-cleanup
TRASH_MAX_AGE_DAYS = _tuning.get('trash_max_age_days', 7)
REVIEW_MAX_AGE_DAYS = _tuning.get('review_max_age_days', 14)

# Confidence thresholds per processor
CONFIDENCE_CLASSIFIER = _tuning.get('confidence_classifier', 40)
CONFIDENCE_TV = _tuning.get('confidence_tv', 60)
CONFIDENCE_CARTOON = _tuning.get('confidence_cartoon', 60)
CONFIDENCE_MOVIE = _tuning.get('confidence_movie', 75)

# ============================================================
# NOISE PATTERNS (Built-in, for structpilot)
# ============================================================

NOISE_PATTERNS = [
    # Resolution
    r"\b480p\b", r"\b720p\b", r"\b1080p\b", r"\b2160p\b", r"\b4k\b",
    # Video Codec
    r"\bx264\b", r"\bx265\b", r"\bh264\b", r"\bh265\b", r"\bhevc\b", r"\bav1\b",
    # Release Type
    r"\bblu ?ray\b", r"\bwebrip\b", r"\bweb[ -]?dl\b", r"\bhdrip\b", r"\bdvdrip\b",
    r"\bbdrip\b", r"\bbrrip\b", r"\bcam\b", r"\bts\b", r"\bhdts\b",
    # Advanced Encoding
    r"\bhdr\b", r"\bhdr10\b", r"\bhdr10\+?\b", r"\bdolby[ ]?vision\b",
    r"\b10bit\b", r"\b8bit\b",
    # Audio Codec
    r"\baac\b", r"\bac3\b", r"\beac3\b", r"\bflac\b", r"\bmp3\b",
    r"\bremux\b", r"\bdts\b", r"\bdts[ -]?hd\b", r"\btruehd\b", r"\batmos\b",
    r"\b5\.1\b", r"\b7\.1\b", r"\b2\.0\b",
    # Release Groups (common patterns)
    r"\bproper\b", r"\brepack\b", r"\binternal\b",
    r"\bmulti\b", r"\bdual[ -]?audio\b",
    # Common release group markers
    r"-[a-zA-Z0-9]{2,12}$",
]

# ============================================================
# WORD MAP (for structpilot human season/episode parsing)
# ============================================================

WORD_NUM = {
    "one": 1, "first": 1, "two": 2, "second": 2, "three": 3, "third": 3,
    "four": 4, "fourth": 4, "five": 5, "fifth": 5, "six": 6, "sixth": 6,
    "seven": 7, "seventh": 7, "eight": 8, "eighth": 8, "nine": 9, "ninth": 9,
    "ten": 10, "tenth": 10
}
