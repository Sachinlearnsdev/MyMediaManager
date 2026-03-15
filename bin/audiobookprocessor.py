#!/usr/bin/env python3
"""
audiobookprocessor.py -- Final audiobook file processor.
Reads tagged audio files from Staged/Audiobooks, builds the final library path
using the output format template, and moves files to Library/Audiobooks/.

Output format (from config): {author}/{title} ({year})/{part}.{ext}
"""

import os
import sys
import time
import json
import re
import shutil
from pathlib import Path
from datetime import datetime, timezone

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import AUDIO_EXTS, AUDIOBOOK_EXTS, SCAN_INTERVAL, CONFIDENCE_AUDIOBOOK
from bin.media_sources import SourcePool
from bin.noise_learner import NoiseLearner
import common

log, CFG = common.setup_logger("audiobookprocessor")
NOISE_LEARNER = NoiseLearner(CFG)

INPUT_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('staged', {}).get('audiobooks', ''))
OUTPUT_DIR = Path(CFG.get('paths', {}).get('output', {}).get('audiobooks', ''))
REVIEW_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('review', ''))
DUP_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('duplicates', 'Duplicates/Music'))
OUTPUT_FMT = CFG.get('output_formats', {}).get('audiobooks', '{author}/{title} ({year})/{part}.{ext}')

# API enrichment (reuses book search APIs for audiobook metadata)
_CACHE_DIR = Path(CFG['paths'].get('cache', {}).get('root', 'cache')) if isinstance(CFG['paths'].get('cache'), dict) else Path(CFG.get('cache', {}).get('root', 'cache'))
_SEARCH_ORDER = CFG.get('api_config', {}).get('books', {}).get('search', ['openlibrary', 'googlebooks'])
_pool = SourcePool(CFG, _CACHE_DIR / "api")


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def enrich_from_api(tags: dict) -> dict:
    """Attempt OpenLibrary/GoogleBooks lookup for audiobook metadata."""
    # For audiobooks, artist = author, album = book title
    author = tags.get("album_artist") or tags.get("artist", "")
    title = tags.get("album") or tags.get("title", "")

    if not title:
        return tags

    query = f"{title} {author}".strip()
    results = _pool.search_chain(query, _SEARCH_ORDER, media_type="book")
    if not results:
        return tags

    best = results[0]
    merged = dict(tags)
    # Map book API fields to audiobook tags
    if best.get('author') and not (merged.get('album_artist') or merged.get('artist')):
        merged['album_artist'] = best['author']
        if not merged.get('artist'):
            merged['artist'] = best['author']
    if best.get('title') and not merged.get('album'):
        merged['album'] = best['title']
    if best.get('year') and not merged.get('year'):
        merged['year'] = best['year']
    merged['_api_enriched'] = True
    return merged


def build_output_path(tags: dict, ext: str) -> Path:
    """Build the final relative path from tags using the audiobook format template."""
    # For audiobooks: artist = author, album = book title
    author = sanitize(tags.get("album_artist") or tags.get("artist") or "Unknown Author")
    title = sanitize(tags.get("album") or tags.get("title") or "Unknown")
    year = tags.get("year") or "0000"
    track = tags.get("track") or 0
    part_name = sanitize(tags.get("title") or f"Part {track:02d}")
    ext_clean = ext.lstrip(".")

    try:
        rel_path = OUTPUT_FMT.format(
            author=author, title=title, year=year,
            part=part_name, track=track, ext=ext_clean
        )
    except (KeyError, ValueError):
        rel_path = f"{author}/{title} ({year})/{part_name}.{ext_clean}"

    return Path(rel_path)


def _write_dup(dest_path, final_name, existing_path):
    common.write_dup_sidecar(dest_path, "audiobookprocessor", final_name, existing_path)


def main():
    for d in [INPUT_DIR, OUTPUT_DIR, REVIEW_DIR, DUP_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR} -> Library: {OUTPUT_DIR}")

    while True:
        try:
            # Accept all audio extensions (audiobooks can be .m4b, .mp3, etc.)
            all_audio = AUDIO_EXTS | AUDIOBOOK_EXTS
            files = sorted(
                [f for f in INPUT_DIR.iterdir()
                 if f.is_file() and f.suffix.lower() in all_audio],
                key=lambda p: p.stat().st_mtime
            )
            for f in files:
                meta_path = f.with_name(f.name + ".meta.json")
                if not meta_path.exists():
                    log.warning(f"No meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "audiobookprocessor", 0, CONFIDENCE_AUDIOBOOK, {}, {}, reason="no_meta")
                    continue

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "audiobookprocessor", 0, CONFIDENCE_AUDIOBOOK, {}, {}, reason="bad_meta")
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # API enrichment when author or title is missing
                if not (tags.get("album_artist") or tags.get("artist")) or not (tags.get("album") or tags.get("title")):
                    try:
                        tags = enrich_from_api(tags)
                        if tags.get('_api_enriched'):
                            author = tags.get('album_artist') or tags.get('artist')
                            log.info(f"Enriched {f.name} via API: {author}/{tags.get('album')}")
                    except Exception as e:
                        log.debug(f"API enrichment failed for {f.name}: {e}")

                # Noise learner: always learns (regardless of apply toggle)
                try:
                    author = tags.get('album_artist') or tags.get('artist', '')
                    title = tags.get('album') or tags.get('title', '')
                    canonical = f"{author} {title}".strip()
                    if canonical:
                        NOISE_LEARNER.learn_from_match(
                            f.stem, canonical, f.name,
                            author, category="music"
                        )
                except Exception:
                    pass

                rel_path = build_output_path(tags, f.suffix)
                target = OUTPUT_DIR / rel_path

                if target.exists():
                    log.warning(f"Duplicate: {rel_path} already exists. Moving to Duplicates.")
                    DUP_DIR.mkdir(parents=True, exist_ok=True)
                    dup_dest = DUP_DIR / f.name
                    shutil.move(str(f), str(dup_dest))
                    _write_dup(dup_dest, str(rel_path), target)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(f), str(target))
                    log.info(f"MOVED -> {target}")

                meta_path.unlink(missing_ok=True)

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
