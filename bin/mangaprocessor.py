#!/usr/bin/env python3
"""
mangaprocessor.py -- Final manga file processor.
Reads classified manga from Staged/Manga, builds the final library path,
and moves files to Library/Comics/Manga/.

Output structure: Library/Comics/Manga/{series}/{series} Vol.{volume:02d}.{ext}
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

from bin.constants import BOOK_EXTS, COMIC_EXTS, SCAN_INTERVAL, CONFIDENCE_COMIC
from bin.media_sources import SourcePool
from bin.noise_learner import NoiseLearner
import common

log, CFG = common.setup_logger("mangaprocessor")
NOISE_LEARNER = NoiseLearner(CFG)

INPUT_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('staged', {}).get('manga', ''))
OUTPUT_DIR = Path(CFG.get('paths', {}).get('output', {}).get('manga', '') or CFG.get('paths', {}).get('output', {}).get('comics', ''))
REVIEW_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('review', ''))
DUP_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('duplicates', 'Duplicates/Books'))
OUTPUT_FMT = CFG.get('output_formats', {}).get('manga', '{series}/{series} Vol.{volume:02d}.{ext}')

# API enrichment (AniList manga + MAL)
_CACHE_DIR = Path(CFG['paths'].get('cache', {}).get('root', 'cache')) if isinstance(CFG['paths'].get('cache'), dict) else Path(CFG.get('cache', {}).get('root', 'cache'))
_SEARCH_ORDER = CFG.get('api_config', {}).get('manga_books', {}).get('search', ['anilist', 'mal'])
_pool = SourcePool(CFG, _CACHE_DIR / "api")


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def enrich_from_api(tags: dict) -> dict:
    """Attempt AniList/MAL manga lookup to fill in missing series info."""
    series = tags.get("series") or tags.get("title", "")
    if not series:
        return tags

    # Use AniList manga-specific search
    anilist = _pool.get('anilist')
    if anilist and hasattr(anilist, 'search_manga'):
        try:
            results = anilist.search_manga(series)
            if results:
                best = results[0]
                merged = dict(tags)
                if best.get('title') and not merged.get('series'):
                    merged['series'] = best.get('title_english') or best['title']
                if best.get('year') and not merged.get('year'):
                    merged['year'] = str(best['year'])
                if best.get('volumes'):
                    merged['total_volumes'] = best['volumes']
                if best.get('author'):
                    merged['author'] = best['author']
                merged['_api_enriched'] = True
                return merged
        except Exception as e:
            log.debug(f"AniList manga search failed: {e}")

    return tags


def build_output_path(tags: dict, ext: str) -> Path:
    """Build the final relative path from tags."""
    series = sanitize(tags.get("series") or tags.get("title") or "Unknown Series")
    volume = tags.get("volume") or tags.get("number") or 0
    year = tags.get("year") or "0000"
    ext_clean = ext.lstrip(".")

    if isinstance(volume, str):
        try:
            volume = int(float(volume))
        except (ValueError, TypeError):
            volume = 0

    try:
        rel_path = OUTPUT_FMT.format(
            series=series, volume=volume, year=year, ext=ext_clean
        )
    except (KeyError, ValueError):
        rel_path = f"Manga/{series}/{series} Vol.{volume:02d}.{ext_clean}"

    return Path(rel_path)


def _write_dup(dest_path, final_name, existing_path):
    common.write_dup_sidecar(dest_path, "mangaprocessor", final_name, existing_path)


def main():
    for d in [INPUT_DIR, OUTPUT_DIR, REVIEW_DIR, DUP_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR} -> Library: {OUTPUT_DIR}")

    all_exts = BOOK_EXTS | COMIC_EXTS

    while True:
        try:
            files = sorted(
                [f for f in INPUT_DIR.iterdir()
                 if f.is_file() and f.suffix.lower() in all_exts],
                key=lambda p: p.stat().st_mtime
            )
            for f in files:
                meta_path = f.with_name(f.name + ".meta.json")
                if not meta_path.exists():
                    log.warning(f"No meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "mangaprocessor", 0, CONFIDENCE_COMIC, {}, {}, reason="no_meta")
                    continue

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "mangaprocessor", 0, CONFIDENCE_COMIC, {}, {}, reason="bad_meta")
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # API enrichment when series name is missing/generic
                if not tags.get("series") or tags.get("series") == "Unknown Series":
                    try:
                        tags = enrich_from_api(tags)
                        if tags.get('_api_enriched'):
                            log.info(f"Enriched {f.name} via AniList: {tags.get('series')}")
                    except Exception as e:
                        log.debug(f"API enrichment failed for {f.name}: {e}")

                # Noise learner: always learns (regardless of apply toggle)
                try:
                    canonical = tags.get('series', '').strip()
                    if canonical:
                        NOISE_LEARNER.learn_from_match(
                            f.stem, canonical, f.name,
                            canonical, category="books"
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
