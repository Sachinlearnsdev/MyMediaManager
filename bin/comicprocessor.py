#!/usr/bin/env python3
"""
comicprocessor.py -- Final comic file processor.
Reads classified comics from Staged/Comics, builds the final library path
using publisher and series metadata, and moves files to Library/Comics/.

Output structure: Library/Comics/{publisher}/{series}/{series} ### ({year}).{ext}
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
from bin.media_sources import SourcePool, download_cover
from bin.noise_learner import NoiseLearner
import common

log, CFG = common.setup_logger("comicprocessor")
NOISE_LEARNER = NoiseLearner(CFG)

INPUT_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('staged', {}).get('comics', ''))
OUTPUT_DIR = Path(CFG.get('paths', {}).get('output', {}).get('comics', ''))
REVIEW_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('review', ''))
DUP_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('duplicates', 'Duplicates/Books'))
OUTPUT_FMT = CFG.get('output_formats', {}).get('comics', '{publisher}/{series}/{series} {number:03d} ({year}).{ext}')

# API enrichment (ComicVine)
_CACHE_DIR = Path(CFG['paths'].get('cache', {}).get('root', 'cache')) if isinstance(CFG['paths'].get('cache'), dict) else Path(CFG.get('cache', {}).get('root', 'cache'))
_SEARCH_ORDER = CFG.get('api_config', {}).get('comics', {}).get('search', ['comicvine'])
_pool = SourcePool(CFG, _CACHE_DIR / "api")

# Publisher normalization
PUBLISHER_MAP = {
    'dc': 'DC', 'dc comics': 'DC',
    'marvel': 'Marvel', 'marvel comics': 'Marvel',
    'image': 'Image', 'image comics': 'Image',
    'dark horse': 'Dark Horse', 'dark horse comics': 'Dark Horse',
    'idw': 'IDW', 'idw publishing': 'IDW',
    'boom! studios': 'BOOM! Studios', 'boom studios': 'BOOM! Studios',
    'dynamite': 'Dynamite', 'dynamite entertainment': 'Dynamite',
    'valiant': 'Valiant', 'valiant comics': 'Valiant',
    'oni press': 'Oni Press',
    'aftershock': 'AfterShock', 'aftershock comics': 'AfterShock',
    'archie': 'Archie', 'archie comics': 'Archie',
    'titan': 'Titan', 'titan comics': 'Titan',
    'vertigo': 'Vertigo',
    'zenescope': 'Zenescope',
    'top cow': 'Top Cow',
}


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def normalize_publisher(pub: str) -> str:
    """Normalize publisher name to canonical form."""
    if not pub:
        return "Independent"
    clean = pub.lower().strip()
    return PUBLISHER_MAP.get(clean, sanitize(pub))


def enrich_from_api(tags: dict) -> dict:
    """Attempt ComicVine API lookup to fill in missing series/publisher info."""
    series = tags.get("series") or tags.get("title", "")
    if not series:
        return tags

    results = _pool.search_chain(series, _SEARCH_ORDER, media_type="comic")
    if not results:
        return tags

    best = results[0]
    merged = dict(tags)
    if best.get('publisher') and not merged.get('publisher'):
        merged['publisher'] = best['publisher']
    if best.get('title') and not merged.get('series'):
        merged['series'] = best['title']
    if best.get('year') and not merged.get('year'):
        merged['year'] = best['year']
    if best.get('cover_url'):
        merged['cover_url'] = best['cover_url']
    merged['_api_enriched'] = True
    return merged


def build_output_path(tags: dict, ext: str) -> Path:
    """Build the final relative path from tags."""
    series = sanitize(tags.get("series") or tags.get("title") or "Unknown Series")
    publisher = normalize_publisher(tags.get("publisher", ""))
    year = tags.get("year") or "0000"
    number = tags.get("number") or 0
    ext_clean = ext.lstrip(".")

    if isinstance(number, str):
        try:
            number = int(float(number))
        except (ValueError, TypeError):
            number = 0

    try:
        rel_path = OUTPUT_FMT.format(
            publisher=publisher, series=series, year=year,
            number=number, ext=ext_clean
        )
    except (KeyError, ValueError):
        rel_path = f"{publisher}/{series}/{series} {number:03d} ({year}).{ext_clean}"

    return Path(rel_path)


def _write_dup(dest_path, final_name, existing_path):
    common.write_dup_sidecar(dest_path, "comicprocessor", final_name, existing_path)


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
                    common.write_reason_sidecar(dest, "comicprocessor", 0, CONFIDENCE_COMIC, {}, {}, reason="no_meta")
                    continue

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "comicprocessor", 0, CONFIDENCE_COMIC, {}, {}, reason="bad_meta")
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # API enrichment when publisher or series is missing
                if not tags.get("publisher") or not tags.get("series"):
                    try:
                        tags = enrich_from_api(tags)
                        if tags.get('_api_enriched'):
                            log.info(f"Enriched {f.name} via ComicVine: {tags.get('publisher')}/{tags.get('series')}")
                    except Exception as e:
                        log.debug(f"API enrichment failed for {f.name}: {e}")

                # Noise learner: always learns (regardless of apply toggle)
                try:
                    canonical = f"{tags.get('publisher', '')} {tags.get('series', '')}".strip()
                    if canonical:
                        NOISE_LEARNER.learn_from_match(
                            f.stem, canonical, f.name,
                            tags.get('series', ''), category="books"
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

                    # Download cover art if available
                    cover_url = tags.get('cover_url', '')
                    if cover_url:
                        download_cover(cover_url, target.parent, "cover.jpg")

                meta_path.unlink(missing_ok=True)

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
