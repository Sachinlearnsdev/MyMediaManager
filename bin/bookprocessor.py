#!/usr/bin/env python3
"""
bookprocessor.py -- Final book file processor.
Reads classified books from Staged/Books, builds the final library path
using metadata, and moves files to Library/Books/.

Output structure: Library/Books/{author}/{title} ({year}).{ext}
Genre routing: Fiction / Non-Fiction / Technical (when genre data available)
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

from bin.constants import BOOK_EXTS, COMIC_EXTS, SCAN_INTERVAL, CONFIDENCE_BOOK
from bin.media_sources import SourcePool, download_cover
from bin.noise_learner import NoiseLearner
import common

log, CFG = common.setup_logger("bookprocessor")
NOISE_LEARNER = NoiseLearner(CFG)

INPUT_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('staged', {}).get('books', ''))
OUTPUT_DIR = Path(CFG.get('paths', {}).get('output', {}).get('books', ''))
REVIEW_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('review', ''))
DUP_DIR = Path(CFG.get('paths', {}).get('books_pipeline', {}).get('duplicates', 'Duplicates/Books'))
OUTPUT_FMT = CFG.get('output_formats', {}).get('books', '{author}/{title} ({year}).{ext}')

# API enrichment
_CACHE_DIR = Path(CFG['paths'].get('cache', {}).get('root', 'cache')) if isinstance(CFG['paths'].get('cache'), dict) else Path(CFG.get('cache', {}).get('root', 'cache'))
_SEARCH_ORDER = CFG.get('api_config', {}).get('books', {}).get('search', ['openlibrary', 'googlebooks'])
_pool = SourcePool(CFG, _CACHE_DIR / "api")

# Genre classification keywords
FICTION_KEYWORDS = frozenset({
    'fiction', 'novel', 'literary', 'sci-fi', 'science fiction', 'fantasy',
    'mystery', 'thriller', 'romance', 'horror', 'adventure', 'dystopian',
    'young adult', 'ya', 'children', 'juvenile', 'fable', 'fairy tale',
})

TECHNICAL_KEYWORDS = frozenset({
    'programming', 'engineering', 'mathematics', 'computer science',
    'computer', 'software', 'algorithm', 'database', 'machine learning',
    'artificial intelligence', 'networking', 'security', 'devops',
    'academic', 'textbook', 'reference', 'technical',
})

NONFICTION_KEYWORDS = frozenset({
    'biography', 'autobiography', 'memoir', 'history', 'science',
    'self-help', 'business', 'travel', 'cooking', 'health', 'politics',
    'philosophy', 'psychology', 'economics', 'sociology', 'religion',
    'true crime', 'journalism', 'essay', 'non-fiction', 'nonfiction',
})


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def detect_genre(tags: dict) -> str:
    """Detect genre category from subjects/genre tags."""
    subjects = tags.get("subjects", [])
    genre_text = " ".join(subjects).lower() if subjects else ""
    # Also check subject field (from PDF)
    if tags.get("subject"):
        genre_text += " " + tags["subject"].lower()

    if not genre_text:
        return ""

    for kw in TECHNICAL_KEYWORDS:
        if kw in genre_text:
            return "Technical"
    for kw in FICTION_KEYWORDS:
        if kw in genre_text:
            return "Fiction"
    for kw in NONFICTION_KEYWORDS:
        if kw in genre_text:
            return "Non-Fiction"

    return ""


def enrich_from_api(tags: dict) -> dict:
    """Attempt API lookup to fill in missing metadata fields."""
    title = tags.get("title", "")
    author = tags.get("author", "")
    isbn = tags.get("isbn", "")

    if not title and not isbn:
        return tags

    # Try ISBN lookup first (exact match)
    if isbn:
        for src_name in _SEARCH_ORDER:
            src = _pool.get(src_name)
            if src and hasattr(src, 'search_by_isbn'):
                result = src.search_by_isbn(isbn)
                if result:
                    return _merge_tags(tags, result)

    # Text search: "title author"
    query = f"{title} {author}".strip()
    if query:
        results = _pool.search_chain(query, _SEARCH_ORDER, media_type="book")
        if results:
            return _merge_tags(tags, results[0])

    return tags


def _merge_tags(tags: dict, api_result: dict) -> dict:
    """Merge API result into tags, preferring existing non-empty values."""
    merged = dict(tags)
    field_map = {
        'author': 'author',
        'title': 'title',
        'year': 'year',
        'isbn': 'isbn',
        'subjects': 'subjects',
        'publisher': 'publisher',
    }
    for api_key, tag_key in field_map.items():
        api_val = api_result.get(api_key)
        if api_val and not merged.get(tag_key):
            merged[tag_key] = api_val
    # If subjects came from API but not in tags, add them
    if api_result.get('subjects') and not merged.get('subjects'):
        merged['subjects'] = api_result['subjects']
    merged['_api_enriched'] = True
    return merged


def build_output_path(tags: dict, ext: str) -> Path:
    """Build the final relative path from tags."""
    author = sanitize(tags.get("author") or "Unknown Author")
    title = sanitize(tags.get("title") or "Unknown")
    year = tags.get("year") or "0000"
    ext_clean = ext.lstrip(".")

    genre = detect_genre(tags)

    try:
        rel_path = OUTPUT_FMT.format(
            author=author, title=title, year=year, ext=ext_clean
        )
    except (KeyError, ValueError):
        rel_path = f"{author}/{title} ({year}).{ext_clean}"

    # Prepend genre folder if detected
    if genre:
        return Path(genre) / rel_path
    return Path(rel_path)


def _write_dup(dest_path, final_name, existing_path):
    common.write_dup_sidecar(dest_path, "bookprocessor", final_name, existing_path)


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
                    common.write_reason_sidecar(dest, "bookprocessor", 0, CONFIDENCE_BOOK, {}, {}, reason="no_meta")
                    continue

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "bookprocessor", 0, CONFIDENCE_BOOK, {}, {}, reason="bad_meta")
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # API enrichment when metadata is sparse
                if not tags.get("author") or not tags.get("title") or tags.get("title") == "Unknown":
                    try:
                        tags = enrich_from_api(tags)
                        if tags.get('_api_enriched'):
                            log.info(f"Enriched {f.name} via API: {tags.get('author')}/{tags.get('title')}")
                    except Exception as e:
                        log.debug(f"API enrichment failed for {f.name}: {e}")

                # Noise learner: always learns (regardless of apply toggle)
                try:
                    canonical = f"{tags.get('author', '')} {tags.get('title', '')}".strip()
                    if canonical:
                        NOISE_LEARNER.learn_from_match(
                            f.stem, canonical, f.name,
                            tags.get('author', ''), category="books"
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
