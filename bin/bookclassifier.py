#!/usr/bin/env python3
"""
bookclassifier.py -- Classifies files as Book, Comic, or Manga.
Reads from Processing (files that have .meta.json sidecars from bookpilot),
classifies based on extension, ComicInfo.xml, publisher tags, and filename patterns,
then routes to the correct staged folder.

Classification logic:
  1. .epub/.mobi/.azw/.azw3/.fb2/.lit/.djvu/.txt → Book (definitive)
  2. .cbz/.cbr/.cb7/.cbt + ComicInfo.xml with manga flag → Manga
  3. .cbz/.cbr/.cb7/.cbt + manga publisher → Manga
  4. .cbz/.cbr/.cb7/.cbt + comic publisher → Comic
  5. .cbz/.cbr/.cb7/.cbt + manga filename patterns → Manga
  6. .cbz/.cbr/.cb7/.cbt default → Comic
  7. .pdf → Book (default, ambiguous)
"""

import os
import sys
import time
import json
import re
import shutil
from pathlib import Path

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import BOOK_EXTS, COMIC_EXTS, SCAN_INTERVAL
import common

log, CFG = common.setup_logger("bookclassifier")

INPUT_DIR = Path(CFG['paths']['books_pipeline']['processing'])
STAGED_BOOKS = Path(CFG['paths']['books_pipeline']['staged']['books'])
STAGED_COMICS = Path(CFG['paths']['books_pipeline']['staged']['comics'])
STAGED_MANGA = Path(CFG['paths']['books_pipeline']['staged']['manga'])
REVIEW_DIR = Path(CFG['paths']['books_pipeline']['review'])
FAILED_DIR = Path(CFG['paths']['books_pipeline']['failed'])

# Publisher sets for classification
MANGA_PUBLISHERS = frozenset({
    'viz', 'viz media', 'kodansha', 'kodansha comics', 'shueisha',
    'shogakukan', 'square enix', 'yen press', 'seven seas',
    'seven seas entertainment', 'tokyopop', 'dark horse manga',
    'vertical', 'vertical comics', 'j-novel club', 'one peace books',
    'ghost ship', 'sol press',
})

COMIC_PUBLISHERS = frozenset({
    'dc', 'dc comics', 'marvel', 'marvel comics', 'image',
    'image comics', 'dark horse', 'dark horse comics', 'idw',
    'idw publishing', 'boom! studios', 'boom studios', 'dynamite',
    'dynamite entertainment', 'valiant', 'valiant comics',
    'oni press', 'aftershock', 'aftershock comics', 'archie',
    'archie comics', 'zenescope', 'titan', 'titan comics',
    'top cow', 'wildstorm', 'vertigo', 'icon',
})

# Book-only extensions (never comic/manga)
BOOK_ONLY_EXTS = frozenset({'.epub', '.mobi', '.azw', '.azw3', '.fb2', '.lit', '.djvu', '.txt'})


def classify(file_path: Path, meta: dict) -> str:
    """Returns 'book', 'comic', or 'manga'."""
    ext = file_path.suffix.lower()

    # Rule 1: Book-only extensions are always books
    if ext in BOOK_ONLY_EXTS:
        return "book"

    # Rule 2: Comic archive formats — need further classification
    if ext in COMIC_EXTS:
        comicinfo = meta.get("comicinfo", {})
        tags = meta.get("tags", {})

        # Check manga flag from ComicInfo.xml
        manga_flag = (comicinfo.get("manga") or "").lower()
        if manga_flag in ("yes", "yesandrighttoleft"):
            return "manga"

        # Check publisher
        publisher = (tags.get("publisher") or comicinfo.get("publisher") or "").lower().strip()
        if publisher:
            if publisher in MANGA_PUBLISHERS:
                return "manga"
            if publisher in COMIC_PUBLISHERS:
                return "comic"

        # Check filename patterns for manga
        name = file_path.stem.lower()
        if re.search(r'\bvol\.?\s*\d+', name, re.I):
            return "manga"
        if re.search(r'\bch\.?\s*\d+', name, re.I):
            return "manga"

        # Japanese characters in title suggest manga
        title = tags.get("series") or tags.get("title") or ""
        if re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', title):
            return "manga"

        # Default for comic archives
        return "comic"

    # Rule 3: PDF is ambiguous — default to book
    if ext == ".pdf":
        return "book"

    return "book"


def has_minimum_tags(tags: dict) -> bool:
    """Check if we have enough tag data for useful output.
    Always returns True — all files pass through to processors where
    API enrichment and filename parsing can fill in missing fields."""
    return True


def main():
    for d in [INPUT_DIR, STAGED_BOOKS, STAGED_COMICS, STAGED_MANGA, REVIEW_DIR, FAILED_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR}")

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
                    continue  # bookpilot hasn't processed this yet

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Failed")
                    shutil.move(str(f), str(FAILED_DIR / f.name))
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # Check if tags are sufficient
                if not has_minimum_tags(tags):
                    log.warning(f"Insufficient tags for {f.name}. Moving to Review.")
                    review_dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(review_dest))
                    try:
                        reason = {
                            "source": "bookclassifier",
                            "reason": "insufficient_tags",
                            "detail": f"Missing title/series. Got: title={tags.get('title')!r}, series={tags.get('series')!r}",
                            "tags": tags,
                        }
                        review_dest.with_name(review_dest.name + ".reason.json").write_text(
                            json.dumps(reason, indent=2, ensure_ascii=False), encoding='utf-8')
                    except Exception:
                        pass
                    meta_path.unlink(missing_ok=True)
                    continue

                # Classify
                classification = classify(f, meta)
                if classification == "manga":
                    dest_dir = STAGED_MANGA
                elif classification == "comic":
                    dest_dir = STAGED_COMICS
                else:
                    dest_dir = STAGED_BOOKS

                dest_file = dest_dir / f.name
                dest_meta = dest_dir / meta_path.name

                shutil.move(str(f), str(dest_file))
                shutil.move(str(meta_path), str(dest_meta))
                log.info(f"Classified: {f.name} -> {classification}")

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
