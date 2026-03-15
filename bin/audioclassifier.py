#!/usr/bin/env python3
"""
audioclassifier.py -- Classifies audio files as Music or Audiobook.
Reads from Processing (files that have .meta.json sidecars from musicpilot),
classifies based on extension + genre tags, and routes to the correct staged folder.

Classification logic:
  1. .m4b extension → Audiobook (definitive)
  2. Genre tag contains audiobook/speech/self-help/podcast/spoken → Audiobook
  3. Everything else → Music
"""

import os
import sys
import time
import json
import shutil
from pathlib import Path

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import AUDIO_EXTS, AUDIOBOOK_EXTS, SCAN_INTERVAL
import common

log, CFG = common.setup_logger("audioclassifier")

INPUT_DIR = Path(CFG['paths']['music_pipeline']['processing'])
STAGED_MUSIC = Path(CFG['paths']['music_pipeline']['staged']['music'])
STAGED_AUDIOBOOKS = Path(CFG['paths']['music_pipeline']['staged']['audiobooks'])
REVIEW_DIR = Path(CFG['paths']['music_pipeline']['review'])
FAILED_DIR = Path(CFG['paths']['music_pipeline']['failed'])

AUDIOBOOK_GENRE_KEYWORDS = frozenset({
    'audiobook', 'audio book', 'speech', 'spoken word', 'spoken',
    'self-help', 'self help', 'podcast', 'lecture', 'narration',
    'books & spoken', 'books',
})


def classify(file_path: Path, meta: dict) -> str:
    """Returns 'audiobook' or 'music'."""
    # Rule 1: Extension-based (definitive)
    if file_path.suffix.lower() in AUDIOBOOK_EXTS:
        return "audiobook"

    # Rule 2: Genre-based
    tags = meta.get("tags", {})
    genre = (tags.get("genre") or "").lower().strip()
    if genre:
        for kw in AUDIOBOOK_GENRE_KEYWORDS:
            if kw in genre:
                return "audiobook"

    # Rule 3: Default
    return "music"


def has_minimum_tags(tags: dict) -> bool:
    """Check if we have enough tag data to produce a useful output filename.
    Always returns True — all files pass through to processors where
    API enrichment and filename parsing can fill in missing fields."""
    return True


def main():
    for d in [INPUT_DIR, STAGED_MUSIC, STAGED_AUDIOBOOKS, REVIEW_DIR, FAILED_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR}")

    while True:
        try:
            files = sorted(
                [f for f in INPUT_DIR.iterdir()
                 if f.is_file() and f.suffix.lower() in AUDIO_EXTS],
                key=lambda p: p.stat().st_mtime
            )
            for f in files:
                meta_path = f.with_name(f.name + ".meta.json")
                if not meta_path.exists():
                    continue  # musicpilot hasn't processed this yet

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
                    log.warning(f"Insufficient tags for {f.name} (no artist/title). Moving to Review.")
                    review_dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(review_dest))
                    # Write reason sidecar
                    try:
                        reason = {
                            "source": "audioclassifier",
                            "reason": "insufficient_tags",
                            "detail": f"Missing artist or title tag. Got: artist={tags.get('artist')!r}, title={tags.get('title')!r}",
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
                if classification == "audiobook":
                    dest_dir = STAGED_AUDIOBOOKS
                else:
                    dest_dir = STAGED_MUSIC

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
