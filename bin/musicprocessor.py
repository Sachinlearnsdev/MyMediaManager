#!/usr/bin/env python3
"""
musicprocessor.py -- Final music file processor.
Reads tagged audio files from Staged/Music, builds the final library path
using the output format template, and moves files to Library/Music/.

Output format (from config): {artist}/{album} ({year})/{track:02d} - {title}.{ext}
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

from bin.constants import AUDIO_EXTS, SCAN_INTERVAL, CONFIDENCE_MUSIC
from bin.media_sources import SourcePool, download_cover
from bin.noise_learner import NoiseLearner
import common

log, CFG = common.setup_logger("musicprocessor")
NOISE_LEARNER = NoiseLearner(CFG)

INPUT_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('staged', {}).get('music', ''))
OUTPUT_DIR = Path(CFG.get('paths', {}).get('output', {}).get('music', ''))
REVIEW_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('review', ''))
DUP_DIR = Path(CFG.get('paths', {}).get('music_pipeline', {}).get('duplicates', 'Duplicates/Music'))
OUTPUT_FMT = CFG.get('output_formats', {}).get('music', '{artist}/{album} ({year})/{track:02d} - {title}.{ext}')

# API enrichment (MusicBrainz primary, AcoustID fingerprint, Spotify/LastFM enrichment)
_CACHE_DIR = Path(CFG['paths'].get('cache', {}).get('root', 'cache')) if isinstance(CFG['paths'].get('cache'), dict) else Path(CFG.get('cache', {}).get('root', 'cache'))
_SEARCH_ORDER = CFG.get('api_config', {}).get('music', {}).get('search', ['musicbrainz'])
_FINGERPRINT_ORDER = CFG.get('api_config', {}).get('music', {}).get('fingerprint', ['acoustid'])
_pool = SourcePool(CFG, _CACHE_DIR / "api")


# Audio quality ranking: higher = better
_QUALITY_RANK = {
    '.flac': 100, '.alac': 95, '.wav': 90, '.aiff': 90,
    '.ape': 85, '.wv': 85,
    '.m4a': 70, '.aac': 65, '.ogg': 60, '.opus': 60,
    '.mp3': 50, '.wma': 40,
    '.m4b': 30,  # audiobook format, lower priority for music
}


def _quality_score(file_path: Path) -> int:
    """Get quality score for a file based on format and size."""
    ext = file_path.suffix.lower()
    base = _QUALITY_RANK.get(ext, 30)
    # Larger files within same format = higher quality (higher bitrate)
    try:
        size_mb = file_path.stat().st_size / (1024 * 1024)
        # Add up to 20 bonus points based on size (caps at 100MB)
        base += min(int(size_mb / 5), 20)
    except OSError:
        pass
    return base


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return "Unknown"
    s = re.sub(r'[\\/*?:"<>|]', '', s)
    return re.sub(r'\s+', ' ', s).strip() or "Unknown"


def enrich_from_api(tags: dict, file_path: Path = None) -> dict:
    """Attempt MusicBrainz/AcoustID lookup to fill in missing metadata."""
    artist = tags.get("artist") or tags.get("album_artist", "")
    title = tags.get("title", "")
    album = tags.get("album", "")

    merged = dict(tags)

    # Strategy 1: MusicBrainz structured search (artist + title)
    from_filename = tags.get('_from_filename', False)
    if artist and title:
        mb = _pool.get('musicbrainz')
        if mb:
            try:
                results = mb.search_recording(artist, title)

                # Strategy 1b: If filename-parsed and no results, try swapped
                # (filename parser can't always tell artist from title)
                if not results and from_filename:
                    log.debug(f"No MB results for {artist}/{title}, trying swapped")
                    results = mb.search_recording(title, artist)
                    if results:
                        # Swap worked — the filename had artist/title reversed
                        merged['artist'] = title
                        merged['title'] = artist
                        artist, title = title, artist

                if results:
                    best = results[0]
                    # When data came from filename, prefer API values over guesses
                    if from_filename:
                        if best.get('artist'):
                            merged['artist'] = best['artist']
                        if best.get('title'):
                            merged['title'] = best['title']
                    if not merged.get('album') and best.get('album'):
                        merged['album'] = best['album']
                    if not merged.get('year') and best.get('year'):
                        merged['year'] = best['year']
                    if best.get('mbid'):
                        merged['mbid'] = best['mbid']
                    merged['_api_enriched'] = True
                    return merged
            except Exception as e:
                log.debug(f"MusicBrainz search failed: {e}")

    # Strategy 2: AcoustID fingerprint (works even with wrong/no metadata)
    if file_path:
        for fp_name in _FINGERPRINT_ORDER:
            fp_client = _pool.get(fp_name)
            if fp_client and hasattr(fp_client, 'generate_fingerprint'):
                try:
                    fingerprint, duration = fp_client.generate_fingerprint(file_path)
                    if fingerprint and duration:
                        results = fp_client.lookup(fingerprint, duration)
                        if results:
                            best = results[0]
                            if best.get('artist') and not merged.get('artist'):
                                merged['artist'] = best['artist']
                            if best.get('title') and not merged.get('title'):
                                merged['title'] = best['title']
                            if best.get('album') and not merged.get('album'):
                                merged['album'] = best['album']
                            if best.get('mbid'):
                                merged['mbid'] = best['mbid']
                            merged['_api_enriched'] = True
                            return merged
                except Exception as e:
                    log.debug(f"AcoustID lookup failed: {e}")

    return merged


def build_output_path(tags: dict, ext: str) -> Path:
    """Build the final relative path from tags using the output format template."""
    artist = sanitize(tags.get("album_artist") or tags.get("artist") or "Unknown Artist")
    album = sanitize(tags.get("album") or "Unknown Album")
    year = tags.get("year") or "0000"
    title = sanitize(tags.get("title") or "Unknown")
    track = tags.get("track") or 0
    disc = tags.get("disc") or 1

    # Strip leading dot from extension
    ext_clean = ext.lstrip(".")

    try:
        rel_path = OUTPUT_FMT.format(
            artist=artist, album=album, year=year,
            title=title, track=track, disc=disc, ext=ext_clean
        )
    except (KeyError, ValueError):
        # Fallback if format string has issues
        rel_path = f"{artist}/{album} ({year})/{track:02d} - {title}.{ext_clean}"

    return Path(rel_path)


def _write_dup(dest_path, final_name, existing_path):
    """Write a .dup.json sidecar next to a file moved to Duplicates."""
    common.write_dup_sidecar(dest_path, "musicprocessor", final_name, existing_path)


def main():
    for d in [INPUT_DIR, OUTPUT_DIR, REVIEW_DIR, DUP_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    log.info(f"Service Started. Watching: {INPUT_DIR} -> Library: {OUTPUT_DIR}")

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
                    log.warning(f"No meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "musicprocessor", 0, CONFIDENCE_MUSIC, {}, {}, reason="no_meta")
                    continue

                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                except Exception:
                    log.warning(f"Bad meta for {f.name}, moving to Review")
                    dest = REVIEW_DIR / f.name
                    shutil.move(str(f), str(dest))
                    common.write_reason_sidecar(dest, "musicprocessor", 0, CONFIDENCE_MUSIC, {}, {}, reason="bad_meta")
                    meta_path.unlink(missing_ok=True)
                    continue

                tags = meta.get("tags", {})

                # API enrichment when key fields are missing
                if not tags.get("artist") or not tags.get("title") or not tags.get("album"):
                    try:
                        tags = enrich_from_api(tags, file_path=f)
                        if tags.get('_api_enriched'):
                            log.info(f"Enriched {f.name} via API: {tags.get('artist')}/{tags.get('title')}")
                    except Exception as e:
                        log.debug(f"API enrichment failed for {f.name}: {e}")

                # Noise learner: always learns (regardless of apply toggle)
                try:
                    canonical = f"{tags.get('artist', '')} {tags.get('title', '')}".strip()
                    if canonical:
                        NOISE_LEARNER.learn_from_match(
                            f.stem, canonical, f.name,
                            tags.get('artist', ''), category="music"
                        )
                except Exception:
                    pass

                rel_path = build_output_path(tags, f.suffix)
                target = OUTPUT_DIR / rel_path

                if target.exists():
                    # Quality comparison: keep the better version
                    new_score = _quality_score(f)
                    existing_score = _quality_score(target)
                    DUP_DIR.mkdir(parents=True, exist_ok=True)

                    if new_score > existing_score:
                        # New file is better quality — replace existing
                        log.info(f"Upgrade: {f.name} ({f.suffix}, score={new_score}) > existing ({target.suffix}, score={existing_score})")
                        dup_dest = DUP_DIR / target.name
                        shutil.move(str(target), str(dup_dest))
                        _write_dup(dup_dest, str(rel_path), f)
                        shutil.move(str(f), str(target))
                        log.info(f"REPLACED -> {target}")
                    else:
                        # Existing is same or better — send new to duplicates
                        log.warning(f"Duplicate (keeping existing): {rel_path} (new={new_score}, existing={existing_score})")
                        dup_dest = DUP_DIR / f.name
                        shutil.move(str(f), str(dup_dest))
                        _write_dup(dup_dest, str(rel_path), target)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(f), str(target))
                    log.info(f"MOVED -> {target}")

                    # Download cover art if available and not already present
                    cover_url = tags.get('cover_url', '')
                    if cover_url:
                        download_cover(cover_url, target.parent)

                # Clean up meta sidecar
                meta_path.unlink(missing_ok=True)

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
