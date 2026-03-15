#!/usr/bin/env python3
"""
musicpilot.py -- Audio tag reader and metadata extractor.
Reads audio files from Processing, extracts ID3/Vorbis/MP4 tags via mutagen,
writes a .meta.json sidecar, then moves file + sidecar to the staging area
for classification.
"""

import os
import sys
import time
import json
import shutil
import re
from pathlib import Path
from datetime import datetime, timezone

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import AUDIO_EXTS, SCAN_INTERVAL
import common

log, CFG = common.setup_logger("musicpilot")

# Where audioclassifier picks up files (same as Processing — classifier reads from here)
INPUT_DIR = Path(CFG['paths']['music_pipeline']['processing'])
# Output: we leave files in place and just add .meta.json; audioclassifier reads from same dir
# Actually, to keep the pipeline moving, musicpilot writes meta and stays — classifier picks up
# files that HAVE a .meta.json sidecar.

try:
    import mutagen
    from mutagen.easyid3 import EasyID3
    from mutagen.id3 import ID3
    from mutagen.mp4 import MP4
    from mutagen.flac import FLAC
    from mutagen.oggvorbis import OggVorbis
    from mutagen.oggopus import OggOpus
    HAS_MUTAGEN = True
except ImportError:
    HAS_MUTAGEN = False
    log.warning("mutagen not installed — tag reading disabled, all files go to Review")


def sanitize(s):
    """Remove filesystem-unsafe characters."""
    if not s:
        return s
    return re.sub(r'[\\/*?:"<>|]', '', s).strip()


# ─── YouTube / Web filename noise patterns ───
_MUSIC_NOISE = [
    # Brackets with noise inside (process first to remove whole groups)
    r'\((?:official|lyric(?:al|s)?|audio|hd|hq|4k|1080p|720p|full\s+(?:song|video)|video\s+song)[^)]*\)',
    r'\[(?:official|lyric(?:al|s)?|audio|hd|hq|4k|1080p|720p|full\s+(?:song|video)|video\s+song)[^\]]*\]',
    # Video descriptors
    r'\b(?:official\s+)?(?:music\s+)?video\b',
    r'\b(?:official\s+)?(?:lyric(?:al|s)?)\s*(?:video)?\b',
    r'\bofficial\s+(?:audio|hd|4k)\b',
    r'\b(?:full\s+)?audio\s+song\b',
    r'\bfull\s+(?:song|video|hd)\b',
    r'\b(?:hd|hq|4k|1080p|720p)\b',
    r'\blyrics?\b',
    # YouTube noise
    r'\bvevo\b',
    r'\btopic\b',
    r'\b(?:provided to youtube)\b',
    # Bollywood-specific
    r'\bvideo\s+song\b',
    r'\btitle\s+(?:track|song)\b',
    r'\bmovie\s+song\b',
]
_MUSIC_NOISE_RE = [re.compile(p, re.IGNORECASE) for p in _MUSIC_NOISE]

# Featured artist patterns (extracted, not discarded)
_FEAT_PAREN_RE = re.compile(r'[\(\[]\s*(?:ft\.?|feat\.?)\s+([^\)\]]+)[\)\]]', re.I)
_FEAT_INLINE_RE = re.compile(r'\s+(?:ft\.?|feat\.?)\s+(.+)$', re.I)


def _clean_music_noise(s: str) -> str:
    """Strip YouTube/web noise from a music string."""
    for pat in _MUSIC_NOISE_RE:
        s = pat.sub('', s)
    # Clean leftover empty brackets/parens and whitespace
    s = re.sub(r'\(\s*\)', '', s)
    s = re.sub(r'\[\s*\]', '', s)
    # Clean dangling separators: " - " or " _ " at edges
    s = re.sub(r'\s*[-_]\s*$', '', s)
    s = re.sub(r'^\s*[-_]\s*', '', s)
    # Clean double separators left by removed noise: " _ _ " -> " _ "
    s = re.sub(r'(\s*_\s*){2,}', ' _ ', s)
    s = re.sub(r'\s{2,}', ' ', s)
    return s.strip(' -_|')


def parse_music_filename(stem: str) -> dict:
    """Parse artist/title/album from YouTube/web-style filenames.

    Handles patterns:
      - Artist - Title
      - Title - Full Audio Song _ Album _ Actors _ Composer
      - Title _ Album _ Actors _ Composer
      - Artist - Title (Album)
      - Title (feat. Artist)
      - Artist - Title ft. Featured
    """
    parsed = {"title": "", "artist": "", "album": ""}

    # Clean noise first
    clean = _clean_music_noise(stem)
    if not clean:
        clean = stem  # noise stripping removed everything, use original

    # Extract featured artist (in parens or inline) before splitting
    featured = ""
    feat_match = _FEAT_PAREN_RE.search(clean)
    if feat_match:
        featured = feat_match.group(1).strip()
        clean = clean[:feat_match.start()] + clean[feat_match.end():]
        clean = clean.strip()

    # Pattern 1: Underscore-separated (YouTube Bollywood/Indian style)
    #   "Title _ Album _ Actors _ Composer"  or  "Title _ Album"
    if ' _ ' in clean:
        parts = [p.strip(' -_') for p in clean.split(' _ ') if p.strip(' -_')]

        if len(parts) >= 4:
            # Bollywood: Title _ Album/Movie _ Actors _ Composer
            parsed["title"] = parts[0]
            parsed["album"] = parts[1]
            parsed["artist"] = parts[-1]  # Composer is usually last
        elif len(parts) == 3:
            # Title _ Album _ Artist/Composer
            parsed["title"] = parts[0]
            parsed["album"] = parts[1]
            parsed["artist"] = parts[2]
        elif len(parts) == 2:
            # Check if first part has "Title - Album" compound
            if ' - ' in parts[0]:
                # "Title - Album _ Artist" pattern
                dash_parts = parts[0].split(' - ', 1)
                parsed["title"] = dash_parts[0].strip()
                parsed["album"] = dash_parts[1].strip()
                parsed["artist"] = parts[1]
            else:
                parsed["title"] = parts[0]
                parsed["album"] = parts[1]

        # If title still contains " - ", split out the prefix (song name vs noise)
        if parsed["title"] and ' - ' in parsed["title"]:
            dash_parts = parsed["title"].split(' - ', 1)
            parsed["title"] = dash_parts[0].strip()

    # Pattern 2: Dash-separated "Artist - Title"
    elif ' - ' in clean:
        parts = clean.split(' - ', 1)
        parsed["artist"] = parts[0].strip()
        title_part = parts[1].strip()

        # Check for inline feat in the title portion
        inline_feat = _FEAT_INLINE_RE.search(title_part)
        if inline_feat and not featured:
            featured = inline_feat.group(1).strip()
            title_part = title_part[:inline_feat.start()].strip()

        parsed["title"] = title_part

    # Pattern 3: Just a title
    else:
        # Check for inline feat
        inline_feat = _FEAT_INLINE_RE.search(clean)
        if inline_feat and not featured:
            featured = inline_feat.group(1).strip()
            clean = clean[:inline_feat.start()].strip()
        parsed["title"] = clean

    # Extract album from parentheses in title if not yet found
    if not parsed["album"] and parsed["title"]:
        album_match = re.search(r'\(([^)]+)\)\s*$', parsed["title"])
        if album_match:
            candidate = album_match.group(1).strip()
            # Don't treat "Remix", "Acoustic", etc. as album names
            if not re.match(r'(?:remix|acoustic|live|cover|karaoke|instrumental|unplugged|deluxe|remaster)', candidate, re.I):
                parsed["album"] = candidate
                parsed["title"] = parsed["title"][:album_match.start()].strip()

    # Append featured artist
    if featured and parsed["artist"]:
        parsed["artist"] = f"{parsed['artist']} feat. {featured}"
    elif featured:
        parsed["artist"] = featured

    # Clean all fields
    for k in parsed:
        parsed[k] = sanitize(parsed[k]) or ""

    return parsed


def read_tags(file_path: Path) -> dict:
    """Extract audio tags from any supported format. Returns a normalized dict."""
    tags = {
        "title": "",
        "artist": "",
        "album": "",
        "album_artist": "",
        "year": "",
        "track": 0,
        "genre": "",
        "disc": 1,
        "duration_seconds": 0,
    }

    if not HAS_MUTAGEN:
        return tags

    try:
        audio = mutagen.File(str(file_path), easy=True)
        if audio is None:
            return tags

        # Duration
        if hasattr(audio, 'info') and audio.info:
            tags["duration_seconds"] = int(getattr(audio.info, 'length', 0) or 0)

        # EasyID3-style access (works for MP3, FLAC, OGG, etc. with easy=True)
        def _get(key):
            val = audio.get(key)
            if val:
                if isinstance(val, list):
                    return str(val[0]).strip()
                return str(val).strip()
            return ""

        tags["title"] = _get("title")
        tags["artist"] = _get("artist")
        tags["album"] = _get("album")
        tags["album_artist"] = _get("albumartist") or _get("artist")
        tags["genre"] = _get("genre")

        # Year: try 'date' first, then 'year'
        year_raw = _get("date") or _get("year")
        if year_raw:
            m = re.search(r'(19|20)\d{2}', year_raw)
            tags["year"] = m.group(0) if m else year_raw[:4]

        # Track number
        track_raw = _get("tracknumber")
        if track_raw:
            # Handle "3/12" format
            track_str = track_raw.split("/")[0].strip()
            try:
                tags["track"] = int(track_str)
            except ValueError:
                pass

        # Disc number
        disc_raw = _get("discnumber")
        if disc_raw:
            disc_str = disc_raw.split("/")[0].strip()
            try:
                tags["disc"] = int(disc_str)
            except ValueError:
                pass

    except Exception as e:
        log.warning(f"Tag read error for {file_path.name}: {e}")

    # If embedded tags are sparse, parse the filename for metadata
    has_artist = bool(tags["artist"])
    has_title = bool(tags["title"])

    if not has_artist or not has_title:
        fn = parse_music_filename(file_path.stem)
        if not has_title and fn["title"]:
            tags["title"] = fn["title"]
            tags["_from_filename"] = True
        if not has_artist and fn["artist"]:
            tags["artist"] = fn["artist"]
            tags["_from_filename"] = True
        if not tags["album"] and fn["album"]:
            tags["album"] = fn["album"]
            tags["_from_filename"] = True

    # Last resort: raw filename stem as title
    if not tags["title"]:
        tags["title"] = file_path.stem

    return tags


def write_meta(file_path: Path, tags: dict):
    """Write a .meta.json sidecar next to the audio file."""
    meta_path = file_path.with_name(file_path.name + ".meta.json")
    data = {
        "source": "musicpilot",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tags": tags,
        "original_name": file_path.name,
    }
    meta_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    return meta_path


# Album batch settling: wait for all album tracks to arrive before processing.
# Tracks are grouped by album tag; if no new files arrive for SETTLE_CYCLES
# consecutive scans, the batch is considered complete and meta is written.
SETTLE_CYCLES = 3  # Number of stable scans before writing meta (SETTLE_CYCLES * SCAN_INTERVAL seconds)


def main():
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Service Started. Watching: {INPUT_DIR}")

    # pending_albums: { album_key: { "files": {path: tags}, "stable_count": int } }
    pending_albums = {}
    # Singles/untagged go straight through
    last_file_set = set()

    while True:
        try:
            files = sorted(
                [f for f in INPUT_DIR.iterdir()
                 if f.is_file() and f.suffix.lower() in AUDIO_EXTS],
                key=lambda p: p.stat().st_mtime
            )
            current_file_set = set()

            # Phase 1: Read tags for new files, group by album
            new_files = []
            for f in files:
                meta_path = f.with_name(f.name + ".meta.json")
                if meta_path.exists():
                    continue  # Already processed
                current_file_set.add(str(f))
                if str(f) not in last_file_set:
                    new_files.append(f)

            for f in new_files:
                log.info(f"Reading tags: {f.name}")
                tags = read_tags(f)

                album = tags.get("album", "")
                artist = tags.get("album_artist") or tags.get("artist", "")
                album_key = f"{artist}||{album}" if album else ""

                if album_key:
                    # Album track: batch it
                    if album_key not in pending_albums:
                        pending_albums[album_key] = {"files": {}, "stable_count": 0}
                    pending_albums[album_key]["files"][str(f)] = tags
                    pending_albums[album_key]["stable_count"] = 0  # Reset: new file arrived
                    log.info(f"Batched: {f.name} -> album '{album}' ({len(pending_albums[album_key]['files'])} tracks)")
                else:
                    # No album tag (single/unknown): write meta immediately
                    write_meta(f, tags)
                    log.info(f"Meta written (single): artist={tags['artist']}, title={tags['title']}")

            # Phase 2: Check album batches for stability
            completed_keys = []
            for album_key, batch in pending_albums.items():
                # Check if any files were removed (moved by another service)
                batch["files"] = {p: t for p, t in batch["files"].items() if Path(p).exists()}
                if not batch["files"]:
                    completed_keys.append(album_key)
                    continue

                # Check if new files arrived for this album this cycle
                batch_paths = set(batch["files"].keys())
                if batch_paths.issubset(last_file_set):
                    batch["stable_count"] += 1
                else:
                    batch["stable_count"] = 0

                # Settled: no new files for SETTLE_CYCLES consecutive scans
                if batch["stable_count"] >= SETTLE_CYCLES:
                    track_count = len(batch["files"])
                    log.info(f"Album settled: '{album_key.split('||')[1]}' ({track_count} tracks)")
                    for fpath, tags in batch["files"].items():
                        fp = Path(fpath)
                        if fp.exists():
                            write_meta(fp, tags)
                    completed_keys.append(album_key)

            for key in completed_keys:
                pending_albums.pop(key, None)

            last_file_set = current_file_set

        except Exception as e:
            log.error(f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
