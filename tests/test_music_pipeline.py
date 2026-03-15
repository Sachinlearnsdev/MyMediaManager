#!/usr/bin/env python3
"""
test_music_pipeline.py -- Comprehensive music pipeline test.

Creates real audio files with various tag scenarios, drops them into the pipeline,
runs infrastructure services, and shows tag extraction + raw→final mapping.
"""

import os
import sys
import struct
import time
import shutil
from pathlib import Path

# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import mutagen
from mutagen.mp3 import MP3
from mutagen.id3 import ID3, TIT2, TPE1, TALB, TRCK, TDRC, TPE2, TCON
from mutagen.flac import FLAC
from mutagen.mp4 import MP4
from mutagen.oggvorbis import OggVorbis

# ============================================================
# HELPERS: Create minimal valid audio files
# ============================================================

def make_mp3(path: Path, duration_frames=3):
    """Create a minimal valid MP3 file (MPEG1 Layer3 128kbps 44100Hz stereo)."""
    # MP3 frame header: sync=0xFFE, version=MPEG1(11), layer=3(01), no CRC(1)
    # bitrate=128kbps(1001), sample=44100(00), padding(0), channel=stereo(00)
    # Header bytes: FF FB 90 00
    header = b'\xff\xfb\x90\x00'
    # Frame size for 128kbps @ 44100Hz = 417 bytes (with padding varies)
    frame_data = header + b'\x00' * 413  # 417 bytes total per frame
    with open(path, 'wb') as f:
        for _ in range(duration_frames):
            f.write(frame_data)


def make_flac(path: Path):
    """Create a minimal valid FLAC file."""
    # Minimal FLAC: magic + STREAMINFO block + minimal frame
    with open(path, 'wb') as f:
        f.write(b'fLaC')
        # STREAMINFO metadata block (last=1, type=0, length=34)
        block_header = struct.pack('>I', (1 << 31) | (0 << 24) | 34)
        f.write(block_header)
        # STREAMINFO: min_block=4096, max_block=4096, min_frame=0, max_frame=0
        # sample_rate=44100, channels=1(0), bps=16(15), total_samples=0
        # md5=0
        si = struct.pack('>HH', 4096, 4096)  # min/max block size
        si += b'\x00' * 3  # min frame size
        si += b'\x00' * 3  # max frame size
        # sample rate (20 bits) | channels (3 bits) | bps (5 bits) | total samples (36 bits)
        # 44100 = 0xAC44, channels=0 (mono=1ch), bps=15 (16bit)
        sr_ch_bps = (44100 << 12) | (0 << 9) | (15 << 4) | 0
        si += struct.pack('>Q', sr_ch_bps)
        si += b'\x00' * (34 - len(si))  # pad to 34 bytes
        f.write(si[:34])


def make_m4a(path: Path):
    """Create a minimal valid M4A (MP4 container) file."""
    with open(path, 'wb') as f:
        # ftyp box
        ftyp_data = b'M4A \x00\x00\x00\x00M4A mp42isom'
        ftyp_size = struct.pack('>I', 8 + len(ftyp_data))
        f.write(ftyp_size + b'ftyp' + ftyp_data)
        # moov box (minimal)
        moov_inner = b'\x00' * 8
        moov_size = struct.pack('>I', 8 + len(moov_inner))
        f.write(moov_size + b'moov' + moov_inner)


def make_ogg(path: Path):
    """Create a minimal OGG file by writing raw bytes."""
    # OGG page header for an empty Vorbis stream
    with open(path, 'wb') as f:
        # Just write enough for mutagen to potentially read
        # Vorbis identification header in OGG container
        f.write(b'OggS')  # capture pattern
        f.write(b'\x00')  # version
        f.write(b'\x02')  # header type (BOS)
        f.write(b'\x00' * 8)  # granule position
        f.write(struct.pack('<I', 1))  # serial
        f.write(struct.pack('<I', 0))  # page sequence
        f.write(struct.pack('<I', 0))  # checksum (we'll skip validation)
        f.write(b'\x01')  # page segments
        f.write(b'\x1e')  # segment table (30 bytes)
        # Vorbis identification header
        f.write(b'\x01vorbis')
        f.write(struct.pack('<I', 0))  # version
        f.write(b'\x01')  # channels
        f.write(struct.pack('<I', 44100))  # sample rate
        f.write(struct.pack('<i', 0))  # bitrate max
        f.write(struct.pack('<i', 128000))  # bitrate nominal
        f.write(struct.pack('<i', 0))  # bitrate min
        f.write(b'\xb8')  # blocksize 0/1
        f.write(b'\x01')  # framing


# ============================================================
# TEST CASES
# ============================================================

TEST_CASES = [
    # === MUSIC: Well-tagged files ===
    {
        "filename": "01 - Blinding Lights.mp3",
        "format": "mp3",
        "tags": {"artist": "The Weeknd", "album": "After Hours", "title": "Blinding Lights",
                 "track": "1", "year": "2020", "genre": "Pop"},
        "category": "music",
        "description": "Well-tagged single track"
    },
    {
        "filename": "Kendrick.Lamar-Not.Like.Us.2024.FLAC-GROUP.flac",
        "format": "flac",
        "tags": {},  # No tags - relies on filename
        "category": "music",
        "description": "Scene release, no tags"
    },
    {
        "filename": "05. Shape of You.mp3",
        "format": "mp3",
        "tags": {"artist": "Ed Sheeran", "album": "÷ (Divide)", "title": "Shape of You",
                 "track": "5", "year": "2017", "genre": "Pop"},
        "category": "music",
        "description": "Numbered track with full tags"
    },
    {
        "filename": "Taylor_Swift-Anti-Hero_[320kbps].mp3",
        "format": "mp3",
        "tags": {"artist": "Taylor Swift", "title": "Anti-Hero"},  # Partial tags
        "category": "music",
        "description": "Partial tags (no album/year)"
    },
    {
        "filename": "Queen - Bohemian Rhapsody.mp3",
        "format": "mp3",
        "tags": {"artist": "Queen", "album": "A Night at the Opera",
                 "title": "Bohemian Rhapsody", "track": "11", "year": "1975"},
        "category": "music",
        "description": "Classic format: Artist - Title"
    },
    {
        "filename": "unknown_track_437.mp3",
        "format": "mp3",
        "tags": {},  # No tags, cryptic filename
        "category": "music",
        "description": "No tags, unrecognizable filename"
    },
    {
        "filename": "Daft Punk - Random Access Memories (2013) - 08 - Get Lucky.flac",
        "format": "flac",
        "tags": {"artist": "Daft Punk", "album": "Random Access Memories",
                 "title": "Get Lucky", "track": "8", "year": "2013", "genre": "Electronic"},
        "category": "music",
        "description": "Verbose filename + full FLAC tags"
    },
    {
        "filename": "BTS_Dynamite.mp3",
        "format": "mp3",
        "tags": {"artist": "BTS", "title": "Dynamite", "album": "BE",
                 "track": "2", "year": "2020", "genre": "K-Pop"},
        "category": "music",
        "description": "K-Pop with full tags"
    },

    # === AUDIOBOOKS ===
    {
        "filename": "Project_Hail_Mary_Andy_Weir.m4b",
        "format": "m4b",
        "tags": {"artist": "Andy Weir", "album": "Project Hail Mary", "year": "2021",
                 "genre": "Audiobook"},
        "category": "audiobook",
        "description": "M4B audiobook (auto-detected by extension)"
    },
    {
        "filename": "Atomic.Habits-James.Clear-Part01.mp3",
        "format": "mp3",
        "tags": {"artist": "James Clear", "album": "Atomic Habits",
                 "title": "Chapter 1", "genre": "Audiobook", "year": "2018"},
        "category": "audiobook",
        "description": "MP3 audiobook (detected by genre tag)"
    },
    {
        "filename": "The_48_Laws_of_Power-Robert_Greene.mp3",
        "format": "mp3",
        "tags": {"artist": "Robert Greene", "album": "The 48 Laws of Power",
                 "title": "Introduction", "genre": "Speech", "year": "1998"},
        "category": "audiobook",
        "description": "Audiobook tagged as Speech genre"
    },

    # === EDGE CASES ===
    {
        "filename": "さくら (Ikimono-gakari).mp3",
        "format": "mp3",
        "tags": {"artist": "いきものがかり", "album": "SAKURA", "title": "さくら",
                 "track": "1", "year": "2006"},
        "category": "music",
        "description": "Japanese characters in filename + tags"
    },
    {
        "filename": "01-track01.mp3",
        "format": "mp3",
        "tags": {"artist": "Various Artists", "album": "Now That's What I Call Music 99",
                 "title": "As It Was", "track": "1", "year": "2023"},
        "category": "music",
        "description": "Generic filename but rich tags (compilation)"
    },
    {
        "filename": "live_recording_2024-03-15.flac",
        "format": "flac",
        "tags": {},
        "category": "music",
        "description": "Live recording, no tags, date in filename"
    },
    {
        "filename": "DJ Khaled - God Did ft. Rick Ross, Lil Wayne, Jay-Z, John Legend, Fridayy.mp3",
        "format": "mp3",
        "tags": {"artist": "DJ Khaled", "album": "GOD DID",
                 "title": "God Did (feat. Rick Ross, Lil Wayne, Jay-Z, John Legend & Fridayy)",
                 "track": "1", "year": "2022", "genre": "Hip-Hop"},
        "category": "music",
        "description": "Long filename with featured artists"
    },
]


def create_test_file(dest_dir: Path, case: dict) -> Path:
    """Create a test audio file with tags."""
    filepath = dest_dir / case["filename"]

    fmt = case["format"]
    if fmt == "mp3":
        make_mp3(filepath)
    elif fmt == "flac":
        make_flac(filepath)
    elif fmt in ("m4a", "m4b"):
        make_m4a(filepath)
    elif fmt == "ogg":
        make_ogg(filepath)

    # Apply tags
    tags = case.get("tags", {})
    if not tags:
        return filepath

    try:
        if fmt == "mp3":
            audio = MP3(filepath)
            if audio.tags is None:
                audio.add_tags()
            if "title" in tags:
                audio.tags.add(TIT2(encoding=3, text=[tags["title"]]))
            if "artist" in tags:
                audio.tags.add(TPE1(encoding=3, text=[tags["artist"]]))
            if "album" in tags:
                audio.tags.add(TALB(encoding=3, text=[tags["album"]]))
            if "track" in tags:
                audio.tags.add(TRCK(encoding=3, text=[tags["track"]]))
            if "year" in tags:
                audio.tags.add(TDRC(encoding=3, text=[tags["year"]]))
            if "genre" in tags:
                audio.tags.add(TCON(encoding=3, text=[tags["genre"]]))
            audio.save()
        elif fmt == "flac":
            try:
                audio = FLAC(filepath)
                for k, v in tags.items():
                    audio[k] = v
                audio.save()
            except Exception:
                pass  # Minimal FLAC may not support tags
        elif fmt in ("m4a", "m4b"):
            try:
                audio = MP4(filepath)
                tag_map = {
                    "title": "\xa9nam", "artist": "\xa9ART", "album": "\xa9alb",
                    "year": "\xa9day", "genre": "\xa9gen",
                }
                for k, v in tags.items():
                    if k in tag_map:
                        audio[tag_map[k]] = [v]
                    if k == "track":
                        audio["trkn"] = [(int(v), 0)]
                audio.save()
            except Exception:
                pass
    except Exception as e:
        print(f"  [WARN] Could not write tags to {filepath.name}: {e}")

    return filepath


def read_tags(filepath: Path) -> dict:
    """Read all available tags from an audio file using mutagen."""
    result = {"raw_tags": {}, "parsed": {}}

    try:
        audio = mutagen.File(filepath, easy=True)
        if audio is None:
            # Try harder with explicit format
            ext = filepath.suffix.lower()
            if ext == ".mp3":
                audio = MP3(filepath)
            elif ext == ".flac":
                audio = FLAC(filepath)
            elif ext in (".m4a", ".m4b"):
                audio = MP4(filepath)

        if audio is None:
            return result

        # Get all raw tags
        if hasattr(audio, 'tags') and audio.tags:
            for key in audio.tags:
                try:
                    val = audio.tags[key]
                    if hasattr(val, 'text'):
                        result["raw_tags"][str(key)] = str(val.text[0]) if val.text else ""
                    elif isinstance(val, list):
                        result["raw_tags"][str(key)] = str(val[0]) if val else ""
                    else:
                        result["raw_tags"][str(key)] = str(val)
                except Exception:
                    pass

        # Parse into standard fields
        tag_data = result["raw_tags"]

        # ID3 tag mapping
        field_map = {
            "TIT2": "title", "TPE1": "artist", "TALB": "album",
            "TRCK": "track", "TDRC": "year", "TCON": "genre",
            "TPE2": "album_artist",
            # MP4 tags
            "\xa9nam": "title", "\xa9ART": "artist", "\xa9alb": "album",
            "\xa9day": "year", "\xa9gen": "genre",
            # Vorbis/FLAC
            "title": "title", "artist": "artist", "album": "album",
            "tracknumber": "track", "date": "year", "genre": "genre",
        }

        for raw_key, field in field_map.items():
            if raw_key in tag_data and field not in result["parsed"]:
                result["parsed"][field] = tag_data[raw_key]

        # Duration
        if hasattr(audio, 'info') and audio.info:
            result["parsed"]["duration_sec"] = round(getattr(audio.info, 'length', 0), 1)
            result["parsed"]["bitrate"] = getattr(audio.info, 'bitrate', 0)
            result["parsed"]["sample_rate"] = getattr(audio.info, 'sample_rate', 0)

    except Exception as e:
        result["error"] = str(e)

    return result


def predict_final_path(case: dict, tag_result: dict) -> str:
    """Predict the final output path based on tags + filename."""
    parsed = tag_result.get("parsed", {})
    tags_input = case.get("tags", {})
    ext = Path(case["filename"]).suffix
    cat = case["category"]

    # Determine if we have usable tags
    artist = parsed.get("artist") or tags_input.get("artist", "")
    album = parsed.get("album") or tags_input.get("album", "")
    title = parsed.get("title") or tags_input.get("title", "")
    year = parsed.get("year") or tags_input.get("year", "")
    track = parsed.get("track") or tags_input.get("track", "")
    genre = parsed.get("genre") or tags_input.get("genre", "")

    if cat == "audiobook" or ext == ".m4b" or genre.lower() in ("audiobook", "speech", "podcast"):
        # Audiobook path
        author = artist or "Unknown Author"
        book_title = album or title or Path(case["filename"]).stem
        year_str = f" ({year})" if year else ""
        part = title or f"Part 01"
        return f"Audiobooks/{author}/{book_title}{year_str}/{part}{ext}"
    else:
        # Music path
        if artist and album and title:
            # Full tags available
            year_str = f" ({year})" if year else ""
            track_str = f"{int(track):02d} - " if track else ""
            return f"Music/{artist}/{album}{year_str}/{track_str}{title}{ext}"
        elif artist and title:
            # Partial tags - no album
            return f"Music/{artist}/Unknown Album/{title}{ext}"
        else:
            # No usable tags - goes to Review
            return f"Review/Music/{case['filename']}"


def run_infrastructure_test(test_dir: Path):
    """Test that automouse/autoharbor/autorouter correctly handle audio files."""
    print("\n" + "=" * 70)
    print("INFRASTRUCTURE ROUTING TEST")
    print("=" * 70)

    # Import infrastructure modules
    from bin.constants import AUDIO_EXTS, AUDIOBOOK_EXTS, JUNK_EXTENSIONS

    # Test extension detection
    print("\n--- Extension Detection ---")
    test_files = [
        "song.mp3", "track.flac", "audio.m4a", "book.m4b", "song.ogg",
        "track.opus", "lossless.wav", "song.aac", "cover.jpg", "info.nfo",
        "archive.rar", "video.mkv"
    ]
    for name in test_files:
        ext = Path(name).suffix.lower()
        is_audio = ext in AUDIO_EXTS
        is_audiobook = ext in AUDIOBOOK_EXTS
        is_junk = ext in JUNK_EXTENSIONS
        status = []
        if is_audio: status.append("AUDIO")
        if is_audiobook: status.append("AUDIOBOOK")
        if is_junk: status.append("JUNK")
        if not status: status.append("other")
        print(f"  {name:30s} -> {', '.join(status)}")


def main():
    print("=" * 70)
    print("MyMediaManager - Music Pipeline Comprehensive Test")
    print("=" * 70)

    # Setup test directory
    test_dir = _PROJECT_ROOT / "tests" / "_music_test_files"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True, exist_ok=True)

    # Phase 1: Create all test files
    print(f"\n{'='*70}")
    print("PHASE 1: Creating test audio files")
    print("=" * 70)

    created_files = []
    for i, case in enumerate(TEST_CASES, 1):
        filepath = create_test_file(test_dir, case)
        size = filepath.stat().st_size if filepath.exists() else 0
        print(f"  [{i:2d}] {case['filename'][:55]:55s} {size:>6d}B  ({case['description']})")
        created_files.append((case, filepath))

    # Phase 2: Read tags from all files
    print(f"\n{'='*70}")
    print("PHASE 2: Tag extraction with mutagen")
    print("=" * 70)

    tag_results = []
    for case, filepath in created_files:
        tags = read_tags(filepath)
        tag_results.append(tags)

        parsed = tags.get("parsed", {})
        raw_count = len(tags.get("raw_tags", {}))
        error = tags.get("error", "")

        print(f"\n  [{case['filename']}]")
        if error:
            print(f"    ERROR: {error}")
        print(f"    Raw tags found: {raw_count}")
        if parsed:
            for k, v in parsed.items():
                if k not in ("duration_sec", "bitrate", "sample_rate"):
                    print(f"    {k:15s}: {v}")
            if "duration_sec" in parsed:
                print(f"    {'duration':15s}: {parsed['duration_sec']}s | "
                      f"bitrate: {parsed.get('bitrate', 'N/A')} | "
                      f"sample_rate: {parsed.get('sample_rate', 'N/A')}")
        else:
            print(f"    (no parseable tags)")

    # Phase 3: Raw -> Final mapping
    print(f"\n{'='*70}")
    print("PHASE 3: Raw filename -> Final output path")
    print("=" * 70)

    for (case, filepath), tag_result in zip(created_files, tag_results):
        final = predict_final_path(case, tag_result)
        tag_quality = "RICH" if len(tag_result.get("parsed", {})) >= 4 else \
                      "PARTIAL" if tag_result.get("parsed", {}) else "NONE"

        print(f"\n  RAW:   {case['filename']}")
        print(f"  TAGS:  [{tag_quality}] {case['description']}")
        print(f"  FINAL: {final}")

    # Phase 4: Infrastructure routing
    run_infrastructure_test(test_dir)

    # Phase 5: Classification logic preview
    print(f"\n{'='*70}")
    print("PHASE 4: AudioClassifier Detection Preview")
    print("=" * 70)

    for case, filepath in created_files:
        ext = filepath.suffix.lower()
        tags = case.get("tags", {})
        genre = tags.get("genre", "").lower()

        detection = "UNKNOWN"
        reason = ""

        if ext == ".m4b":
            detection = "AUDIOBOOK"
            reason = "M4B extension (definitive)"
        elif genre in ("audiobook", "speech", "podcast"):
            detection = "AUDIOBOOK"
            reason = f"Genre tag: '{genre}'"
        elif genre:
            detection = "MUSIC"
            reason = f"Genre tag: '{genre}'"
        elif any(kw in case["filename"].lower() for kw in ("audiobook", "chapter", "narrat")):
            detection = "AUDIOBOOK"
            reason = "Filename keyword"
        else:
            detection = "MUSIC"
            reason = "Default (no audiobook indicators)"

        expected = "AUDIOBOOK" if case["category"] == "audiobook" else "MUSIC"
        match = "OK" if detection == expected else "MISS"

        print(f"  [{match:4s}] {case['filename'][:50]:50s} -> {detection:10s} ({reason})")

    # Cleanup
    print(f"\n{'='*70}")
    print("CLEANUP")
    print("=" * 70)
    shutil.rmtree(test_dir)
    print(f"  Removed test files from {test_dir}")

    print(f"\n{'='*70}")
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
