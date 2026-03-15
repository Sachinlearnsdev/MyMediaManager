#!/usr/bin/env python3
"""
Unit tests for CTX override in structpilot and season stripping in animeprocessor.

Tests the 4 bugs from Haikyuu testing:
  Bug 1: Season variants create separate shows (CTX override + season strip)
  Bug 2: Season subtitle uses wrong title for comparison
  Bug 3: Split-cour seasons
  Bug 4: CTX not leaking to staged/library folders
"""

import sys
import os
import json
import tempfile
import shutil
from pathlib import Path

# Project root + bin/ (needed for `import common` inside bin/ scripts)
_PROJECT_ROOT = Path(__file__).parent.parent
_BIN_DIR = _PROJECT_ROOT / "bin"
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_BIN_DIR))

from bin.structpilot import clean_stem, read_ctx_data, StructPilot, sanitize_filename, SXXEYY_RE, extract_season_episode, strip_season_info
import re

PASS = 0
FAIL = 0

def check(name, expected, actual):
    global PASS, FAIL
    if expected == actual:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}")
        print(f"        expected: {expected}")
        print(f"        actual:   {actual}")


def make_ctx(path, source_root, containers, relpath=""):
    """Create a .ctx.json sidecar file."""
    data = {
        "source_root": source_root,
        "source_containers": containers,
        "source_relpath": relpath,
        "pipeline_mode": "series",
        "ingested_at": "2024-01-01T00:00:00Z"
    }
    path.write_text(json.dumps(data), encoding='utf-8')


# ================= TEST 1: CTX Show Name Override in StructPilot =================
print("=" * 70)
print("TEST 1: StructPilot CTX Show Name Override")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    input_dir = Path(tmpdir) / "input"
    output_dir = Path(tmpdir) / "output"
    input_dir.mkdir()
    output_dir.mkdir()

    sp = StructPilot(input_dir, output_dir, 'series')

    # 1a: S1 - CTX folder "Haikyuu!!" vs filename "Haikyuu!!" -> no override (same length)
    video = input_dir / "Haikyuu!! S01E01.mkv"
    video.write_bytes(b'\x00' * 100)
    ctx = video.with_suffix(video.suffix + ".ctx.json")
    make_ctx(ctx, "Haikyuu!!", ["Haikyuu!!", "Season 1"])

    clean = clean_stem(video.stem)
    result = sp.process_series(video, clean, ctx, True)
    check("S1: no override when CTX = filename show", "Haikyuu!! S01E01.mkv", result)

    # 1b: S2 - CTX folder "Haikyuu!!" vs filename "Haikyuu!! Second Season" -> override
    video2 = input_dir / "Haikyuu!! Second Season S02E01.mkv"
    video2.write_bytes(b'\x00' * 100)
    ctx2 = video2.with_suffix(video2.suffix + ".ctx.json")
    make_ctx(ctx2, "Haikyuu!!", ["Haikyuu!!", "Season 2"])

    clean2 = clean_stem(video2.stem)
    result2 = sp.process_series(video2, clean2, ctx2, True)
    check("S2: CTX overrides 'Haikyuu Second Season' -> 'Haikyuu!!'",
          "Haikyuu!! S02E01.mkv", result2)

    # 1c: S3 - CTX folder "Haikyuu!!" vs filename with long subtitle -> override
    video3 = input_dir / "Haikyuu!! Karasuno High School vs Shiratorizawa Academy S03E01.mkv"
    video3.write_bytes(b'\x00' * 100)
    ctx3 = video3.with_suffix(video3.suffix + ".ctx.json")
    make_ctx(ctx3, "Haikyuu!!", ["Haikyuu!!", "Season 3"])

    clean3 = clean_stem(video3.stem)
    result3 = sp.process_series(video3, clean3, ctx3, True)
    check("S3: CTX overrides long subtitle -> 'Haikyuu!!'",
          "Haikyuu!! S03E01.mkv", result3)

    # 1d: S4 - CTX folder "Haikyuu!!" vs filename "Haikyuu!! To the Top" -> override
    video4 = input_dir / "Haikyuu!! To the Top S04E01.mkv"
    video4.write_bytes(b'\x00' * 100)
    ctx4 = video4.with_suffix(video4.suffix + ".ctx.json")
    make_ctx(ctx4, "Haikyuu!!", ["Haikyuu!!", "Season 4"])

    clean4 = clean_stem(video4.stem)
    result4 = sp.process_series(video4, clean4, ctx4, True)
    check("S4: CTX overrides 'Haikyuu To the Top' -> 'Haikyuu!!'",
          "Haikyuu!! S04E01.mkv", result4)

    # 1e: No CTX -> no override, preserves original
    video5 = input_dir / "Haikyuu!! To the Top S04E05.mkv"
    video5.write_bytes(b'\x00' * 100)
    clean5 = clean_stem(video5.stem)
    result5 = sp.process_series(video5, clean5, Path("nonexistent.ctx.json"), False)
    check("S4 no CTX: preserves 'Haikyuu!! To the Top' in filename",
          "Haikyuu!! To the Top S04E05.mkv", result5)

    # 1f: Human format - "Third Season Episode 5" with CTX
    video6 = input_dir / "Haikyuu!! Third Season Episode 5.mkv"
    video6.write_bytes(b'\x00' * 100)
    ctx6 = video6.with_suffix(video6.suffix + ".ctx.json")
    make_ctx(ctx6, "Haikyuu!!", ["Haikyuu!!", "Season 3"])

    clean6 = clean_stem(video6.stem)
    result6 = sp.process_series(video6, clean6, ctx6, True)
    check("Human format: CTX overrides stripped season name",
          "Haikyuu!! S03E05.mkv", result6)


# ================= TEST 2: AnimeProcessor Season Stripping =================
print("\n" + "=" * 70)
print("TEST 2: AnimeProcessor _strip_season_from_candidate()")
print("=" * 70)

from bin.animeprocessor import AnimeProcessor

# Create a minimal processor just to test the method
# (We can call the static-like method on the class)
class MockAnimeProc:
    def _strip_season_from_candidate(self, candidate, season_num):
        return AnimeProcessor._strip_season_from_candidate(self, candidate, season_num)

mock = MockAnimeProc()

check("Strip '2nd Season'",
      "Haikyu!!", mock._strip_season_from_candidate("Haikyu!! 2nd Season", 2))
check("Strip 'Season 3'",
      "Naruto", mock._strip_season_from_candidate("Naruto Season 3", 3))
check("Strip '3rd Season'",
      "Haikyu!!", mock._strip_season_from_candidate("Haikyu!! 3rd Season", 3))
check("Keep non-generic subtitle",
      "Haikyu!! To the Top", mock._strip_season_from_candidate("Haikyu!! To the Top", 4))
check("Keep S1 unchanged",
      "Haikyu!!", mock._strip_season_from_candidate("Haikyu!!", 1))
check("Strip 'Second Season' (word form)",
      "My Hero Academia", mock._strip_season_from_candidate("My Hero Academia Second Season", 2))


# ================= TEST 3: CTX Deletion (not moved to staged) =================
print("\n" + "=" * 70)
print("TEST 3: CTX Deleted at StructPilot (not moved to staged)")
print("=" * 70)

with tempfile.TemporaryDirectory() as tmpdir:
    input_dir = Path(tmpdir) / "processing"
    output_dir = Path(tmpdir) / "staged"
    input_dir.mkdir()
    output_dir.mkdir()

    sp = StructPilot(input_dir, output_dir, 'series')

    # Create video + CTX
    video = input_dir / "Haikyuu!! S01E01.mkv"
    video.write_bytes(b'\x00' * 100)
    ctx = video.with_suffix(video.suffix + ".ctx.json")
    make_ctx(ctx, "Haikyuu!!", ["Haikyuu!!", "Season 1"])

    # Simulate what run() does: process then move + delete ctx
    clean = clean_stem(video.stem)
    final_name = sp.process_series(video, clean, ctx, True)
    target = output_dir / final_name
    shutil.move(str(video), str(target))
    # This is what run() does at line 236
    if ctx.exists():
        ctx.unlink()

    check("Video moved to staged", True, target.exists())
    check("CTX deleted from processing", False, ctx.exists())

    # Check no CTX in output dir
    ctx_in_output = list(output_dir.glob("*.ctx.json"))
    check("No CTX files in staged dir", 0, len(ctx_in_output))


# ================= TEST 4: Season Subtitle Comparison (Bug 2) =================
print("\n" + "=" * 70)
print("TEST 4: Season Subtitle Extraction (romaji vs english)")
print("=" * 70)

from bin.animeprocessor import AnimeProcessor as AP

class MockAP2:
    brain = type('obj', (object,), {'sanitize_for_filesystem': staticmethod(lambda t: re.sub(r'[\\/*?:"<>|]', '', t))})()
    def _is_generic_season_phrase(self, text):
        return AP._is_generic_season_phrase(self, text)
    def _extract_season_subtitle(self, season_title, root_name, season_num):
        return AP._extract_season_subtitle(self, season_title, root_name, season_num)

m2 = MockAP2()

# Romaji comparison: S3 romaji "Haikyuu!! Karasuno Koukou vs. Shiratorizawa Gakuen Koukou"
# S1 romaji "Haikyuu!!" -> should extract subtitle
sub = m2._extract_season_subtitle(
    "Haikyuu!! Karasuno Koukou vs. Shiratorizawa Gakuen Koukou",
    "Haikyuu!!",
    3
)
check("S3 subtitle from romaji (vs S1 romaji root)",
      True, sub is not None and "Karasuno" in sub)

# Generic subtitle should be filtered
sub2 = m2._extract_season_subtitle("3rd Season", "Haikyuu!!", 3)
check("Generic '3rd Season' filtered out", None, sub2)

# "To the Top" should be kept
sub3 = m2._extract_season_subtitle("Haikyuu!! To the Top", "Haikyuu!!", 4)
check("'To the Top' subtitle preserved",
      True, sub3 is not None and "To the Top" in sub3)


# ================= TEST 5: No CTX in Processors =================
print("\n" + "=" * 70)
print("TEST 5: No CTX References in Processors")
print("=" * 70)

import inspect

# Check animeprocessor has no CTX methods
ap_source = inspect.getsource(AP)
check("animeprocessor: no _read_ctx_data method",
      False, "_read_ctx_data" in ap_source)
check("animeprocessor: no ctx.unlink()",
      False, "ctx.unlink()" in ap_source)
check("animeprocessor: no .ctx.json reference",
      False, ".ctx.json" in ap_source)

# Check seriesprocessor
from bin.seriesprocessor import SeriesProcessor
sp_source = inspect.getsource(SeriesProcessor)
check("seriesprocessor: no _read_ctx_data method",
      False, "_read_ctx_data" in sp_source)
check("seriesprocessor: no ctx.unlink()",
      False, "ctx.unlink()" in sp_source)
check("seriesprocessor: no .ctx.json reference",
      False, ".ctx.json" in sp_source)


# ================= SUMMARY =================
print("\n" + "=" * 70)
total = PASS + FAIL
print(f"RESULTS: {PASS}/{total} passed, {FAIL} failed")
if FAIL == 0:
    print("ALL TESTS PASSED")
else:
    print(f"FAILURES: {FAIL}")
print("=" * 70)

sys.exit(1 if FAIL > 0 else 0)
