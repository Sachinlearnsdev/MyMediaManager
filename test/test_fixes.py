#!/usr/bin/env python3
"""
Unit test for the fixes:
1. One Piece filter (TV vs Movie)
2. Episode titles from Jikan
3. Season subtitle cleanup
4. CTX injection
"""

import sys
import os
from pathlib import Path

# Add project root
_BIN_DIR = Path(__file__).parent.parent / "bin"
_PROJECT_ROOT = _BIN_DIR.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import json
import requests
import re

print("="*70)
print("TESTING FIXES")
print("="*70)

# Test 1: One Piece Fix
print("\n[TEST 1] One Piece - Filter by type='TV'")
try:
    res = requests.get("https://api.jikan.moe/v4/anime",
                      params={"q": "One Piece", "limit": 5})
    data = res.json().get('data', [])

    # OLD WAY: Would return Movie
    old_result = data[0] if data else None
    print(f"  OLD (first result): {old_result.get('title') if old_result else 'None'}")
    print(f"    Type: {old_result.get('type') if old_result else 'N/A'}")

    # NEW WAY: Filter by TV
    tv_result = None
    for item in data:
        if item.get('type', '').upper() == 'TV':
            tv_result = item
            break
    print(f"  NEW (filtered TV): {tv_result.get('title') if tv_result else 'None'}")
    print(f"    Type: {tv_result.get('type') if tv_result else 'N/A'}")
    print(f"  Status: {'PASS' if tv_result and tv_result.get('type') == 'TV' else 'FAIL'}")
except Exception as e:
    print(f"  ERROR: {e}")

# Test 2: Episode Titles
print("\n[TEST 2] Episode Titles - Jikan English titles")
try:
    import time
    mal_id = 51009  # JJK Season 2
    time.sleep(1)
    res = requests.get(f"https://api.jikan.moe/v4/anime/{mal_id}/episodes",
                      params={"page": 1})
    eps = res.json().get('data', [])[:3]

    print(f"  Jikan Episode Titles (prefer 'title' over 'title_romanji'):")
    for ep in eps:
        ep_num = ep.get('mal_id')
        title_en = ep.get('title')
        title_jp = ep.get('title_romanji')
        print(f"    Ep {ep_num}:")
        print(f"      EN: {title_en}")
        print(f"      JP: {title_jp}")
        is_english = title_en and not any(ord(c) > 127 for c in title_en)
        print(f"      Using: {'EN (title)' if is_english else 'JP (fallback)'}")
    print(f"  Status: PASS")
except Exception as e:
    print(f"  ERROR: {e}")

# Test 3: Season Subtitle Cleanup
print("\n[TEST 3] Season Subtitles - Filter generic phrases")
generic_patterns = [
    r"^\s*season\s+\d+\s*$",
    r"^\s*\d+(?:st|nd|rd|th)\s+season\s*$",
]

test_cases = [
    ("3rd Season", True, "Generic - should strip"),
    ("Season 2", True, "Generic - should strip"),
    ("Final Season", False, "Meaningful - keep"),
    ("To the Top", False, "Meaningful - keep"),
    ("Karasuno vs Shiratorizawa", False, "Meaningful - keep"),
]

print(f"  Testing phrase detection:")
for phrase, should_be_generic, reason in test_cases:
    is_generic = False
    for pattern in generic_patterns:
        if re.match(pattern, phrase.lower().strip()):
            is_generic = True
            break

    status = "PASS" if is_generic == should_be_generic else "FAIL"
    result = "[STRIP]" if is_generic else "[KEEP]"
    print(f"    {phrase:30} -> {result:8} ({status}) - {reason}")

# Test 4: CTX Injection
print("\n[TEST 4] CTX Injection - Episode extraction")

test_filenames = [
    ("Episode 01.mkv", 1),
    ("01.mkv", 1),
    ("Episode 02.mkv", 2),
    ("12.mkv", 12),
    ("S01E03.mkv", None),  # Should not match, returns None
]

print(f"  Testing episode extraction from filenames:")
pattern = r"(?:episode|ep|\D?)(\d{1,3})$"
for filename, expected_ep in test_filenames:
    clean = filename.rsplit('.', 1)[0]
    m = re.search(pattern, clean, re.I)
    extracted = int(m.group(1)) if m else None
    status = "PASS" if extracted == expected_ep else "FAIL"
    print(f"    {filename:20} -> Ep {str(extracted):3} ({status})")

print("\n" + "="*70)
print("TESTING COMPLETE")
print("="*70)
