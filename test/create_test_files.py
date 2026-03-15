#!/usr/bin/env python3
"""
Test the fixes with sample files
"""

import os
import subprocess
from pathlib import Path
import shutil
import json
import time

# Create test directory structure
TEST_BASE = Path("E:\\CODE\\MyMediaManager\\test\\sample_files")
TEST_BASE.mkdir(parents=True, exist_ok=True)

# Clear previous test files
for f in TEST_BASE.glob("**/*"):
    if f.is_file():
        f.unlink()

def create_test_file(rel_path, ext=".mkv"):
    """Create a dummy test video file"""
    full_path = TEST_BASE / (rel_path + ext)
    full_path.parent.mkdir(parents=True, exist_ok=True)
    full_path.touch()
    return full_path

def create_ctx_file(video_path, show_name, season, source_containers=None):
    """Create a CTX file for a video"""
    ctx_path = video_path.parent / f"{video_path.stem}.ctx.json"

    if source_containers is None:
        source_containers = [show_name, f"Season {season}"]

    ctx_data = {
        "source_root": show_name,
        "source_containers": source_containers,
        "source_relpath": f"{show_name}/Season {season}/{video_path.name}"
    }
    ctx_path.write_text(json.dumps(ctx_data, indent=2))
    return ctx_path

# Test Case 1: One Piece (should use TV series, not movie)
print("Test 1: One Piece - S01E01")
create_test_file("One Piece/S01E01")

# Test Case 2: Haikyuu Season 3 (should get "Karasuno vs Shiratorizawa", not "3rd Season")
print("Test 2: Haikyuu - S03E01")
create_test_file("Haikyuu/S03E01")

# Test Case 3: JJK Season 2 (should NOT have redundant "Season 2")
print("Test 3: JJK - S02E01")
create_test_file("Jujutsu Kaisen S02/S02E01")

# Test Case 4: CTX Injection (Episode 01.mkv in nested folder)
print("Test 4: CTX Injection - Naruto nested")
ep_file = create_test_file("Naruto/Season 1/Episode 01")
create_ctx_file(ep_file, "Naruto", 1, ["Naruto", "Season 1"])

# Test Case 5: CTX Injection - JJK (Episode naming)
print("Test 5: CTX Injection - JJK nesterd")
ep_file2 = create_test_file("Jujutsu Kaisen Season 2/01")
create_ctx_file(ep_file2, "Jujutsu Kaisen", 2, ["Jujutsu Kaisen", "Season 2"])

# Test Case 6: Attack on Titan S04 (should have "Final Season")
print("Test 6: Attack on Titan - S04E01")
create_test_file("Attack on Titan/S04E01")

# Test Case 7: Avatar (Cartoon - should be detected as cartoon, not anime)
print("Test 7: Avatar - S01E01")
create_test_file("Avatar/S01E01")

# Test Case 8: Breaking Bad (TV - should use TVDB)
print("Test 8: Breaking Bad - S01E01")
create_test_file("Breaking Bad/S01E01")

print(f"\nTest files created in {TEST_BASE}")
print("\nTest Files:")
for f in sorted(TEST_BASE.glob("**/*.mkv")):
    rel = f.relative_to(TEST_BASE)
    ctx_f = f.parent / f"{f.stem}.ctx.json"
    ctx_marker = " [+CTX]" if ctx_f.exists() else ""
    print(f"  {rel}{ctx_marker}")
