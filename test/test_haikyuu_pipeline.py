#!/usr/bin/env python3
"""
Simulate the pipeline for Haikyuu test files.

Traces what autoharbor would create (CTX sidecars), then runs structpilot's
process_series() on each file to verify the output filenames.

Does NOT start actual services - just simulates the logic.
"""

import sys
import os
import json
import tempfile
import shutil
import re
from pathlib import Path

_PROJECT_ROOT = Path(__file__).parent.parent
_BIN_DIR = _PROJECT_ROOT / "bin"
sys.path.insert(0, str(_PROJECT_ROOT))
sys.path.insert(0, str(_BIN_DIR))

from bin.structpilot import clean_stem, read_ctx_data, StructPilot, sanitize_filename

# Use temp dir to avoid !! path issues and ensure clean state
import tempfile as _tmpmod
_DROP_TMP = Path(_tmpmod.mkdtemp(prefix="haikyuu_test_"))
DROP_DIR = _DROP_TMP / "Haikyuu!!"

# Create files directly in temp
_structure = {
    "Season 1": [f"[Breeze] Haikyuu!! - {i:02d} [1080p BD][AV1][dual audio].mkv" for i in range(1, 26)],
    "Season 2": [f"[Breeze] Haikyuu!! Second Season - {i:02d} [1080p BD][AV1][dual audio].mkv" for i in range(1, 26)],
    "Season 3": [f"[Breeze] Haikyuu!! Karasuno High School vs. Shiratorizawa Academy - {i:02d} [1080p BD][AV1][dual audio].mkv" for i in range(1, 11)],
    "Season 4": (
        [f"[Breeze] Haikyu S04E{i:02d} [1080p BD][AV1][dual audio].mkv" for i in range(1, 5)] +
        [f"[Breeze] Haikyu.S04E{i:02d} [1080p BD][AV1][dual audio].mkv" for i in range(5, 26)]
    ),
    "OAD": [
        "[Breeze] Haikyuu!! - OAD [1080p][AV1].mkv",
        "[Breeze] Haikyuu!! Karasuno High School vs. Shiratorizawa Academy - OAD [1080p][AV1].mkv",
        "[Breeze] Haikyuu!! Second Season - OAD [1080p][AV1].mkv",
    ],
    "OVA": [
        "[Breeze] Haikyuu!! Land vs. Air - 01 [1080p BD][AV1][dual audio].mkv",
        "[Breeze] Haikyuu!! Land vs. Air - 02 [1080p BD][AV1][dual audio].mkv",
    ],
}

_count = 0
for _folder, _files in _structure.items():
    _fpath = DROP_DIR / _folder
    _fpath.mkdir(parents=True, exist_ok=True)
    for _fname in _files:
        (_fpath / _fname).write_bytes(b'\x00' * 1024)
        _count += 1

print(f"Created {_count} test files in temp dir")

# Collect all files from the drop folder structure
files_by_season = {}
for season_dir in sorted(DROP_DIR.iterdir()):
    if not season_dir.is_dir():
        continue
    season_name = season_dir.name
    files_by_season[season_name] = sorted(
        f for f in season_dir.iterdir() if f.is_file() and f.suffix.lower() == '.mkv'
    )

print("=" * 80)
print("HAIKYUU PIPELINE SIMULATION")
print("=" * 80)
print(f"\nDrop folder: {DROP_DIR}")
print(f"Seasons found: {list(files_by_season.keys())}")
total_files = sum(len(v) for v in files_by_season.values())
print(f"Total files: {total_files}")

# Simulate the pipeline in a temp dir
with tempfile.TemporaryDirectory() as tmpdir:
    processing_dir = Path(tmpdir) / "Processing"
    staged_dir = Path(tmpdir) / "Staged" / "Identify"
    processing_dir.mkdir(parents=True)
    staged_dir.mkdir(parents=True)

    sp = StructPilot(processing_dir, staged_dir, 'series')

    issues = []
    results_by_season = {}

    for season_name, season_files in files_by_season.items():
        print(f"\n{'-' * 80}")
        print(f"  {season_name} ({len(season_files)} files)")
        print(f"{'-' * 80}")

        season_results = []

        for orig_file in season_files:
            # Step 1: Simulate autoharbor - flatten file to Processing/ with CTX
            flat_name = orig_file.name
            dest = processing_dir / flat_name
            shutil.copy2(str(orig_file), str(dest))

            # Create CTX sidecar (what autoharbor would create)
            ctx_path = dest.with_suffix(dest.suffix + ".ctx.json")
            # source_root = top-level folder ("Haikyuu!!")
            # source_containers = folder path segments
            ctx_data = {
                "source_root": "Haikyuu!!",
                "source_containers": ["Haikyuu!!", season_name],
                "source_relpath": f"Haikyuu!!/{season_name}/{flat_name}",
                "pipeline_mode": "series",
                "ingested_at": "2026-01-01T00:00:00Z"
            }
            ctx_path.write_text(json.dumps(ctx_data), encoding='utf-8')

            # Step 2: Run structpilot's cleaning + CTX override
            cleaned = clean_stem(dest.stem)
            final_name = sp.process_series(dest, cleaned, ctx_path, True)

            # Step 3: Check results
            season_results.append({
                "original": flat_name,
                "cleaned": cleaned,
                "final": final_name,
            })

            # Cleanup for next file
            if dest.exists(): dest.unlink()
            if ctx_path.exists(): ctx_path.unlink()

        results_by_season[season_name] = season_results

        # Print results for this season
        for r in season_results:
            # Compact display
            print(f"  {r['original'][:60]:60} -> {r['final']}")

    # ================= VALIDATION =================
    print(f"\n{'=' * 80}")
    print("VALIDATION")
    print(f"{'=' * 80}")

    # Check 1: Each season's files should have consistent show names within that season
    # (structpilot cleans filenames; processors canonicalize via API later)
    print("\n  [CHECK 1] Per-season show name consistency")
    all_ok = True
    for season_name, results in results_by_season.items():
        names = set()
        for r in results:
            m = re.match(r'(.+?)\s+S\d{2}E\d{2,4}', r['final'])
            if m:
                names.add(m.group(1).strip())
        if len(names) == 1:
            print(f"    {season_name}: PASS - '{list(names)[0]}'")
        elif len(names) == 0:
            print(f"    {season_name}: INFO - No SxxEyy files (OAD/OVA)")
        else:
            print(f"    {season_name}: FAIL - Inconsistent: {names}")
            issues.append(f"{season_name} has inconsistent show names: {names}")
            all_ok = False
    if all_ok:
        print(f"    PASS")

    # Check 2: S1 files should get SxxEyy from CTX injection (they have bare numbers)
    print("\n  [CHECK 2] S1 CTX injection (bare '01' -> S01E01)")
    s1_results = results_by_season.get("Season 1", [])
    s1_injected = 0
    for r in s1_results:
        if re.search(r'S01E\d{2}', r['final']):
            s1_injected += 1
    print(f"    {s1_injected}/{len(s1_results)} files got S01Exx format")
    if s1_injected == len(s1_results):
        print(f"    PASS")
    else:
        print(f"    FAIL - Some S1 files missing SxxEyy")
        issues.append("S1 CTX injection incomplete")

    # Check 3: S2 files should have CTX override stripping "Second Season"
    print("\n  [CHECK 3] S2 CTX override (strip 'Second Season')")
    s2_results = results_by_season.get("Season 2", [])
    s2_clean = 0
    for r in s2_results:
        if "Second Season" not in r['final'] and "second season" not in r['final'].lower():
            s2_clean += 1
    print(f"    {s2_clean}/{len(s2_results)} files have clean show name (no 'Second Season')")
    if s2_clean == len(s2_results):
        print(f"    PASS")
    else:
        print(f"    FAIL - Some S2 files still have 'Second Season' in name")
        issues.append("S2 CTX override not working")

    # Check 4: S3 files should get proper S03Exx format via CTX injection
    # (subtitle stays in show name — processor canonicalizes later via API)
    print("\n  [CHECK 4] S3 CTX injection (bare numbers -> S03Exx)")
    s3_results = results_by_season.get("Season 3", [])
    s3_injected = 0
    for r in s3_results:
        if re.search(r'S03E\d{2}', r['final']):
            s3_injected += 1
    print(f"    {s3_injected}/{len(s3_results)} files got S03Exx format")
    if s3_injected == len(s3_results):
        print(f"    PASS")
    else:
        print(f"    FAIL - Some S3 files missing S03Exx")
        issues.append("S3 CTX injection incomplete")

    # Check 5: S4 files already have SxxEyy - should preserve format
    # (filename spelling "Haikyu" preserved; processor canonicalizes via API)
    print("\n  [CHECK 5] S4 SxxEyy preservation")
    s4_results = results_by_season.get("Season 4", [])
    s4_good = 0
    for r in s4_results:
        if re.search(r'S04E\d{2}', r['final']):
            s4_good += 1
    print(f"    {s4_good}/{len(s4_results)} files kept S04Exx format")
    if s4_good == len(s4_results):
        print(f"    PASS")
    else:
        print(f"    FAIL - Some S4 files lost SxxEyy format")
        issues.append("S4 SxxEyy preservation failed")

    # Check 6: No CTX files in staged dir
    print("\n  [CHECK 6] No CTX files in staged directory")
    ctx_in_staged = list(staged_dir.glob("**/*.ctx.json"))
    if len(ctx_in_staged) == 0:
        print(f"    PASS - Staged dir is CTX-free")
    else:
        print(f"    FAIL - {len(ctx_in_staged)} CTX files found in staged")
        issues.append(f"{len(ctx_in_staged)} CTX files leaked to staged")

    # Check 7: OAD/OVA handling
    print("\n  [CHECK 7] OAD/OVA files")
    oad_results = results_by_season.get("OAD", [])
    ova_results = results_by_season.get("OVA", [])
    print(f"    OAD: {len(oad_results)} files")
    for r in oad_results:
        print(f"      {r['final']}")
    print(f"    OVA: {len(ova_results)} files")
    for r in ova_results:
        print(f"      {r['final']}")

    # ================= SUMMARY =================
    print(f"\n{'=' * 80}")
    if issues:
        print(f"ISSUES FOUND: {len(issues)}")
        for i, issue in enumerate(issues, 1):
            print(f"  {i}. {issue}")
    else:
        print("ALL CHECKS PASSED")
    print(f"{'=' * 80}")

    # Cleanup temp
    shutil.rmtree(str(_DROP_TMP), ignore_errors=True)

    sys.exit(1 if issues else 0)
