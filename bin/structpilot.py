#!/usr/bin/env python3

import os
import sys
import time
import re
import json
import shutil
import argparse
from pathlib import Path

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, SCAN_INTERVAL, NOISE_PATTERNS, WORD_NUM
import common

# Regex
SXXEYY_RE = re.compile(r"\b[Ss](\d{1,2})[Ee](\d{1,4})\b", re.IGNORECASE)
SQUARE_BRACKETS_RE = re.compile(r"\[[^\[\]]*\]")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")

# Initialized in __main__ with mode
log_obj = None
cfg = None

def log(msg, level="info"):
    if level == "error": log_obj.error(msg)
    elif level == "warning": log_obj.warning(msg)
    else: log_obj.info(msg)

def sanitize_filename(name: str) -> str:
    name = re.sub(r'[\\/*?:"<>|]', "", name)
    return re.sub(r"\s+", " ", name).strip()

def clean_stem(stem: str) -> str:
    # 1. Strip [Brackets]
    while SQUARE_BRACKETS_RE.search(stem):
        stem = SQUARE_BRACKETS_RE.sub(" ", stem)
    # 2. Replace separators
    stem = re.sub(r"[.\-_+]", " ", stem)
    # 3. Strip built-in noise
    for pat in NOISE_PATTERNS:
        stem = re.sub(pat, "", stem, flags=re.I)
    # 4. Normalize Spaces
    return re.sub(r"\s{2,}", " ", stem).strip(" -_.")

def read_ctx_data(ctx_path: Path):
    """Returns season number AND a potential show name from folder path."""
    try:
        data = json.loads(ctx_path.read_text())
        season = None
        folder_show_name = None

        specials_re = re.compile(r'^(?:OAD|OVA|ONA|Specials?|Extras?)$', re.I)

        for entry in data.get("source_containers", []):
            m = re.search(r"\bseason\s*(\d{1,2})\b", entry, re.I)
            if m: season = int(m.group(1))
            if season is None:
                m = re.search(r"\bS(\d{1,2})(?:\b|E)", entry, re.I)
                if m: season = int(m.group(1))

        if season is None:
            rel = data.get("source_relpath", "")
            m = re.search(r"\bseason\s*(\d{1,2})\b", rel, re.I)
            if m: season = int(m.group(1))
        if season is None:
            rel = data.get("source_relpath", "")
            m = re.search(r"\bS(\d{1,2})(?:\b|E)", rel, re.I)
            if m: season = int(m.group(1))

        containers = data.get("source_containers", [])
        if containers:
            parent = containers[-1]
            if re.search(r"^season\s*\d+$", parent, re.IGNORECASE) or specials_re.match(parent.strip()):
                if len(containers) > 1:
                    parent = containers[-2]
                else:
                    parent = data.get("source_root", "")
            if parent:
                folder_show_name = clean_stem(parent)
        else:
            # No containers: file was at folder root.
            # source_root has the drop folder name (often show name + season)
            root = data.get("source_root", "")
            if root:
                if season is None:
                    m = re.search(r"\bseason\s*(\d{1,2})\b", root, re.I)
                    if m: season = int(m.group(1))
                if season is None:
                    m = re.search(r"\bS(\d{1,2})(?:\b|E)", root, re.I)
                    if m: season = int(m.group(1))
                clean_root = re.sub(r"\bseason\s*\d{1,2}\b", "", root, flags=re.I).strip(" -")
                clean_root = re.sub(r"\bS\d{1,2}(?:\b|E\d+)", "", clean_root, flags=re.I).strip(" -")
                clean_root = re.sub(r"\[[^\[\]]*\]", "", clean_root).strip(" -")
                if clean_root:
                    folder_show_name = clean_stem(clean_root)

        return season, folder_show_name
    except Exception as e:
        log_obj.debug(f"CTX read error: {e}")
    return None, None

# ================= LOGIC =================
def extract_season_episode(stem):
    hum_s_pat = r"\b(one|two|three|four|five|six|seven|eight|nine|ten|" \
                r"first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\b\s+season\b"

    s_val = None
    m_s = re.search(hum_s_pat, stem, re.I)
    if m_s:
        s_val = WORD_NUM[m_s.group(1).lower()]
    else:
        m_s = re.search(r"\bseason\s+(\d{1,2})\b", stem, re.I)
        if m_s: s_val = int(m_s.group(1))

    e_val = None
    m_e = re.search(r"\bepisode\s+(\d{1,4})\b", stem, re.I)
    if m_e:
        e_val = int(m_e.group(1))
    else:
        m_e = re.search(r"(?:^|[\s\-])(\d{1,4})$", stem)
        if m_e: e_val = int(m_e.group(1))

    return s_val, e_val

def strip_season_info(stem):
    pat = r"\b(one|two|three|four|five|six|seven|eight|nine|ten|" \
          r"first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\b\s+season\b"
    stem = re.sub(pat, "", stem, flags=re.I)
    stem = re.sub(r"\bseason\s+\d{1,2}\b", "", stem, flags=re.I)
    stem = re.sub(r"\bepisode\s+\d{1,4}\b", "", stem, flags=re.I)
    # Strip trailing bare episode number (left over after "Second Season" removal)
    stem = re.sub(r"\s+\d{1,4}$", "", stem)
    return re.sub(r"\s{2,}", " ", stem).strip()

# ================= PROCESSOR =================
class StructPilot:
    def __init__(self, input_dir, output_dir, mode):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.mode = mode

        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True, exist_ok=True)

    def process_series(self, video_path, clean_name, ctx, ctx_exists):
        final_name = None
        ext = video_path.suffix

        # A. Check for Human/Explicit Season
        hum_s, hum_e = extract_season_episode(clean_name)

        if hum_s is not None and hum_e is not None:
            show_name = strip_season_info(clean_name)
            if not show_name: show_name = clean_name.split("Season")[0].strip()
            final_name = f"{show_name} S{hum_s:02}E{hum_e:02}{ext}"
            log(f"Human Format -> {final_name}")

        # B. Check for SxxEyy
        elif not final_name:
            m = SXXEYY_RE.search(clean_name)
            if m:
                s = int(m.group(1))
                e = int(m.group(2))
                show_name = clean_name[:m.start()].strip()
                final_name = f"{show_name} S{s:02}E{e:02}{ext}"
                log(f"SxxEyy Format -> {final_name}")

        # C. CTX Injection (Episode only file / Unknown Show fix)
        if not final_name and ctx_exists:
            ep_match = re.search(r"(?:^|[\s\-])(\d{1,3})$", clean_name)
            if ep_match:
                e_val = int(ep_match.group(1))
                ctx_s, ctx_folder_show = read_ctx_data(ctx)

                if ctx_s is not None:
                    show_name = clean_name[:ep_match.start()].strip()
                    if not show_name and ctx_folder_show:
                        show_name = ctx_folder_show
                        log(f"Recovered Show Name from Folder: {show_name}")
                    if not show_name: show_name = "Unknown Show"
                    final_name = f"{show_name} S{ctx_s:02}E{e_val:02}{ext}"
                    log(f"CTX Injection -> {final_name}")

        # D. Fallback
        if not final_name:
            final_name = f"{clean_name}{ext}"

        return sanitize_filename(final_name)

    def process_movie(self, video_path, clean_name):
        ext = video_path.suffix
        final_name = f"{clean_name}{ext}"
        return sanitize_filename(final_name)

    def run(self):
        log(f"Service Started ({self.mode}). Watching: {self.input_dir}")

        while True:
            try:
                files = sorted([f for f in self.input_dir.iterdir() if f.is_file()], key=lambda p: p.stat().st_mtime)
                for f in files:
                    if f.suffix.lower() in VIDEO_EXTS:

                        ctx = f.with_suffix(f.suffix + ".ctx.json")
                        if not ctx.exists():
                            ctx = f.with_name(f.stem + ".ctx.json")
                        ctx_exists = ctx.exists()

                        clean_stem_str = clean_stem(f.stem)

                        if self.mode == 'series':
                            final_name = self.process_series(f, clean_stem_str, ctx, ctx_exists)
                        else:
                            final_name = self.process_movie(f, clean_stem_str)

                        target = self.output_dir / final_name
                        if target.exists():
                            ts = int(time.time())
                            target = self.output_dir / f"{Path(final_name).stem}_{ts}{f.suffix}"

                        try:
                            shutil.move(str(f), str(target))
                            log(f"Moved: {target.name}")
                            if ctx_exists:
                                ctx.unlink()
                                log("CTX Consumed")
                        except Exception as e:
                            log(f"Move Error: {e}", "error")

            except Exception as e:
                log(f"Loop Error: {e}", "error")

            time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True, choices=['series', 'movies'])
    args = parser.parse_args()
    log_obj, cfg = common.setup_logger("structpilot", args.mode)

    if args.mode == 'series':
        INPUT_DIR = Path(cfg['paths']['series_pipeline']['processing'])
        OUTPUT_DIR = Path(cfg['paths']['series_pipeline']['staged']['identify'])
    else:
        INPUT_DIR = Path(cfg['paths']['movie_pipeline']['processing'])
        OUTPUT_DIR = Path(cfg['paths']['movie_pipeline']['staged']['movies'])

    if not INPUT_DIR.exists():
        log(f"Input dir missing, creating: {INPUT_DIR}", "warning")
        INPUT_DIR.mkdir(parents=True, exist_ok=True)

    StructPilot(INPUT_DIR, OUTPUT_DIR, args.mode).run()
