#!/usr/bin/env python3
"""
seriesprocessor.py -- Unified TV and Cartoon processor.
Replaces tvprocessor.py and cartoonprocessor.py.
Launched with: --type tv OR --type cartoons
"""

import os
import sys
import time
import json
import shutil
import re
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, SCAN_INTERVAL, CONFIDENCE_TV, CONFIDENCE_CARTOON
import common
from bin.central_logic import CentralLogic
from bin.show_cache import ShowCache
from bin.noise_learner import NoiseLearner
from bin.media_sources import SourcePool


# ================= PROCESSOR =================
class SeriesProcessor:
    """
    Unified processor for TV shows and cartoons.
    media_type: "tv" or "cartoons"
    """

    def __init__(self, media_type: str):
        self.media_type = media_type
        self.log_obj, self.cfg = common.setup_logger(f"{media_type}proc")
        self.brain = CentralLogic()

        try:
            staged_key = media_type  # "tv", "cartoons", "reality", "talkshow", "documentaries"
            self.input_dir = Path(self.cfg['paths']['series_pipeline']['staged'][staged_key])
            # documentaries output key is "documentaries_series" (not "documentaries")
            output_key = "documentaries_series" if media_type == "documentaries" else media_type
            self.output_root = Path(self.cfg['paths']['output'][output_key])
        except KeyError as e:
            self.log(f"Config Key Missing: {e}", "error")
            sys.exit(1)

        cache_root = Path(self.cfg.get('cache', {}).get('root', str(common.BASE_DIR / 'cache')))
        self.pool = SourcePool(self.cfg, cache_root / media_type)
        self.show_cache = ShowCache(self.cfg)
        self.noise_learner = NoiseLearner(self.cfg)
        self.regex = re.compile(r"(.+?)[ ._-]*S(\d{1,2})E(\d{1,4})", re.IGNORECASE)

        # Configurable API priority
        api_cfg = self.cfg.get('api_config', {}).get(media_type, {})
        self.search_order = api_cfg.get('search', ['tvdb'])
        self.episode_order = api_cfg.get('episode_titles', ['tvdb'])

        # Per-type confidence threshold
        _thresholds = {'cartoons': CONFIDENCE_CARTOON}
        self.threshold = _thresholds.get(media_type, CONFIDENCE_TV)

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)

    def log(self, msg, level="info"):
        if level == "error": self.log_obj.error(msg)
        elif level == "warning": self.log_obj.warning(msg)
        else: self.log_obj.info(msg)

    def _get_review_dir(self) -> Path:
        raw = self.cfg['paths']['series_pipeline'].get('review', '')
        return Path(raw) if raw else Path(self.cfg['paths']['roots']['manager']) / "Review" / "Shows"

    def _get_dup_dir(self) -> Path:
        raw = self.cfg['paths']['series_pipeline'].get('duplicates', '')
        return Path(raw) if raw else Path(self.cfg['paths']['roots']['manager']) / "Duplicates" / "Shows"

    def _write_reason(self, dest_path: Path, confidence: int, candidate, best_match,
                       best_source=None, best_id=None):
        """Write a .reason.json sidecar next to a file moved to Review."""
        try:
            data = {
                "source": f"seriesprocessor_{self.media_type}",
                "reason": "low_confidence",
                "confidence": confidence, "threshold": self.threshold,
                "candidate": candidate, "best_match": best_match,
                "detail": f"Best confidence {confidence}% below threshold {self.threshold}%",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            # Store match data for the Approve feature
            if best_match and best_source and best_id:
                data["match_data"] = {
                    "source": best_source,
                    "source_id": str(best_id),
                    "title": best_match,
                    "media_type": self.media_type,
                }
            reason_path = dest_path.with_name(dest_path.name + ".reason.json")
            reason_path.write_text(json.dumps(data, indent=2), encoding='utf-8')
        except Exception:
            pass

    def _write_dup(self, dest_path: Path, final_name: str, existing_path: Path):
        """Write a .dup.json sidecar next to a file moved to Duplicates."""
        try:
            dup_path = dest_path.with_name(dest_path.name + ".dup.json")
            existing_size = existing_path.stat().st_size if existing_path.exists() else 0
            new_size = dest_path.stat().st_size if dest_path.exists() else 0
            dup_path.write_text(json.dumps({
                "source": f"seriesprocessor_{self.media_type}",
                "final_name": final_name,
                "existing_path": str(existing_path),
                "existing_size_mb": round(existing_size / (1024*1024), 1),
                "new_size_mb": round(new_size / (1024*1024), 1),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, indent=2), encoding='utf-8')
        except Exception:
            pass

    def process_file(self, file_path: Path):
        try:
            match = self.regex.match(file_path.name)
            season_num = None
            ep_num = None
            candidate = None

            # SxxEyy format required (structpilot guarantees this)
            if match:
                candidate, _, _ = self.brain.get_title_candidate(file_path.name)
                season_num = int(match.group(2))
                ep_num = int(match.group(3))

            if not candidate or season_num is None or ep_num is None:
                return

            self.log(f"Processing: {file_path.name}")

            best_id = None
            best_title = None
            best_source = None
            max_score = 0

            # --- META SIDECAR CHECK (from classifier) ---
            meta_path = file_path.with_name(file_path.name + ".meta.json")
            meta_used = False
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                    meta_ids = meta.get("api_ids", {})
                    if meta_ids.get("tvdb_id"):
                        best_id = meta_ids["tvdb_id"]
                        best_title = meta.get("title")
                        best_source = "tvdb"
                        max_score = meta.get("confidence", 100)
                        meta_used = True
                        self.log(f"Sidecar IDs: {candidate} -> {best_title} (tvdb:{best_id})")
                except Exception:
                    pass

            if not meta_used:
                # --- CACHE CHECK ---
                cache_hit = self.show_cache.lookup(candidate)
                if not cache_hit:
                    cache_hit = self.show_cache.lookup_fuzzy(candidate)

                if cache_hit and cache_hit.get("api_ids", {}).get("tvdb_id"):
                    best_id = cache_hit["api_ids"]["tvdb_id"]
                    best_title = cache_hit["canonical_name"]
                    best_source = "tvdb"
                    max_score = cache_hit.get("confidence_at_match", 100)
                    self.log(f"Cache hit: {candidate} -> {best_title}")

            if not best_id:
                # --- API SEARCH (using configured priority) ---
                queries = self.brain.generate_query_matrix(candidate)
                for q in queries:
                    results = self.pool.search_chain(q, self.search_order, media_type=self.media_type)

                    results = self.brain.tvmaze_spinoff_fallback(results, q, self.pool, self.media_type)

                    for r in results:
                        api_title = r.get('title')
                        score = self.brain.calculate_confidence(file_path.name, api_title)
                        matched = api_title  # Track which title actually matched

                        # Alias check (fixes Money Heist / La Casa de Papel)
                        if score < 100 and r.get('aliases'):
                            for alias in r['aliases']:
                                a_score = self.brain.calculate_confidence(file_path.name, alias)
                                if a_score > score:
                                    score = a_score
                                    matched = alias

                        # Non-Latin primary title: fetch extended details for English name
                        # (TVDB stores Indian/Asian shows with local-script primary titles)
                        if score < self.threshold and api_title and re.search(r'[^\x00-\x7F]', api_title):
                            src = self.pool.get(r.get('source'))
                            if src:
                                ext = src.get_details(r.get('source_id'))
                                if ext:
                                    eng = ext.get('title')
                                    if eng and eng != api_title:
                                        eng_score = self.brain.calculate_confidence(file_path.name, eng)
                                        if eng_score > score:
                                            score = eng_score
                                            matched = eng

                        if score > max_score:
                            max_score = score
                            best_id = r.get('source_id')
                            best_title = matched
                            best_source = r.get('source')

                    if max_score >= 95:
                        break

            # --- THRESHOLD ---
            if not best_id or max_score < self.threshold:
                self.log(f"Low confidence ({int(max_score)}%). Moving to Review.", "warning")
                dest = self._get_review_dir() / file_path.name
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file_path), str(dest))
                self._write_reason(dest, int(max_score), candidate, best_title,
                                   best_source=best_source, best_id=best_id)
                # Clean up sidecars
                meta = file_path.with_name(file_path.name + ".meta.json")
                if meta.exists():
                    meta.unlink()
                return

            # --- METADATA ---
            src = self.pool.get(best_source) if best_source else None

            # English title resolution — verify API title matches what we scored against.
            # Prevents alias-matched titles being overwritten by unrelated API names
            # (e.g., "Breaking Bad" alias on TVDB's "Metástasis" entry → keep "Breaking Bad")
            english_show_name = src.get_english_title(best_id) if src else None
            if english_show_name and best_title:
                verify = self.brain.calculate_confidence(best_title, english_show_name)
                if verify < 50:
                    self.log(f"Title mismatch: API='{english_show_name}' vs matched='{best_title}', keeping matched")
                    final_show_name = best_title
                else:
                    final_show_name = english_show_name
            else:
                final_show_name = english_show_name if english_show_name else best_title

            # Episode title (using configured priority)
            ep_title = None
            ep_order = list(self.episode_order)
            if best_source and best_source not in ep_order:
                ep_order.insert(0, best_source)
            id_map = {best_source: best_id} if best_source else {}
            ep_map = self.pool.get_episodes_chain(ep_order, id_map, season=season_num)
            if ep_map:
                ep_title = ep_map.get(str(ep_num))

            if self.brain.is_non_latin_title(ep_title):
                ep_title = None

            # Season subtitle
            season_subtitle = src.get_season_subtitle(best_id, season_num) if src else None

            # Filter out subtitle if it's just the show name repeated
            if season_subtitle and final_show_name:
                if season_subtitle.strip().lower() == final_show_name.strip().lower():
                    season_subtitle = None

            # --- CACHE + LEARN ---
            api_ids = {f"{best_source}_id": best_id} if best_source else {}
            self.show_cache.register(
                final_show_name, self.media_type,
                api_ids,
                raw_name=candidate, confidence=int(max_score)
            )
            # Noise learner disabled: write-only system (structpilot never reads learned patterns)

            # --- BUILD PATH ---
            safe_show = self.brain.sanitize_for_filesystem(final_show_name)
            ext = file_path.suffix

            if ep_title:
                safe_ep = self.brain.sanitize_for_filesystem(ep_title)
                if safe_ep:  # Guard against empty string after sanitization
                    new_name = f"{safe_show} - S{season_num:02}E{ep_num:02} - {safe_ep}{ext}"
                else:
                    new_name = f"{safe_show} - S{season_num:02}E{ep_num:02}{ext}"
            else:
                new_name = f"{safe_show} - S{season_num:02}E{ep_num:02}{ext}"

            # Season folder with subtitle
            if season_subtitle:
                clean_sub = self.brain.sanitize_for_filesystem(season_subtitle)
                folder_name = f"Season {season_num:02} - {clean_sub}" if clean_sub else f"Season {season_num:02}"
            else:
                folder_name = f"Season {season_num:02}"

            dest_dir = self.output_root / safe_show / folder_name
            dest_path = dest_dir / new_name

            # --- EXECUTE ---
            if dest_path.exists():
                self.log(f"Duplicate: {dest_path.name} already in library. Moving to Duplicates.", "warning")
                dup_dir = self._get_dup_dir()
                dup_dir.mkdir(parents=True, exist_ok=True)
                dup_dest = dup_dir / file_path.name
                shutil.move(str(file_path), str(dup_dest))
                self._write_dup(dup_dest, dest_path.name, dest_path)
                meta = file_path.with_name(file_path.name + ".meta.json")
                if meta.exists():
                    meta.unlink()
                return

            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file_path), str(dest_path))
            self.log(f"Library Import: {dest_path.name}")

            # Clean up meta sidecar
            meta = file_path.with_name(file_path.name + ".meta.json")
            if meta.exists():
                meta.unlink()

        except Exception as e:
            self.log(f"Error processing {file_path.name}: {e}", "error")

    def run(self):
        self.log(f"{self.media_type.title()} Service Started. Watching: {self.input_dir}")
        while True:
            try:
                files = sorted([
                    f for f in self.input_dir.iterdir()
                    if f.is_file() and f.suffix.lower() in VIDEO_EXTS
                ])
                for f in files:
                    self.process_file(f)
            except Exception as e:
                self.log(f"Loop error: {e}", "error")
            time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True, choices=["tv", "cartoons", "reality", "talkshow", "documentaries"])
    args = parser.parse_args()
    SeriesProcessor(args.type).run()
