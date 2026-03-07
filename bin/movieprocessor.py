#!/usr/bin/env python3
# movieprocessor.py — Brain-Enabled Movie Engine

import os
import sys
import time
import re
import json
import shutil
import argparse
from pathlib import Path
from datetime import datetime, timezone

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, SCAN_INTERVAL, CONFIDENCE_MOVIE
import common
from bin.central_logic import CentralLogic
from bin.noise_learner import NoiseLearner
from bin.media_sources import SourcePool

# Setup Logger & Config
log, CFG = common.setup_logger("movieproc")
BRAIN = CentralLogic()

# Configurable API priority
_movie_api_cfg = CFG.get('api_config', {}).get('movies', {})
MOVIE_SEARCH_ORDER = _movie_api_cfg.get('search', ['tmdb'])

# Centralized source pool
cache_root = Path(CFG.get('cache', {}).get('root', str(common.BASE_DIR / 'cache')))
POOL = SourcePool(CFG, cache_root / "movies")
NOISE_LEARNER = NoiseLearner(CFG)

def _write_reason(dest_path, confidence, candidate, best_match, best_result=None):
    """Write a .reason.json sidecar next to a file moved to Review."""
    try:
        data = {
            "source": "movieprocessor", "reason": "low_confidence",
            "confidence": confidence, "threshold": CONFIDENCE_MOVIE,
            "candidate": candidate, "best_match": best_match,
            "detail": f"Best confidence {confidence}% below threshold {CONFIDENCE_MOVIE}%",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # Store full match data for the Approve feature
        if best_result:
            data["match_data"] = {
                "source": best_result.get("source"),
                "source_id": str(best_result.get("source_id", "")),
                "title": best_result.get("title"),
                "year": best_result.get("year"),
            }
        reason_path = dest_path.with_name(dest_path.name + ".reason.json")
        reason_path.write_text(json.dumps(data, indent=2), encoding='utf-8')
    except Exception:
        pass

def _write_dup(dest_path, final_name, existing_path):
    """Write a .dup.json sidecar next to a file moved to Duplicates."""
    try:
        dup_path = dest_path.with_name(dest_path.name + ".dup.json")
        existing_size = existing_path.stat().st_size if existing_path.exists() else 0
        new_size = dest_path.stat().st_size if dest_path.exists() else 0
        dup_path.write_text(json.dumps({
            "source": "movieprocessor",
            "final_name": final_name,
            "existing_path": str(existing_path),
            "existing_size_mb": round(existing_size / (1024*1024), 1),
            "new_size_mb": round(new_size / (1024*1024), 1),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, indent=2), encoding='utf-8')
    except Exception:
        pass

# ================= CORE LOGIC =================
def process_movie(file: Path, output_movies: Path, output_anime: Path, output_standup: Path, output_docs: Path, review_dir: Path, dup_dir: Path):
    log.info(f"Processing: {file.name}")

    # Check show cache first
    try:
        from bin.show_cache import ShowCache
        cache = ShowCache(CFG)
        candidate_for_cache, _, _ = BRAIN.get_title_candidate(file.name)
        hit = cache.lookup(candidate_for_cache)
        if hit and hit.get("api_ids", {}).get("tmdb_id"):
            best_result = {"id": int(hit["api_ids"]["tmdb_id"]), "title": hit["canonical_name"],
                           "release_date": hit.get("year", "0000")}
            log.info(f"Cache hit: {candidate_for_cache} -> {hit['canonical_name']}")
            final_title = BRAIN.sanitize_for_filesystem(best_result["title"])
            # Use cache year, then try extracting from filename, then fallback to 0000
            cache_year = (best_result.get("release_date") or "")[:4]
            if not cache_year or cache_year == "0000":
                _, file_year, _ = BRAIN.get_title_candidate(file.name)
                cache_year = file_year or "0000"
            final_year = cache_year
            final_filename = f"{final_title} ({final_year}){file.suffix}"

            dest_root = output_movies
            tmdb_src = POOL.get('tmdb')
            details = tmdb_src.get_details(best_result['id']) if tmdb_src else None
            if details:
                genres = [g['id'] for g in details.get('genres', [])]
                countries = [c['iso_3166_1'] for c in details.get('production_countries', [])]
                # Anime Movie: Animation genre + JP as SOLE production country
                if 16 in genres and countries == ['JP']:
                    dest_root = output_anime
                    collection = details.get('belongs_to_collection')
                    if collection:
                        col_name = BRAIN.sanitize_for_filesystem(collection['name'])
                        dest_root = output_anime / col_name
                # Stand-Up: Comedy genre + keywords OR solo-comedy with short runtime (<=80min)
                elif 35 in genres:
                    is_standup = False
                    kw_data = details.get('keywords', {})
                    keywords = [k.get('name', '').lower() for k in kw_data.get('keywords', [])]
                    for kw in keywords:
                        if any(t in kw for t in ('stand-up', 'standup', 'stand up', 'comedy special',
                                                  'comedy concert', 'comedian', 'one-man show',
                                                  'one man show', 'live comedy', 'comedy act',
                                                  'netflix special')):
                            is_standup = True
                            break
                    if not is_standup:
                        runtime = details.get('runtime', 0) or 0
                        if genres == [35] and 0 < runtime <= 80:
                            is_standup = True
                    if is_standup:
                        dest_root = output_standup
                # Documentary: only if PRIMARY genre (first)
                elif genres and genres[0] == 99:
                    dest_root = output_docs

            dest_root.mkdir(parents=True, exist_ok=True)
            target_path = dest_root / final_filename
            if target_path.exists():
                log.warning(f"Duplicate: {final_filename} already in library. Moving to Duplicates.")
                dup_dir.mkdir(parents=True, exist_ok=True)
                dup_dest = dup_dir / file.name
                shutil.move(str(file), str(dup_dest))
                _write_dup(dup_dest, final_filename, target_path)
            else:
                shutil.move(str(file), str(target_path))
                log.info(f"MOVED -> {target_path}")
            return
    except Exception as e:
        log.debug(f"Cache lookup skipped: {e}")

    # 1. Ask Brain for Structural Slicing
    candidate, detected_year, metadata = BRAIN.get_title_candidate(file.name)

    # Guard: reject suspiciously short candidates (prevents "The", "Par" bogus entries)
    if len(candidate.strip()) < 2:
        log.warning(f"Candidate too short ('{candidate}'). Moving to Review.")
        dest = review_dir / file.name
        review_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file), str(dest))
        _write_reason(dest, 0, candidate, None)
        return

    # 2. Generate the Sequential Query Matrix
    queries = BRAIN.generate_query_matrix(candidate)

    best_result = None
    max_score = 0
    best_query_words = 0

    # 3. Hit API with each query scenario (using configured priority)
    for q in queries:
        if len(q) < 2: continue
        q_words = len(q.split())
        results = POOL.search_chain(q, MOVIE_SEARCH_ORDER, media_type="movie")

        for r in results:
            api_title = r.get("title")
            api_year = r.get("year")

            score = BRAIN.calculate_confidence(
                filename=file.name,
                api_title=api_title,
                api_year=api_year,
                file_year=detected_year,
                year_mode="tiebreaker"
            )

            if score > max_score:
                # Specificity guard: a shorter (less specific) query must beat
                # the current best by a significant margin to override.
                # Prevents generic single-word matches from displacing relevant
                # multi-word matches (e.g. "Abhishek" movie overriding
                # "Abhishek Upmanyu" comedian specials).
                if best_result and max_score >= 40 and q_words < best_query_words:
                    margin = (best_query_words - q_words) * 20
                    if score <= max_score + margin:
                        continue
                max_score = score
                best_result = r
                best_query_words = q_words
            elif score == max_score and score > 0 and detected_year:
                # Tiebreaker: prefer result whose year matches the filename year
                old_year = (best_result.get("year") or "")[:4]
                new_year = (r.get("year") or "")[:4]
                if new_year == detected_year and old_year != detected_year:
                    best_result = r
                    best_query_words = q_words

        if max_score >= 100: break

    # 5. Apply Threshold
    if not best_result or max_score < CONFIDENCE_MOVIE:
        log.warning(f"Low confidence ({int(max_score)}%). Moving to Review.")
        dest = review_dir / file.name
        review_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(file), str(dest))
        _write_reason(dest, int(max_score), candidate,
                      best_result.get("title") if best_result else None,
                      best_result=best_result)
        return

    # 6. Success: Build Metadata
    final_title = BRAIN.sanitize_for_filesystem(best_result["title"])

    dest_root = output_movies

    # For TMDB results, fetch full details for anime detection + year fallback
    details = POOL.get_details_for(best_result['source'], best_result['source_id'])

    # Year: prefer API result, then TMDB details, then filename, then 0000
    api_year = best_result.get("year")
    if (not api_year or api_year == "0000") and details:
        api_year = (details.get("release_date") or "")[:4] or None
    final_year = (api_year or detected_year or "0000")[:4]
    final_filename = f"{final_title} ({final_year}){file.suffix}"

    if details:
        genres = [g['id'] for g in details.get('genres', [])] if 'genres' in details else []
        countries = [c['iso_3166_1'] for c in details.get('production_countries', [])] if 'production_countries' in details else []

        # Routing: Anime Movie Detection (Animation genre + JP as SOLE production country)
        if 16 in genres and countries == ['JP']:
            dest_root = output_anime
            collection = details.get('belongs_to_collection')
            if collection:
                col_name = BRAIN.sanitize_for_filesystem(collection['name'])
                dest_root = output_anime / col_name
        # Stand-Up Comedy (Genre 35 + keywords OR solo-comedy with short runtime <=80min)
        elif 35 in genres:
            is_standup = False
            kw_data = details.get('keywords', {})
            keywords = [k.get('name', '').lower() for k in kw_data.get('keywords', [])]
            for kw in keywords:
                if any(t in kw for t in ('stand-up', 'standup', 'stand up', 'comedy special',
                                          'comedy concert', 'comedian', 'one-man show',
                                          'one man show', 'live comedy', 'comedy act',
                                          'netflix special')):
                    is_standup = True
                    break
            if not is_standup:
                runtime = details.get('runtime', 0) or 0
                if genres == [35] and 0 < runtime <= 80:
                    is_standup = True
            if is_standup:
                dest_root = output_standup
        # Documentary (only if PRIMARY genre — first)
        elif genres and genres[0] == 99:
            dest_root = output_docs

    # 7. Execute Move
    dest_root.mkdir(parents=True, exist_ok=True)
    target_path = dest_root / final_filename

    if target_path.exists():
        log.warning(f"Duplicate: {final_filename} already in library. Moving to Duplicates.")
        dup_dir.mkdir(parents=True, exist_ok=True)
        dup_dest = dup_dir / file.name
        shutil.move(str(file), str(dup_dest))
        _write_dup(dup_dest, final_filename, target_path)
    else:
        shutil.move(str(file), str(target_path))
        log.info(f"MOVED -> {target_path}")

    # Register in cache
    try:
        from bin.show_cache import ShowCache
        cache = ShowCache(CFG)
        genre_names = [g.get('name', '').lower() for g in details.get('genres', [])] if details else []
        cache.register(best_result["title"], "movie",
                       {"tmdb_id": str(best_result['source_id'])},
                       raw_name=candidate, confidence=int(max_score),
                       genres=genre_names)
    except Exception as e:
        log.debug(f"Cache register skipped: {e}")

    # Noise learner disabled: write-only system (structpilot never reads learned patterns)

def main(mode):
    if mode != 'movies':
        sys.exit(1)

    INPUT_DIR = Path(CFG['paths']['movie_pipeline']['staged']['movies'])
    LIB_MOVIES = Path(CFG['paths']['output']['movies'])
    REVIEW_DIR = Path(CFG['paths']['movie_pipeline']['review'])
    DUP_DIR = Path(CFG['paths']['movie_pipeline'].get('duplicates', 'Duplicates/Movies'))
    LIB_ANIME = Path(CFG['paths']['output'].get('anime_movies', CFG['paths']['output'].get('anime', str(common.BASE_DIR / 'library' / 'Anime' / 'Movies'))))
    LIB_STANDUP = Path(CFG['paths']['output'].get('standup', 'Stand-Up'))
    LIB_DOCS = Path(CFG['paths']['output'].get('documentaries_movies', 'Documentaries/Movies'))

    log.info(f"Service Started (Strict Mode). Watching: {INPUT_DIR}")

    while True:
        try:
            files = sorted([f for f in INPUT_DIR.iterdir() if f.is_file() and f.suffix.lower() in VIDEO_EXTS])
            for f in files:
                process_movie(f, LIB_MOVIES, LIB_ANIME, LIB_STANDUP, LIB_DOCS, REVIEW_DIR, DUP_DIR)
        except Exception as e:
            log.error(f"Loop Error: {e}")
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    args = parser.parse_args()
    main(args.mode)
