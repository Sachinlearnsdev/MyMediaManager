#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

from bin.constants import VIDEO_EXTS, SCAN_INTERVAL, CONFIDENCE_CLASSIFIER, CONFIDENCE_TV
import common
from bin.central_logic import CentralLogic
from bin.noise_learner import NoiseLearner
from bin.media_sources import SourcePool

# Global State
BRAIN = CentralLogic()

log_obj, cfg = common.setup_logger("contentclassifier")
NOISE_LEARNER = NoiseLearner(cfg)

def log(level, msg):
    if level in ["CRITICAL", "ERROR"]: log_obj.error(msg)
    elif level == "WARN": log_obj.warning(msg)
    else: log_obj.info(f"[{level}] {msg}")

# Configurable API priority (defaults match proven chain)
_classifier_api_cfg = cfg.get('api_config', {}).get('classifier', {})
ANIME_CHECK_ORDER = _classifier_api_cfg.get('anime_check', ['mal', 'jikan'])
TV_CHECK_ORDER = _classifier_api_cfg.get('tv_check', ['tvdb'])

# Centralized source pool
cache_root = Path(cfg.get('cache', {}).get('root', str(common.BASE_DIR / 'cache')))
POOL = SourcePool(cfg, cache_root / "classifier")

def _has_non_latin(text):
    """Check if text contains non-Latin characters (Hindi, CJK, Arabic, etc.)."""
    return bool(re.search(r'[^\x00-\x7F]', text)) if text else False

def _title_distance(raw, title):
    """Word count difference. Lower = closer match (tiebreaker for equal scores)."""
    rw = len(re.sub(r'[^a-z0-9 ]+', ' ', raw.lower()).split())
    tw = len(re.sub(r'[^a-z0-9 ]+', ' ', title.lower()).split())
    return abs(rw - tw)

# ======================================================
# CORE RESOLVER (The Brain Interface)
# ======================================================
def resolve_content(raw_name):
    # Check show cache first
    try:
        from bin.show_cache import ShowCache
        cache = ShowCache(cfg)
        hit = cache.lookup(raw_name)
        if not hit:
            hit = cache.lookup_fuzzy(raw_name)
        if hit:
            log("INFO", f"Cache hit: {raw_name} -> {hit['canonical_name']} ({hit['media_type']})")
            return {
                "title": hit["canonical_name"],
                "is_anime": hit["media_type"] == "anime",
                "media_type": hit["media_type"],
                "genres": hit.get("genres", []),
                "country": hit.get("country", ""),
                "api_ids": hit.get("api_ids", {}),
            }, hit.get("confidence_at_match", 100)
    except Exception as e:
        log_obj.debug(f"Cache lookup skipped: {e}")

    # 1. Ask Brain for Title Candidate
    candidate, _, _ = BRAIN.get_title_candidate(raw_name)

    # 2. Generate the Sequential Query Matrix
    queries = BRAIN.generate_query_matrix(candidate)

    # If get_title_candidate stripped a year from the name (e.g. "Scam 1992" -> "Scam"),
    # prepend the full raw_name as the first query since it's already SxxEyy-stripped
    if raw_name.lower() != candidate.lower() and raw_name not in queries:
        queries.insert(0, raw_name)

    best_match = None
    best_score = 0
    best_api_ids = {}
    best_anime_score = 0  # Track Phase A anime matches separately for disambiguation
    best_title_dist = float('inf')  # Tiebreaker: fewer extra words = closer match

    for q in queries:
        if len(q) < 2: continue

        # --- PHASE A: ANIME CHECK (using configured API priority) ---
        anime_results = POOL.search_chain(q, ANIME_CHECK_ORDER, media_type="anime")
        for a in anime_results:
            # Filter: only consider TV series, OVA, ONA for show classification
            a_type = a.get('media_type', '').upper()
            if a_type not in ('TV', 'OVA', 'ONA'):
                continue

            for title_key in ['title_english', 'title']:
                api_title = a.get(title_key)
                if not api_title: continue

                score = BRAIN.calculate_confidence(raw_name, api_title)
                # Track best anime match for animation/anime disambiguation
                # Require anime title to cover at least half the query words to count
                # (prevents short generic anime like "ROLL", "Island" from being
                # used as evidence to classify "Roll No 21", "Total Drama Island")
                api_wc = len(re.sub(r'[^a-z0-9 ]+', ' ', api_title.lower()).split())
                raw_wc = len(re.sub(r'[^a-z0-9 ]+', ' ', raw_name.lower()).split())
                if score > best_anime_score and api_wc >= max(1, raw_wc * 0.5):
                    best_anime_score = score
                # Require minimum CONFIDENCE_TV (60%) for anime to beat a TV match
                if score >= CONFIDENCE_TV and score > best_score:
                    best_score = score
                    final_title = a.get('title_english') or a.get('title')
                    best_title_dist = _title_distance(raw_name, final_title)
                    best_match = {"title": final_title, "is_anime": True, "genres": [], "_details": None}
                    best_api_ids = {f"{a['source']}_id": a['source_id']} if a.get('source_id') else {}

        # --- PHASE B: TV/CARTOON CHECK (using configured API priority) ---
        tv_results = POOL.search_chain(q, TV_CHECK_ORDER, media_type="tv")

        tv_results = BRAIN.tvmaze_spinoff_fallback(tv_results, q, POOL, 'tv')

        for t in tv_results:
            # Check primary title
            score = BRAIN.calculate_confidence(raw_name, t.get('title'))
            matched_title = t.get('title')

            # Check aliases (TVDB stores non-English shows with local-script primary titles)
            for alias in (t.get('aliases') or []):
                alias_score = BRAIN.calculate_confidence(raw_name, alias)
                if alias_score > score:
                    score = alias_score
                    matched_title = alias

            # Non-Latin primary title with low score and no useful alias:
            # Fetch extended details to get English translations
            # (TVDB stores Indian/Asian shows with local-script primary titles,
            #  English names only available via /series/{id}/extended translations)
            if score < CONFIDENCE_TV and _has_non_latin(t.get('title', '')):
                src = POOL.get(t['source'])
                if src:
                    ext_details = src.get_details(t['source_id'])
                    if ext_details:
                        eng_title = ext_details.get('title')
                        if eng_title and eng_title != t.get('title'):
                            eng_score = BRAIN.calculate_confidence(raw_name, eng_title)
                            if eng_score > score:
                                score = eng_score
                                matched_title = eng_title

            title_dist = _title_distance(raw_name, matched_title)
            if score > best_score or (score == best_score and title_dist < best_title_dist):
                # Tiebreaker: when scores equal, closest title to raw name wins
                # (prevents spinoffs like "Silicon Valley Hidden Stories" beating "Silicon Valley")
                best_score = score
                best_title_dist = title_dist
                # Fetch extended metadata for genre-based routing (cached if already fetched above)
                src = POOL.get(t['source'])
                details = src.get_details(t['source_id']) if src else None
                genres = details.get('genres', []) if details else []
                real_title = details.get('title', matched_title) if details else matched_title
                # Prefer the matched alias/english title over translated title
                if details and matched_title != t.get('title'):
                    real_title = matched_title
                best_match = {
                    "title": real_title,
                    "is_anime": "anime" in (genres or []),
                    "genres": genres,
                    "_details": details,  # Preserve for country-based disambiguation
                }
                best_api_ids = {f"{t['source']}_id": t['source_id']} if t.get('source_id') else {}

        if best_score >= 95: break

    # Register in cache on success
    if best_match and best_score >= CONFIDENCE_CLASSIFIER:
        try:
            from bin.show_cache import ShowCache
            cache = ShowCache(cfg)
            media_type = "anime" if best_match["is_anime"] else "tv"
            genres = best_match.get("genres") or []
            details = best_match.get("_details")
            if best_match["is_anime"]:
                media_type = "anime"
            elif "anime" in genres:
                # TVDB anime genre slug
                media_type = "anime"
            elif "animation" in genres:
                # Disambiguation: anime vs western cartoon
                # Require anime DB match to be BOTH >= CONFIDENCE_TV (60) AND close to
                # the best overall score. This prevents false positives where a short/generic
                # anime title (e.g., "ROLL", "Island") gets a high score against a longer
                # show name (e.g., "Roll No. 21", "Total Drama Island") but the TVDB match
                # is clearly better.
                anime_is_credible = (
                    best_anime_score >= CONFIDENCE_TV
                    and best_anime_score >= best_score - 10  # Within 10% of best overall
                )
                if anime_is_credible:
                    # Show was found in anime databases (MAL/Jikan) with solid match → anime
                    media_type = "anime"
                else:
                    # Check TVDB country — Japanese animation = anime
                    country = ""
                    if details and details.get("raw"):
                        country = (details["raw"].get("originalCountry") or "").lower()
                    if country in ("jpn", "jp", "japan"):
                        media_type = "anime"
                    else:
                        media_type = "cartoons"
            elif "documentary" in genres:
                media_type = "documentaries"
            elif "reality" in genres:
                media_type = "reality"
            elif "talk-show" in genres:
                media_type = "talkshow"
            best_match["media_type"] = media_type
            # Extract country for cache storage
            cache_country = ""
            if details and details.get("raw"):
                cache_country = (details["raw"].get("originalCountry") or "")
            cache.register(best_match["title"], media_type, best_api_ids,
                           raw_name=raw_name, confidence=int(best_score),
                           genres=genres, country=cache_country)
        except Exception as e:
            log_obj.debug(f"Cache register skipped: {e}")

        # Noise learner disabled: write-only system (structpilot never reads learned patterns)
        # Kept import for future integration but no-op for now to avoid list/set bugs

        return best_match, int(best_score)
    return None, int(best_score)

def _write_reason(dest_path, source, reason, confidence, threshold, candidate, best_match, detail):
    """Write a .reason.json sidecar next to a file moved to Review."""
    try:
        reason_path = dest_path.with_name(dest_path.name + ".reason.json")
        reason_path.write_text(json.dumps({
            "source": source, "reason": reason,
            "confidence": confidence, "threshold": threshold,
            "candidate": candidate, "best_match": best_match,
            "detail": detail,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }, indent=2), encoding='utf-8')
    except Exception:
        pass

# ======================================================
# MAIN LOOP
# ======================================================
def main(mode):
    config = common.load_config()

    paths = config['paths']['series_pipeline']
    INPUT_DIR = Path(paths['staged']['identify'])

    REVIEW_DIR = Path(paths['review'])

    # ShowCache for anime base name normalization
    from bin.show_cache import ShowCache
    anime_cache = ShowCache(config)

    DESTINATIONS = {
        "anime": Path(paths['staged']['anime']),
        "cartoons": Path(paths['staged']['cartoons']),
        "documentaries": Path(paths['staged']['documentaries']),
        "reality": Path(paths['staged']['reality']),
        "talkshow": Path(paths['staged']['talkshow']),
        "tv": Path(paths['staged']['tv']),
    }

    REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    log("INFO", f"Service Started. Watching: {INPUT_DIR}")

    while True:
        try:
            for file in sorted(INPUT_DIR.iterdir(), key=lambda p: p.stat().st_mtime):
                if not file.is_file() or file.suffix.lower() not in VIDEO_EXTS: continue

                log("INFO", f"Classifying: {file.name}")

                m = re.match(r"^(?P<name>.+?)\s+(?P<numbering>[Ss]\d{1,2}[Ee]\d{1,4}).*?$", file.name)
                if not m:
                    dest = REVIEW_DIR / file.name
                    shutil.move(str(file), str(dest))
                    _write_reason(dest, "classifier", "no_sxxeyy", 0, CONFIDENCE_CLASSIFIER,
                                  file.name, None, "No SxxEyy pattern found in filename")
                    continue

                raw_name = m.group("name").replace(".", " ").strip()
                numbering = m.group("numbering")

                match, score = resolve_content(raw_name)

                if match:
                    # Use media_type directly (set from cache or API classification)
                    media_type = match.get("media_type", "tv")
                    dest = DESTINATIONS.get(media_type, DESTINATIONS["tv"])

                    # For anime: normalize to base show name if a shorter canonical exists
                    title_for_filename = match["title"]
                    if media_type == "anime":
                        try:
                            base_hit = anime_cache.lookup_anime_base(match["title"])
                            if base_hit:
                                title_for_filename = base_hit["canonical_name"]
                                log("INFO", f"Anime base name: {match['title']} -> {title_for_filename}")
                        except Exception:
                            pass

                    safe_title = BRAIN.sanitize_for_filesystem(title_for_filename)
                    new_filename = f"{safe_title} {numbering}{file.suffix}"

                    log("SUCCESS", f"Routed: {file.name} -> {dest.name} ({match['title']})")
                    dest_path = dest / new_filename
                    shutil.move(str(file), str(dest_path))

                    # Write .meta.json sidecar so processor can skip API re-search
                    try:
                        meta_path = dest_path.with_name(dest_path.name + ".meta.json")
                        meta_path.write_text(json.dumps({
                            "title": match["title"],
                            "media_type": media_type,
                            "api_ids": match.get("api_ids", {}),
                            "confidence": score,
                        }, indent=2), encoding='utf-8')
                    except Exception:
                        pass
                else:
                    log("WARN", f"Review: {file.name} (best score: {score}%)")
                    dest = REVIEW_DIR / file.name
                    shutil.move(str(file), str(dest))
                    _write_reason(dest, "classifier", "low_confidence", score,
                                  CONFIDENCE_CLASSIFIER, raw_name, None,
                                  f"Best confidence {score}% below threshold {CONFIDENCE_CLASSIFIER}%")

        except Exception as e:
            log("ERROR", f"Loop Error: {e}")

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    main(parser.parse_args().mode)
