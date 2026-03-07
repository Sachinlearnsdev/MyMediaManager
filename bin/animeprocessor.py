#!/usr/bin/env python3
"""
animeprocessor.py -- Multi-source anime processor using centralized API layer.

Features:
  - MAL Official API v2 for primary search + sequel chain (with Jikan/AniList/Kitsu fallback)
  - Jikan for episode titles
  - AniList GraphQL for English show titles + episode title fallback
  - Smart season folder naming (subtitle extraction)
  - Show-level cache with season data
  - Adaptive noise learning
"""

import os
import sys
import time
import json
import shutil
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone

# Dynamic path resolution
_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import VIDEO_EXTS, SCAN_INTERVAL
import common
from bin.central_logic import CentralLogic
from bin.show_cache import ShowCache
from bin.noise_learner import NoiseLearner
from bin.media_sources import SourcePool


# ================= ANIME SEARCH ENGINE =================
class AnimeSearchEngine:
    """Multi-source anime search using SourcePool.

    Season resolution logic, dedup guards, and sequel chain walking
    stay here. API calls are delegated to SourcePool.
    """

    def __init__(self, cache_dir: Path, cfg: dict, logger=None):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._logger = logger
        self.pool = SourcePool(cfg, cache_dir)
        self.episode_cache = {}

        # Consolidated season cache
        self._season_cache_path = cache_dir / "_season_cache.json"
        self._season_store = self._load_season_cache()
        self._migrate_season_files()

        # Configurable API priority (defaults match proven 140/140 chain)
        anime_api_cfg = cfg.get('api_config', {}).get('anime', {})
        self.search_order = anime_api_cfg.get('search', ['mal', 'jikan', 'anilist'])
        self.episode_order = anime_api_cfg.get('episode_titles', ['jikan', 'anilist'])
        self.sequel_order = anime_api_cfg.get('sequel_chain', ['mal', 'jikan'])

    def _load_season_cache(self) -> dict:
        if self._season_cache_path.exists():
            try:
                return json.loads(self._season_cache_path.read_text(encoding='utf-8'))
            except Exception:
                pass
        return {}

    def _save_season_cache(self):
        try:
            self._season_cache_path.write_text(
                json.dumps(self._season_store, ensure_ascii=False),
                encoding='utf-8'
            )
        except Exception:
            pass

    def _migrate_season_files(self):
        """Migrate individual season_*.json files into consolidated cache."""
        migrated = 0
        for f in list(self.cache_dir.iterdir()):
            if f.name.startswith('_') or not f.name.endswith('.json'):
                continue
            # Only migrate files that look like season caches (not source subdirs)
            if not f.is_file():
                continue
            try:
                data = json.loads(f.read_text(encoding='utf-8'))
                key = f.stem
                if key not in self._season_store:
                    self._season_store[key] = data
                f.unlink()
                migrated += 1
            except Exception:
                pass
        if migrated > 0:
            self._save_season_cache()

    def _cache_key(self, key_type, identifier):
        safe_id = hashlib.md5(str(identifier).encode('utf-8')).hexdigest()
        return f"{key_type}_{safe_id}"

    def _log(self, msg):
        if self._logger:
            self._logger.info(msg)

    def _search_by_source(self, query, prefer_base=False):
        """Search using configured source order. Returns entry dict or None."""
        for api_name in self.search_order:
            entry = self._search_single_source(api_name, query, prefer_base=prefer_base)
            if entry:
                return entry
        return None

    def _search_single_source(self, api_name, query, prefer_base=False):
        """Search a single source and return standardized entry or None."""
        src = self.pool.get(api_name)
        if not src:
            return None
        try:
            results = src.search(query, media_type="anime")
        except Exception:
            return None

        if not results:
            return None

        show_name_for_validation = re.sub(
            r'\b(season|part|cour)\s*\d+\b', '', query, flags=re.I
        ).strip()

        season_re = re.compile(r'\bseason\s*\d+|\d+(?:st|nd|rd|th)\s*season', re.I)
        best_fallback = None

        for r in results:
            mt = (r.get('media_type') or '').lower()
            if mt != 'tv':
                continue

            if not self._validate_result_relevance(show_name_for_validation, r):
                continue

            if prefer_base:
                title = r.get('title', '')
                title_eng = r.get('title_english', '') or ''
                if season_re.search(title) or season_re.search(title_eng):
                    if best_fallback is None:
                        best_fallback = r
                    continue

            # Build entry
            entry = self._result_to_entry(api_name, r)
            if entry:
                return entry

        if best_fallback:
            return self._result_to_entry(api_name, best_fallback)

        return None

    def _result_to_entry(self, api_name, r):
        """Convert a standard search result to an internal entry dict."""
        source_id = r.get('source_id')
        entry = {
            'mal_id': source_id if api_name in ('mal', 'jikan', 'anilist') else None,
            'title': r.get('title', ''),
            'title_english': r.get('title_english'),
            '_source': api_name,
            '_source_id': source_id,
        }

        # For MAL results, include related anime for sequel chain
        if '_related_anime' in r:
            entry['_related_anime'] = r['_related_anime']

        # If no English title, try AniList fallback
        if not entry['title_english'] and entry['mal_id']:
            anilist = self.pool.get('anilist')
            if anilist:
                try:
                    eng_title = anilist.get_english_title(entry['mal_id'])
                    if eng_title:
                        entry['title_english'] = eng_title
                except Exception:
                    pass

        return entry

    def _get_sequel_chain_for(self, mal_id):
        """Follow sequel chain using configured source priority."""
        id_map = {}
        for name in self.sequel_order:
            if name in ('mal', 'jikan', 'anilist'):
                id_map[name] = mal_id
        chain = self.pool.get_sequel_chain_from(self.sequel_order, id_map)
        return chain

    def search_season_entry(self, show_name, season_num, s1_mal_id=None):
        """Finds the specific MAL ID for 'Show Name Season X'."""
        if season_num > 1:
            query = f"{show_name} Season {season_num}"
        else:
            query = show_name
        self._log(f"[DEBUG] search_season_entry: show='{show_name}' S{season_num} query='{query}' s1_mal_id={s1_mal_id}")

        # Cache check (consolidated)
        cache_key = self._cache_key("season", f"{show_name}_s{season_num}")
        cached = self._season_store.get(cache_key)
        if cached:
            try:
                self._log(f"[DEBUG] Season cache HIT: {cache_key} -> mal_id={cached['result'].get('mal_id')}")
                return cached['result']
            except Exception:
                pass

        entry = None

        # For S2+: sequel chain FIRST (deterministic), then fallback to search
        if season_num > 1 and s1_mal_id:
            chain = self._get_sequel_chain_for(s1_mal_id)
            chain_idx = season_num - 2
            self._log(f"[DEBUG] Sequel chain from mal_id={s1_mal_id}: {[(c.get('mal_id'), c.get('title','')[:40]) for c in (chain or [])]}")
            if chain and 0 <= chain_idx < len(chain):
                entry = chain[chain_idx]
                self._log(f"[DEBUG] Sequel chain[{chain_idx}] -> mal_id={entry.get('mal_id')} title='{entry.get('title')}'")

            # Fallback: search + dedup guard if sequel chain didn't cover this season
            if not entry:
                self._log(f"[DEBUG] Sequel chain miss, searching: '{query}'")
                entry = self._search_by_source(query)
                if entry:
                    self._log(f"[DEBUG] Search result: mal_id={entry.get('mal_id')} title='{entry.get('title')}'")
                # Dedup: reject if same as S1 or any earlier chain entry
                if entry:
                    known_ids = {s1_mal_id}
                    if chain:
                        for i in range(min(chain_idx, len(chain))):
                            cid = chain[i].get('mal_id')
                            if cid:
                                known_ids.add(cid)
                    if entry.get('mal_id') in known_ids:
                        self._log(f"[DEBUG] Dedup rejected: mal_id={entry.get('mal_id')} in known_ids={known_ids}")
                        entry = None

                # Alt queries with full dedup
                if not entry:
                    known_ids = {s1_mal_id}
                    if chain:
                        for c in chain:
                            cid = c.get('mal_id')
                            if cid:
                                known_ids.add(cid)
                    alt_queries = [
                        f"{show_name} Part {season_num}",
                        f"{show_name} {season_num}",
                        f"{show_name} Cour {season_num}",
                    ]
                    for alt_q in alt_queries:
                        self._log(f"[DEBUG] Alt query: '{alt_q}'")
                        alt_entry = self._search_by_source(alt_q)
                        if alt_entry:
                            self._log(f"[DEBUG] Alt result: mal_id={alt_entry.get('mal_id')} title='{alt_entry.get('title')}'")
                        if alt_entry and alt_entry.get('mal_id') not in known_ids:
                            entry = alt_entry
                            break
        else:
            # S1: search by name
            self._log(f"[DEBUG] S1 search: '{query}' prefer_base=True")
            entry = self._search_by_source(query, prefer_base=(season_num == 1))
            if entry:
                self._log(f"[DEBUG] S1 search result: mal_id={entry.get('mal_id')} title='{entry.get('title')}' eng='{entry.get('title_english')}'")
            else:
                self._log(f"[DEBUG] S1 search returned None")

            # Verify result is actually S1, not a later season.
            # E.g. searching "Haikyu!! Karasuno High vs Shiratorizawa Academy"
            # returns eng="Haikyu!! 3rd Season" (mal_id=32935) which is S3, not S1.
            # Detect season indicator in English title, extract base name, re-search.
            if entry:
                eng = entry.get('title_english') or ''
                season_in_eng = re.search(
                    r'\d+(?:st|nd|rd|th)\s*season|\bseason\s*\d+', eng, re.I
                )
                if season_in_eng:
                    base_name = re.sub(
                        r'\s*\d+(?:st|nd|rd|th)\s*season|\s*season\s*\d+',
                        '', eng, flags=re.I
                    ).strip(' -:')
                    if base_name and len(base_name) > 2:
                        self._log(f"[DEBUG] S1 result '{eng}' looks like a season entry. Retrying with base: '{base_name}'")
                        real_s1 = self._search_by_source(base_name, prefer_base=True)
                        if real_s1:
                            self._log(f"[DEBUG] Real S1 found: mal_id={real_s1.get('mal_id')} title='{real_s1.get('title')}' eng='{real_s1.get('title_english')}'")
                            entry = real_s1

        # Strip internal fields before caching
        if entry:
            entry.pop('_related_anime', None)
            entry.pop('_source', None)
            entry.pop('_source_id', None)

        # Cache the result (consolidated)
        if entry:
            self._season_store[cache_key] = {
                'result': entry,
                'cached_at': datetime.now().isoformat(),
            }
            self._save_season_cache()

        return entry

    def _validate_result_relevance(self, show_name, result):
        """Check if a search result is actually relevant to the show name."""
        show_words = set(re.sub(r'[^a-z0-9 ]', ' ', show_name.lower()).split())
        show_words = {w for w in show_words if len(w) > 1}
        if not show_words:
            return True

        titles = []
        if result.get('title'):
            titles.append(result['title'])
        if result.get('title_english'):
            titles.append(result['title_english'])
        for syn in result.get('title_synonyms', []):
            if syn:
                titles.append(syn)

        for title in titles:
            title_words = set(re.sub(r'[^a-z0-9 ]', ' ', title.lower()).split())
            overlap = show_words & title_words
            if len(overlap) >= max(1, len(show_words) * 0.5):
                return True

        return False

    def get_episode_map(self, mal_id, merge_continuation=False):
        """Fetches English episode titles using configured API priority.

        If merge_continuation is True, also fetches the immediate TV sequel's
        episodes and appends them (renumbered) after the base entry's episodes.
        This handles split-cour seasons like 'To the Top' (13 eps) + 'To the Top
        Part 2' (12 eps) being treated as one user-facing season.
        """
        cache_key = (mal_id, merge_continuation)
        if cache_key in self.episode_cache:
            return self.episode_cache[cache_key]

        ep_map = self._fetch_episode_map_single(mal_id)

        if merge_continuation and ep_map:
            # Check for a direct TV sequel to merge
            chain = self._get_sequel_chain_for(mal_id)
            if chain:
                cont = chain[0]  # immediate sequel
                cont_id = cont.get('mal_id')
                if cont_id:
                    cont_map = self._fetch_episode_map_single(cont_id)
                    if cont_map:
                        base_max = max((int(k) for k in ep_map if k.isdigit()), default=0)
                        for ep_str, title in cont_map.items():
                            if ep_str.isdigit():
                                new_ep = str(int(ep_str) + base_max)
                                if new_ep not in ep_map:
                                    ep_map[new_ep] = title

        self.episode_cache[cache_key] = ep_map
        return ep_map

    def _fetch_episode_map_single(self, mal_id):
        """Fetch episode map for a single MAL ID from configured sources."""
        ep_map = {}
        id_map = {}
        for name in self.episode_order:
            if name in ('jikan', 'anilist', 'mal'):
                id_map[name] = mal_id

        for api in self.episode_order:
            src = self.pool.get(api)
            if not src:
                continue
            sid = id_map.get(api)
            if sid is None:
                continue
            try:
                api_map = src.get_episodes(sid)
            except Exception:
                continue
            if not ep_map:
                ep_map = api_map
            else:
                for ep_num, title in api_map.items():
                    existing = ep_map.get(ep_num, '')
                    if not existing or self._looks_non_english(existing):
                        ep_map[ep_num] = title

        return ep_map

    @staticmethod
    def _looks_non_english(text):
        """Heuristic: detect if episode title is Romaji rather than English."""
        if not text:
            return True
        if any(ord(c) > 127 for c in text):
            return True
        words = text.lower().split()
        if not words:
            return False
        romaji_endings = re.compile(
            r'^[a-z]+(ou|uu|shi|chi|tsu|ka|ki|ku|ke|ko|sa|su|se|so|'
            r'ta|te|to|na|ni|nu|ne|no|ha|hi|fu|he|ho|ma|mi|mu|me|mo|'
            r'ya|yu|yo|ra|ri|ru|re|ro|wa|wo|ga|gi|gu|ge|go|za|zu|ze|zo|'
            r'da|de|do|ba|bi|bu|be|bo|pa|pi|pu|pe|po)$', re.I
        )
        romaji_count = sum(1 for w in words if romaji_endings.match(w))
        return romaji_count >= len(words) * 0.6


# ================= PROCESSOR =================
class AnimeProcessor:
    def __init__(self):
        self.log_obj, self.cfg = common.setup_logger("animeproc")
        self.brain = CentralLogic()

        try:
            self.input_dir = Path(self.cfg['paths']['series_pipeline']['staged']['anime'])
            self.output_root = Path(self.cfg['paths']['output']['anime_shows'])
        except KeyError as e:
            self.log(f"Config Key Missing: {e}", "error")
            sys.exit(1)

        cache_root = Path(self.cfg.get('cache', {}).get('root', str(common.BASE_DIR / 'cache')))
        self.engine = AnimeSearchEngine(cache_root / "anime", self.cfg, logger=self.log_obj)
        self.show_cache = ShowCache(self.cfg)
        self.noise_learner = NoiseLearner(self.cfg)
        self.regex = re.compile(r"(.+?)[ ._-]*S(\d{1,2})E(\d{1,4})", re.IGNORECASE)

        self._fail_counts = {}  # {filename: retry_count} for no-match files
        self._MAX_RETRIES = 3

        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_root.mkdir(parents=True, exist_ok=True)

    def log(self, msg, level="info"):
        if level == "error": self.log_obj.error(msg)
        elif level == "warning": self.log_obj.warning(msg)
        else: self.log_obj.info(msg)

    def _get_failed_dir(self) -> Path:
        return Path(self.cfg['paths']['series_pipeline'].get('failed', ''))

    def _get_review_dir(self) -> Path:
        return Path(self.cfg['paths']['series_pipeline'].get('review', ''))

    def _write_reason(self, dest_path: Path, candidate: str, detail: str):
        try:
            reason_path = dest_path.with_name(dest_path.name + ".reason.json")
            reason_path.write_text(json.dumps({
                "source": "animeprocessor", "reason": "no_match",
                "confidence": 0, "threshold": 0,
                "candidate": candidate, "best_match": None,
                "detail": detail,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, indent=2), encoding='utf-8')
        except Exception:
            pass

    def _get_dup_dir(self) -> Path:
        raw = self.cfg['paths']['series_pipeline'].get('duplicates', '')
        return Path(raw) if raw else Path(self.cfg['paths']['roots']['manager']) / "Duplicates" / "Shows"

    def _write_dup(self, dest_path: Path, final_name: str, existing_path: Path):
        """Write a .dup.json sidecar next to a file moved to Duplicates."""
        try:
            dup_path = dest_path.with_name(dest_path.name + ".dup.json")
            existing_size = existing_path.stat().st_size if existing_path.exists() else 0
            new_size = dest_path.stat().st_size if dest_path.exists() else 0
            dup_path.write_text(json.dumps({
                "source": "animeprocessor",
                "final_name": final_name,
                "existing_path": str(existing_path),
                "existing_size_mb": round(existing_size / (1024*1024), 1),
                "new_size_mb": round(new_size / (1024*1024), 1),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, indent=2), encoding='utf-8')
        except Exception:
            pass

    def _resolve_english_title(self, season_data, candidate):
        """English title resolution chain."""
        if season_data.get('title_english'):
            return season_data['title_english']
        # AniList fallback
        anilist = self.engine.pool.get('anilist')
        if anilist and season_data.get('mal_id'):
            anilist_title = anilist.get_english_title(season_data['mal_id'])
            if anilist_title:
                return anilist_title
        return season_data.get('title') or candidate

    def _is_generic_season_phrase(self, text):
        """Check if text is a generic season phrase to be stripped."""
        generic_patterns = [
            r"^\s*season\s+\d+\s*$",
            r"^\s*\d+(?:st|nd|rd|th)\s+season\s*$",
            r"^\s*season\s+\d+\s+part\s+\d+\s*$",
            r"^\s*part\s+\d+\s*$",
        ]
        text_lower = text.lower().strip()
        for pattern in generic_patterns:
            if re.match(pattern, text_lower):
                return True
        return False

    def _extract_season_subtitle(self, season_title, root_name, season_num):
        """Extract meaningful subtitle from season title."""
        safe_season = self.brain.sanitize_for_filesystem(season_title)
        safe_root = self.brain.sanitize_for_filesystem(root_name)

        if safe_root.lower() in safe_season.lower() and len(safe_season) > len(safe_root):
            sub = re.sub(re.escape(safe_root), "", safe_season, flags=re.IGNORECASE).strip(" -:")
            if sub and not self._is_generic_season_phrase(sub):
                return sub
            return None

        if safe_season.lower() != safe_root.lower() and not self._is_generic_season_phrase(safe_season):
            return safe_season

        return None

    def _resolve_season_folder(self, season_num, season_data, s1_data, safe_root):
        """Smart season folder naming.

        Tries romaji title first (has distinctive names like 'Karasuno High School
        vs Shiratorizawa Academy'), then English title as fallback.
        Uses S1's romaji/English titles respectively for comparison to avoid
        spelling mismatches (e.g. MAL English 'Haikyu!!' vs romaji 'Haikyuu!!').
        """
        if season_num == 1:
            return f"Season {season_num:02}"

        # Use S1's romaji title for comparing with romaji season titles,
        # and S1's English title for comparing with English season titles.
        s1_romaji = self.brain.sanitize_for_filesystem(
            s1_data.get('title', '') if s1_data else ''
        )
        s1_english = self.brain.sanitize_for_filesystem(safe_root)

        # Try romaji title first (usually has the distinctive season subtitle)
        romaji_title = season_data.get('title', '')
        if romaji_title:
            root_for_cmp = s1_romaji if s1_romaji else s1_english
            subtitle = self._extract_season_subtitle(romaji_title, root_for_cmp, season_num)
            if subtitle:
                return f"Season {season_num:02} - {subtitle}"

        # Fallback to English title
        eng_title = self._resolve_english_title(season_data, "")
        if eng_title:
            subtitle = self._extract_season_subtitle(eng_title, s1_english, season_num)
            if subtitle:
                return f"Season {season_num:02} - {subtitle}"

        return f"Season {season_num:02}"

    def _strip_season_from_candidate(self, candidate, season_num):
        """Strip generic season indicators to get the base show name.

        E.g., 'Haikyu!! 2nd Season' -> 'Haikyu!!', 'Naruto Season 3' -> 'Naruto'.
        Leaves non-generic subtitles intact: 'Haikyu!! To the Top' -> unchanged.
        """
        if season_num <= 1:
            return candidate

        patterns = [
            r'\s+\d+(?:st|nd|rd|th)\s+season\b',
            r'\s+season\s+\d+',
            r'\b(?:second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)\s+season\b',
        ]

        stripped = candidate
        for pattern in patterns:
            stripped = re.sub(pattern, '', stripped, flags=re.IGNORECASE)
        stripped = re.sub(r'\s*[-:]\s*$', '', stripped).strip()
        stripped = re.sub(r'\s+', ' ', stripped).strip()

        return stripped if stripped else candidate

    def process_file(self, file_path: Path):
        try:
            match = self.regex.match(file_path.name)
            season_num = None
            ep_num = None
            candidate = None

            # --- SxxEyy format (normal case) ---
            if match:
                candidate, _, _ = self.brain.get_title_candidate(file_path.name)
                candidate = candidate.strip() if candidate else candidate
                season_num = int(match.group(2))
                ep_num = int(match.group(3))

                # Strip generic season suffixes for S2+
                if season_num > 1:
                    stripped = self._strip_season_from_candidate(candidate, season_num)
                    if stripped != candidate:
                        self.log(f"Season strip: '{candidate}' -> '{stripped}'")
                        candidate = stripped

            # If still no match, skip
            if not candidate or season_num is None or ep_num is None:
                return

            self.log(f"Processing: {file_path.name}")
            self.log(f"[DEBUG] candidate='{candidate}' season={season_num} ep={ep_num}")

            # --- META SIDECAR CHECK (from classifier) ---
            meta_path = file_path.with_name(file_path.name + ".meta.json")
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                    meta_ids = meta.get("api_ids", {})
                    if meta_ids.get("mal_id"):
                        self.log(f"Sidecar IDs: {candidate} (mal:{meta_ids['mal_id']})")
                except Exception:
                    pass

            # --- CACHE CHECK ---
            cache_hit = self.show_cache.lookup(candidate)
            if not cache_hit:
                self.log(f"[DEBUG] Exact cache miss for '{candidate}', trying fuzzy...")
                cache_hit = self.show_cache.lookup_fuzzy(candidate)
                if not cache_hit:
                    self.log(f"[DEBUG] Fuzzy cache miss for '{candidate}'")
                else:
                    self.log(f"[DEBUG] Fuzzy cache HIT for '{candidate}' -> {cache_hit.get('canonical_name')}")
            else:
                self.log(f"[DEBUG] Exact cache HIT for '{candidate}' -> {cache_hit.get('canonical_name')}")

            season_data = None
            s1_data = None
            root_name = None

            if cache_hit and cache_hit.get("api_ids", {}).get("mal_id"):
                cached_seasons = cache_hit.get("seasons", {})
                if str(season_num) in cached_seasons:
                    season_data = cached_seasons[str(season_num)]
                if "1" in cached_seasons:
                    s1_data = cached_seasons["1"]
                root_name = cache_hit.get("canonical_name")
                self.log(f"Cache hit: {candidate} -> {root_name}")
                self.log(f"[DEBUG] From cache: s1_data={s1_data is not None} season_data={season_data is not None} cached_seasons={list(cached_seasons.keys())}")

            # Fetch S1 data first (needed for MAL ID deduplication)
            if not s1_data:
                self.log(f"[DEBUG] Fetching S1 data for '{candidate}'...")
                s1_data = self.engine.search_season_entry(candidate, 1)
                if s1_data:
                    self.log(f"[DEBUG] S1 result: mal_id={s1_data.get('mal_id')} title='{s1_data.get('title')}' eng='{s1_data.get('title_english')}'")
                else:
                    self.log(f"[DEBUG] S1 search returned None")

            # If no season data from cache, search with dedup guard
            if not season_data:
                s1_id = s1_data['mal_id'] if s1_data else None
                self.log(f"[DEBUG] Fetching S{season_num} data for '{candidate}' (s1_mal_id={s1_id})...")
                season_data = self.engine.search_season_entry(candidate, season_num, s1_mal_id=s1_id)
                if season_data:
                    self.log(f"[DEBUG] S{season_num} result: mal_id={season_data.get('mal_id')} title='{season_data.get('title')}' eng='{season_data.get('title_english')}'")
                else:
                    self.log(f"[DEBUG] S{season_num} search returned None")
            if not season_data:
                # Track retries — move to review after max attempts
                fname = file_path.name
                self._fail_counts[fname] = self._fail_counts.get(fname, 0) + 1
                if self._fail_counts[fname] >= self._MAX_RETRIES:
                    self.log(f"No match for {candidate} S{season_num} after {self._MAX_RETRIES} attempts. Moving to Review.", "warning")
                    review_dir = self._get_review_dir()
                    review_dir.mkdir(parents=True, exist_ok=True)
                    dest = review_dir / file_path.name
                    shutil.move(str(file_path), str(dest))
                    self._write_reason(dest, candidate, f"No anime match found for '{candidate}' season {season_num} after {self._MAX_RETRIES} attempts")
                    # Clean up sidecars
                    meta = file_path.with_name(file_path.name + ".meta.json")
                    if meta.exists():
                        meta.unlink()
                    del self._fail_counts[fname]
                else:
                    self.log(f"No match for {candidate} S{season_num} (attempt {self._fail_counts[fname]}/{self._MAX_RETRIES})", "warning")
                return

            # --- RESOLVE ENGLISH ROOT NAME ---
            if not root_name:
                if s1_data:
                    self.log(f"[DEBUG] Resolving root_name from s1_data: mal_id={s1_data.get('mal_id')} title='{s1_data.get('title')}' eng='{s1_data.get('title_english')}'")
                    root_name = self._resolve_english_title(s1_data, candidate)
                else:
                    self.log(f"[DEBUG] Resolving root_name from season_data (no s1): mal_id={season_data.get('mal_id')}")
                    root_name = self._resolve_english_title(season_data, candidate)
                self.log(f"[DEBUG] root_name resolved to: '{root_name}'")

            safe_root = self.brain.sanitize_for_filesystem(root_name)

            # --- EPISODE TITLE ---
            # First try without continuation merge
            ep_map = self.engine.get_episode_map(season_data['mal_id'])

            lookup_ep = ep_num
            if (s1_data and season_data and season_num > 1
                    and season_data['mal_id'] == s1_data['mal_id']):
                cached_seasons = {}
                if cache_hit:
                    cached_seasons = cache_hit.get("seasons", {})

                offset = 0
                for prev_s in range(1, season_num):
                    prev_key = str(prev_s)
                    if prev_key in cached_seasons:
                        prev_ep_count = cached_seasons[prev_key].get("episode_count", 0)
                        offset += prev_ep_count
                    else:
                        offset = 0
                        break

                if offset > 0:
                    lookup_ep = offset + ep_num

            ep_title = ep_map.get(str(lookup_ep), "")

            # If episode not found, try merging continuation sequel episodes
            # (handles split-cour like "To the Top" + "To the Top Part 2")
            if not ep_title and str(lookup_ep) not in ep_map:
                ep_map = self.engine.get_episode_map(season_data['mal_id'], merge_continuation=True)
                ep_title = ep_map.get(str(lookup_ep), "")

            if self.brain.is_non_latin_title(ep_title):
                ep_title = ""

            # --- CACHE + LEARN ---
            mal_id = s1_data['mal_id'] if s1_data else season_data['mal_id']
            self.show_cache.register(
                root_name, "anime",
                {"mal_id": mal_id},
                raw_name=candidate, confidence=100
            )
            slug = self.show_cache._make_slug(root_name)
            if s1_data:
                self.show_cache.update_season(slug, 1, {
                    "mal_id": s1_data['mal_id'],
                    "title_english": s1_data.get('title_english'),
                    "title": s1_data.get('title'),
                })
            season_cache_data = {
                "mal_id": season_data['mal_id'],
                "title_english": season_data.get('title_english'),
                "title": season_data.get('title'),
            }
            existing_season = cache_hit.get("seasons", {}).get(str(season_num), {}) if cache_hit else {}
            prev_max = existing_season.get("episode_count", 0)
            season_cache_data["episode_count"] = max(prev_max, ep_num)
            self.show_cache.update_season(slug, season_num, season_cache_data)
            # Noise learner disabled: write-only system (structpilot never reads learned patterns)

            # --- BUILD PATH ---
            season_folder = self._resolve_season_folder(season_num, season_data, s1_data, safe_root)

            clean_ep = self.brain.sanitize_for_filesystem(ep_title) if ep_title else ""
            new_name = f"{safe_root} - S{season_num:02}E{ep_num:02}"
            if clean_ep:
                new_name += f" - {clean_ep}"
            new_name += file_path.suffix

            dest_dir = self.output_root / safe_root / season_folder
            target = dest_dir / new_name

            # --- EXECUTE ---
            if target.exists():
                self.log(f"Duplicate: {target.name} already in library. Moving to Duplicates.", "warning")
                dup_dir = self._get_dup_dir()
                dup_dir.mkdir(parents=True, exist_ok=True)
                dup_dest = dup_dir / file_path.name
                shutil.move(str(file_path), str(dup_dest))
                self._write_dup(dup_dest, target.name, target)
                meta = file_path.with_name(file_path.name + ".meta.json")
                if meta.exists():
                    meta.unlink()
                return

            dest_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file_path), str(target))
            self.log(f"Library Import: {target.name}")

            # Clean up meta sidecar
            meta = file_path.with_name(file_path.name + ".meta.json")
            if meta.exists():
                meta.unlink()

        except Exception as e:
            self.log(f"Error processing {file_path.name}: {e}", "error")

    def run(self):
        self.log(f"Anime Service Started. Watching: {self.input_dir}")
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
    AnimeProcessor().run()
