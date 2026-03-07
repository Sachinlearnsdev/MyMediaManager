#!/usr/bin/env python3
"""
media_sources.py -- Centralized API client layer.

Single file housing all external media data source clients.
Each client normalizes results to a standard format. SourcePool
provides the factory + chain-dispatch interface used by processors.

Clients: MAL, Jikan, AniList, Kitsu, TVDB, TMDB, Trakt, TVmaze, OMDb
"""

import os
import sys
import time
import json
import re
import hashlib
import requests
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import datetime, timedelta

_BIN_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_BIN_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from bin.constants import (
    JIKAN_BASE_URL, ANILIST_API_URL, MAL_OFFICIAL_BASE_URL,
    TVDB_API_BASE, TMDB_API_BASE,
    TRAKT_API_BASE, TVMAZE_API_BASE, KITSU_API_BASE, OMDB_API_BASE,
)

import logging
_log = logging.getLogger("media_sources")


# ============================================================
# INFRASTRUCTURE
# ============================================================

class RateLimiter:
    """Per-host proactive rate limiter (thread-safe)."""

    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.monotonic()


class RetryPolicy:
    """Exponential backoff retry for transient failures (429, 5xx)."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0):
        self.max_retries = max_retries
        self.base_delay = base_delay

    def execute(self, func, *args, **kwargs):
        last_exc = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = func(*args, **kwargs)
                if resp.status_code == 429 and attempt < self.max_retries:
                    time.sleep(self.base_delay * (2 ** attempt))
                    continue
                return resp
            except (requests.ConnectionError, requests.Timeout) as e:
                last_exc = e
                if attempt < self.max_retries:
                    time.sleep(self.base_delay * (2 ** attempt))
        raise last_exc if last_exc else RuntimeError("RetryPolicy exhausted")


# ============================================================
# BASE CLASS
# ============================================================

class MediaSource(ABC):
    """Abstract base class for all media data source clients.

    Required:
        search(query, media_type) -> list[dict]

    Optional (override as needed):
        get_details(source_id) -> dict|None
        get_episodes(source_id, season) -> dict        {ep_num_str: title_str}
        get_english_title(source_id) -> str|None
        get_season_subtitle(source_id, season) -> str|None
        get_sequel_chain(source_id) -> list|None
    """

    SOURCE_NAME = "base"    # Override in subclasses

    def __init__(self, cfg: dict, cache_dir: Path):
        self.cfg = cfg
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.retry = RetryPolicy()
        # Consolidated cache: single file per source directory
        self._consolidated_path = self.cache_dir / "_consolidated.json"
        self._cache_store = self._load_consolidated()
        self._cache_dirty = False
        self._migrate_individual_files()

    @abstractmethod
    def search(self, query: str, media_type: str = "tv") -> list:
        """Search for media. Returns list of standard result dicts."""
        ...

    def get_details(self, source_id) -> dict | None:
        return None

    def get_episodes(self, source_id, season: int = 1) -> dict:
        return {}

    def get_english_title(self, source_id) -> str | None:
        return None

    def get_season_subtitle(self, source_id, season: int) -> str | None:
        return None

    def get_sequel_chain(self, source_id) -> list | None:
        return None

    # -- Consolidated cache --

    def _load_consolidated(self) -> dict:
        """Load the consolidated cache file (all entries for this source dir)."""
        if self._consolidated_path.exists():
            try:
                return json.loads(self._consolidated_path.read_text(encoding='utf-8'))
            except Exception:
                pass
        return {}

    def _save_consolidated(self):
        """Persist consolidated cache to disk."""
        if not self._cache_dirty:
            return
        try:
            self._consolidated_path.write_text(
                json.dumps(self._cache_store, ensure_ascii=False),
                encoding='utf-8'
            )
            self._cache_dirty = False
        except Exception:
            pass

    def _migrate_individual_files(self):
        """Migrate any existing individual cache files into the consolidated store."""
        migrated = 0
        for f in list(self.cache_dir.iterdir()):
            if f.name.startswith('_') or not f.name.endswith('.json'):
                continue
            # Individual cache files are named: {source}_{key_type}_{md5}.json
            try:
                data = json.loads(f.read_text(encoding='utf-8'))
                key = f.stem  # e.g. "mal_sequels_abc123"
                if key not in self._cache_store:
                    self._cache_store[key] = data
                    self._cache_dirty = True
                f.unlink()
                migrated += 1
            except Exception:
                pass
        if migrated > 0:
            self._save_consolidated()
            _log.debug(f"Migrated {migrated} cache files into {self._consolidated_path.name}")

    def _cache_key(self, key_type: str, identifier) -> str:
        safe_id = hashlib.md5(str(identifier).encode('utf-8')).hexdigest()
        return f"{self.SOURCE_NAME}_{key_type}_{safe_id}"

    def _read_cache(self, key_type: str, identifier, ttl_hours: int = 24):
        key = self._cache_key(key_type, identifier)
        entry = self._cache_store.get(key)
        if not entry:
            return None
        try:
            cached_at = datetime.fromisoformat(entry.get("cached_at", "1970-01-01"))
            if datetime.now() - cached_at < timedelta(hours=ttl_hours):
                return entry.get("payload")
        except Exception:
            pass
        return None

    def _write_cache(self, key_type: str, identifier, payload):
        key = self._cache_key(key_type, identifier)
        self._cache_store[key] = {
            "payload": payload,
            "cached_at": datetime.now().isoformat(),
        }
        self._cache_dirty = True
        self._save_consolidated()

    def cleanup_expired(self, default_ttl_hours: int = 24):
        """Remove expired entries from the consolidated cache."""
        now = datetime.now()
        expired = []
        for key, entry in self._cache_store.items():
            try:
                cached_at = datetime.fromisoformat(entry.get("cached_at", "1970-01-01"))
                # Use longer TTL for sequel chains (7 days)
                ttl = 168 if "sequel" in key else default_ttl_hours
                if now - cached_at > timedelta(hours=ttl):
                    expired.append(key)
            except Exception:
                expired.append(key)
        for key in expired:
            del self._cache_store[key]
        if expired:
            self._cache_dirty = True
            self._save_consolidated()
            _log.debug(f"Cleaned {len(expired)} expired entries from {self._consolidated_path.name}")
        return len(expired)

    @staticmethod
    def _std_result(source, source_id, title, title_english=None,
                    media_type="tv", year=None, **extra):
        """Build a standard search result dict."""
        r = {
            'source': source,
            'source_id': source_id,
            'title': title,
            'title_english': title_english,
            'media_type': media_type,
            'year': str(year) if year else None,
        }
        r.update(extra)
        return r


# ============================================================
# MAL Official API v2
# ============================================================

class MALClient(MediaSource):
    SOURCE_NAME = "mal"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.client_id = cfg.get('api_keys', {}).get('mal', '').strip()
        self.available = bool(self.client_id)
        if self.available:
            self.session.headers.update({'X-MAL-CLIENT-ID': self.client_id})

    def search(self, query, media_type="anime"):
        if not self.available:
            return []
        try:
            fields = "alternative_titles,media_type,num_episodes,start_date,related_anime"
            resp = self.retry.execute(
                self.session.get, f"{MAL_OFFICIAL_BASE_URL}/anime",
                params={"q": query, "limit": 10, "fields": fields}, timeout=10
            )
            if resp.status_code == 403:
                self.available = False
                return []
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json().get('data', []):
                node = item.get('node', {})
                alt = node.get('alternative_titles', {})
                mt = (node.get('media_type') or '').lower()
                year = (node.get('start_date') or '')[:4] or None
                results.append(self._std_result(
                    source='mal',
                    source_id=node.get('id'),
                    title=node.get('title', ''),
                    title_english=alt.get('en') or None,
                    media_type=mt if mt in ('tv', 'movie', 'ova', 'ona') else mt,
                    year=year,
                    title_synonyms=alt.get('synonyms', []),
                    _related_anime=node.get('related_anime') or [],
                ))
            return results
        except Exception as e:
            _log.warning(f"MAL search error: {e}")
        return []

    def get_details(self, mal_id):
        if not self.available:
            return None
        try:
            fields = "alternative_titles,media_type,num_episodes,start_date,related_anime"
            resp = self.retry.execute(
                self.session.get, f"{MAL_OFFICIAL_BASE_URL}/anime/{mal_id}",
                params={"fields": fields}, timeout=10
            )
            if resp.status_code == 403:
                self.available = False
                return None
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            _log.warning(f"MAL details error: {e}")
        return None

    def get_sequel_chain(self, mal_id):
        """Follow sequel chain via MAL Official API. Returns list of TV sequel entries."""
        if not self.available:
            return None
        cached = self._read_cache("sequels", mal_id, ttl_hours=168)
        if cached is not None:
            return cached

        chain = []
        visited = {mal_id}
        fields = "alternative_titles,media_type,num_episodes,related_anime"

        try:
            resp = self.retry.execute(
                self.session.get, f"{MAL_OFFICIAL_BASE_URL}/anime/{mal_id}",
                params={"fields": fields}, timeout=10
            )
            if resp.status_code == 403:
                self.available = False
                return None
            if resp.status_code != 200:
                return []
            current_related = resp.json().get('related_anime', [])
        except Exception:
            return []

        for _ in range(20):
            sequel_id = None
            for rel in (current_related or []):
                if rel.get('relation_type') == 'sequel':
                    node = rel.get('node', {})
                    sid = node.get('id')
                    if sid and sid not in visited:
                        sequel_id = sid
                        break
            if not sequel_id:
                break
            visited.add(sequel_id)

            try:
                resp = self.retry.execute(
                    self.session.get, f"{MAL_OFFICIAL_BASE_URL}/anime/{sequel_id}",
                    params={"fields": fields}, timeout=10
                )
                if resp.status_code == 403:
                    self.available = False
                    break
                if resp.status_code != 200:
                    break
                detail = resp.json()
                mt = (detail.get('media_type') or '').lower()
                current_related = detail.get('related_anime', [])

                if mt == 'tv':
                    alt = detail.get('alternative_titles', {})
                    chain.append({
                        'mal_id': sequel_id,
                        'title': detail.get('title'),
                        'title_english': alt.get('en'),
                    })
            except Exception:
                break

        self._write_cache("sequels", mal_id, chain)
        return chain


# ============================================================
# Jikan (MyAnimeList unofficial) v4
# ============================================================

class JikanClient(MediaSource):
    SOURCE_NAME = "jikan"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.limiter = RateLimiter(min_interval=1.0)

    def search(self, query, media_type="anime"):
        self.limiter.wait()
        try:
            resp = self.retry.execute(
                self.session.get, f"{JIKAN_BASE_URL}/anime",
                params={"q": query, "limit": 10}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json().get('data', []):
                mt = (item.get('type') or '').lower()
                year = None
                if item.get('aired', {}).get('from'):
                    year = item['aired']['from'][:4]
                results.append(self._std_result(
                    source='jikan',
                    source_id=item.get('mal_id'),
                    title=item.get('title', ''),
                    title_english=item.get('title_english'),
                    media_type=mt if mt in ('tv', 'movie', 'ova', 'ona') else mt,
                    year=year,
                    title_synonyms=item.get('title_synonyms', []),
                ))
            return results
        except Exception as e:
            _log.warning(f"Jikan search error: {e}")
        return []

    def get_episodes(self, mal_id, season=1):
        """Fetch episode titles from Jikan. Returns {ep_num_str: title}."""
        ep_map = {}
        page = 1
        while True:
            self.limiter.wait()
            try:
                resp = self.retry.execute(
                    self.session.get,
                    f"{JIKAN_BASE_URL}/anime/{mal_id}/episodes",
                    params={"page": page}, timeout=10
                )
                if resp.status_code != 200:
                    break
                payload = resp.json()
                for ep in payload.get('data', []):
                    e_num = str(ep.get('mal_id'))
                    title = ep.get('title') or ep.get('title_romanji', '')
                    ep_map[e_num] = title
                if not payload.get('pagination', {}).get('has_next_page'):
                    break
                page += 1
            except Exception:
                break
        return ep_map

    def get_sequel_chain(self, mal_id):
        """Follow sequel chain via Jikan relations API."""
        cached = self._read_cache("sequels", mal_id, ttl_hours=168)
        if cached is not None:
            return cached

        chain = []
        current_id = mal_id
        visited = {mal_id}

        for _ in range(20):
            self.limiter.wait()
            try:
                resp = self.retry.execute(
                    self.session.get,
                    f"{JIKAN_BASE_URL}/anime/{current_id}/relations",
                    timeout=10
                )
                if resp.status_code != 200:
                    break
                relations = resp.json().get('data', [])

                sequel_id = None
                for rel in relations:
                    if rel.get('relation') == 'Sequel':
                        for entry in rel.get('entry', []):
                            if entry.get('type') == 'anime' and entry['mal_id'] not in visited:
                                sequel_id = entry['mal_id']
                                break
                    if sequel_id:
                        break
                if not sequel_id:
                    break

                visited.add(sequel_id)

                self.limiter.wait()
                detail_resp = self.retry.execute(
                    self.session.get,
                    f"{JIKAN_BASE_URL}/anime/{sequel_id}",
                    timeout=10
                )
                if detail_resp.status_code != 200:
                    break
                detail = detail_resp.json().get('data', {})

                if detail.get('type', '').upper() == 'TV':
                    chain.append({
                        'mal_id': sequel_id,
                        'title': detail.get('title'),
                        'title_english': detail.get('title_english'),
                    })
                current_id = sequel_id
            except Exception:
                break

        self._write_cache("sequels", mal_id, chain)
        return chain


# ============================================================
# AniList GraphQL
# ============================================================

class AniListClient(MediaSource):
    SOURCE_NAME = "anilist"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.limiter = RateLimiter(min_interval=0.7)

    def search(self, query, media_type="anime"):
        self.limiter.wait()
        gql = '''
        query ($search: String) {
          Page(perPage: 10) {
            media(search: $search, type: ANIME) {
              idMal
              id
              title { english romaji }
              format
              startDate { year }
            }
          }
        }
        '''
        try:
            resp = self.retry.execute(
                self.session.post, ANILIST_API_URL,
                json={'query': gql, 'variables': {'search': query}}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for m in resp.json().get('data', {}).get('Page', {}).get('media', []):
                fmt = (m.get('format') or '').lower()
                mt_map = {'tv': 'tv', 'tv_short': 'tv', 'movie': 'movie',
                          'ova': 'ova', 'ona': 'ona', 'special': 'special'}
                mt = mt_map.get(fmt, fmt)
                results.append(self._std_result(
                    source='anilist',
                    source_id=m.get('idMal') or m.get('id'),
                    title=m.get('title', {}).get('romaji', ''),
                    title_english=m.get('title', {}).get('english'),
                    media_type=mt,
                    year=m.get('startDate', {}).get('year'),
                    anilist_id=m.get('id'),
                ))
            return results
        except Exception as e:
            _log.warning(f"AniList search error: {e}")
        return []

    def get_english_title(self, mal_id):
        """Fetch English title by MAL ID via AniList."""
        self.limiter.wait()
        gql = '''
        query ($id: Int) {
          Media (idMal: $id, type: ANIME) {
            title { english romaji }
          }
        }
        '''
        try:
            resp = self.retry.execute(
                self.session.post, ANILIST_API_URL,
                json={'query': gql, 'variables': {'id': mal_id}}, timeout=10
            )
            if resp.status_code == 200:
                data = resp.json().get('data', {}).get('Media', {})
                if data:
                    t = data.get('title', {})
                    return t.get('english') or t.get('romaji')
        except Exception:
            pass
        return None

    def get_episodes(self, mal_id, season=1):
        """Fetch episode titles via AniList streaming episodes."""
        self.limiter.wait()
        gql = '''
        query ($id: Int) {
          Media (idMal: $id, type: ANIME) {
            streamingEpisodes { title }
          }
        }
        '''
        ep_map = {}
        try:
            resp = self.retry.execute(
                self.session.post, ANILIST_API_URL,
                json={'query': gql, 'variables': {'id': mal_id}}, timeout=10
            )
            if resp.status_code == 200:
                data = resp.json().get('data', {}).get('Media', {})
                if data and data.get('streamingEpisodes'):
                    for ep in data['streamingEpisodes']:
                        raw_title = ep.get('title', '')
                        m = re.match(r'Episode\s+(\d+)\s+-\s+(.+)', raw_title, re.IGNORECASE)
                        if m:
                            ep_map[str(int(m.group(1)))] = m.group(2).strip()
        except Exception:
            pass
        return ep_map


# ============================================================
# Kitsu JSON:API
# ============================================================

class KitsuClient(MediaSource):
    SOURCE_NAME = "kitsu"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.limiter = RateLimiter(min_interval=0.5)

    def search(self, query, media_type="anime"):
        self.limiter.wait()
        try:
            resp = self.retry.execute(
                self.session.get, f"{KITSU_API_BASE}/api/edge/anime",
                params={"filter[text]": query, "page[limit]": 10}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json().get('data', []):
                attrs = item.get('attributes', {})
                subtype = (attrs.get('subtype') or '').lower()
                mt_map = {'tv': 'tv', 'movie': 'movie', 'ova': 'ova', 'ona': 'ona',
                          'special': 'special'}
                mt = mt_map.get(subtype, subtype)
                year = None
                if attrs.get('startDate'):
                    year = attrs['startDate'][:4]
                results.append(self._std_result(
                    source='kitsu',
                    source_id=item.get('id'),
                    title=attrs.get('canonicalTitle', ''),
                    title_english=attrs.get('titles', {}).get('en') or attrs.get('titles', {}).get('en_us'),
                    media_type=mt,
                    year=year,
                    kitsu_id=item.get('id'),
                ))
            return results
        except Exception as e:
            _log.warning(f"Kitsu search error: {e}")
        return []

    def get_episodes(self, kitsu_id, season=1):
        """Fetch episode titles from Kitsu."""
        self.limiter.wait()
        ep_map = {}
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{KITSU_API_BASE}/api/edge/anime/{kitsu_id}/episodes",
                params={"page[limit]": 50, "sort": "number"}, timeout=10
            )
            if resp.status_code == 200:
                for ep in resp.json().get('data', []):
                    attrs = ep.get('attributes', {})
                    num = attrs.get('number')
                    title = attrs.get('titles', {}).get('en_us') or attrs.get('canonicalTitle', '')
                    if num:
                        ep_map[str(num)] = title
        except Exception as e:
            _log.warning(f"Kitsu episodes error: {e}")
        return ep_map

    def get_sequel_chain(self, kitsu_id):
        """Follow sequel relationships via Kitsu media-relationships."""
        cached = self._read_cache("sequels", kitsu_id, ttl_hours=168)
        if cached is not None:
            return cached

        chain = []
        current_id = kitsu_id
        visited = {str(kitsu_id)}

        for _ in range(20):
            self.limiter.wait()
            try:
                resp = self.retry.execute(
                    self.session.get,
                    f"{KITSU_API_BASE}/api/edge/anime/{current_id}/relationships/media-relationships",
                    params={"include": "destination", "page[limit]": 20},
                    timeout=10
                )
                if resp.status_code != 200:
                    break

                data = resp.json()
                sequel_id = None
                for rel in data.get('data', []):
                    if rel.get('attributes', {}).get('role') == 'sequel':
                        dest = rel.get('relationships', {}).get('destination', {}).get('data', {})
                        if dest.get('type') == 'anime' and str(dest.get('id')) not in visited:
                            sequel_id = dest['id']
                            break

                if not sequel_id:
                    # Try from included data
                    for inc in data.get('included', []):
                        if inc.get('type') == 'anime' and str(inc.get('id')) not in visited:
                            attrs = inc.get('attributes', {})
                            subtype = (attrs.get('subtype') or '').lower()
                            if subtype == 'tv':
                                chain.append({
                                    'kitsu_id': inc['id'],
                                    'title': attrs.get('canonicalTitle'),
                                    'title_english': attrs.get('titles', {}).get('en'),
                                })
                    break

                visited.add(str(sequel_id))

                # Fetch sequel details
                self.limiter.wait()
                detail = self.retry.execute(
                    self.session.get,
                    f"{KITSU_API_BASE}/api/edge/anime/{sequel_id}",
                    timeout=10
                )
                if detail.status_code != 200:
                    break
                attrs = detail.json().get('data', {}).get('attributes', {})
                subtype = (attrs.get('subtype') or '').lower()
                if subtype == 'tv':
                    chain.append({
                        'kitsu_id': sequel_id,
                        'title': attrs.get('canonicalTitle'),
                        'title_english': attrs.get('titles', {}).get('en'),
                    })
                current_id = sequel_id
            except Exception:
                break

        self._write_cache("sequels", kitsu_id, chain)
        return chain


# ============================================================
# TVDB v4
# ============================================================

class TVDBClient(MediaSource):
    SOURCE_NAME = "tvdb"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.api_key = cfg.get('api_keys', {}).get('tvdb', '').strip()
        self.token = None
        self._series_extended = {}
        self._episode_cache = {}
        if self.api_key:
            self._authenticate()

    def _authenticate(self):
        try:
            resp = self.session.post(f"{TVDB_API_BASE}/login", json={"apikey": self.api_key})
            if resp.status_code == 200:
                self.token = resp.json()['data']['token']
                self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        except Exception:
            pass

    def _ensure_auth(self):
        if not self.token:
            self._authenticate()

    def search(self, query, media_type="tv"):
        if not self.api_key:
            return []
        self._ensure_auth()
        try:
            resp = self.retry.execute(
                self.session.get, f"{TVDB_API_BASE}/search",
                params={"query": query, "type": "series"}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json().get('data', []):
                year = item.get('year') or (item.get('first_air_time') or '')[:4] or None
                results.append(self._std_result(
                    source='tvdb',
                    source_id=item.get('tvdb_id'),
                    title=item.get('name', ''),
                    title_english=None,
                    media_type='tv',
                    year=year,
                    aliases=item.get('aliases') or [],
                ))
            return results
        except Exception as e:
            _log.warning(f"TVDB search error: {e}")
        return []

    def _fetch_extended(self, series_id):
        if series_id in self._series_extended:
            return self._series_extended[series_id]
        self._ensure_auth()
        data = {}
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{TVDB_API_BASE}/series/{series_id}/extended",
                params={"meta": "translations"}, timeout=10
            )
            if resp.status_code == 200:
                data = resp.json().get('data', {})
        except Exception:
            pass
        self._series_extended[series_id] = data
        return data

    def get_details(self, series_id):
        """Returns extended data dict with genres, translations, etc."""
        data = self._fetch_extended(series_id)
        if not data:
            return None
        title = data.get("name")
        genres = [g.get("slug", "").lower() for g in (data.get("genres") or [])]
        translations = (data.get("translations") or {}).get("nameTranslations") or []
        for t in translations:
            if t.get("language") == "eng":
                title = t.get("name")
                break
        return {"title": title, "genres": genres, "raw": data}

    def get_english_title(self, series_id):
        data = self._fetch_extended(series_id)
        if not data:
            return None
        translations = (data.get("translations") or {}).get("nameTranslations") or []
        for t in translations:
            if t.get("language") == "eng":
                return t.get("name")
        return data.get("name")

    def get_episodes(self, series_id, season=1):
        key = (series_id, season)
        if key in self._episode_cache:
            return self._episode_cache[key]
        self._ensure_auth()
        ep_map = {}
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{TVDB_API_BASE}/series/{series_id}/episodes/default",
                params={"season": season, "lang": "eng"}, timeout=10
            )
            if resp.status_code == 200:
                for e in resp.json().get('data', {}).get('episodes', []):
                    if e.get('seasonNumber') == season:
                        ep_map[str(e['number'])] = e.get('name')
        except Exception:
            pass
        self._episode_cache[key] = ep_map
        return ep_map

    def get_season_subtitle(self, series_id, season_num):
        data = self._fetch_extended(series_id)
        if not data:
            return None
        for s in (data.get('seasons') or []):
            if s.get('number') == season_num:
                name = s.get('name', '')
                if name and f"Season {season_num}" not in name:
                    return name
        return None


# ============================================================
# TMDB v3
# ============================================================

class TMDBClient(MediaSource):
    SOURCE_NAME = "tmdb"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.api_key = cfg.get('api_keys', {}).get('tmdb', '').strip()

    def search(self, query, media_type="tv"):
        if not self.api_key:
            return []
        endpoint = "search/tv" if media_type != "movie" else "search/movie"
        try:
            resp = self.retry.execute(
                self.session.get, f"{TMDB_API_BASE}/{endpoint}",
                params={"api_key": self.api_key, "query": query, "include_adult": False},
                timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json().get('results', []):
                if media_type == "movie":
                    title = item.get('title', '')
                    year = (item.get('release_date') or '')[:4] or None
                else:
                    title = item.get('name', '')
                    year = (item.get('first_air_date') or '')[:4] or None
                results.append(self._std_result(
                    source='tmdb',
                    source_id=item.get('id'),
                    title=title,
                    title_english=item.get('original_name') if media_type != "movie" else item.get('original_title'),
                    media_type=media_type,
                    year=year,
                ))
            return results
        except Exception as e:
            _log.warning(f"TMDB search error: {e}")
        return []

    def get_details(self, tmdb_id):
        """Fetch movie or TV details (with keywords appended for movies)."""
        if not self.api_key:
            return None
        # Try movie first (with keywords), then TV
        for endpoint in [f"movie/{tmdb_id}", f"tv/{tmdb_id}"]:
            try:
                params = {"api_key": self.api_key}
                if endpoint.startswith("movie/"):
                    params["append_to_response"] = "keywords"
                resp = self.retry.execute(
                    self.session.get, f"{TMDB_API_BASE}/{endpoint}",
                    params=params, timeout=10
                )
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                continue
        return None

    def get_keywords(self, tmdb_id):
        """Fetch keywords for a movie. Returns list of lowercase keyword name strings."""
        if not self.api_key:
            return []
        try:
            resp = self.retry.execute(
                self.session.get, f"{TMDB_API_BASE}/movie/{tmdb_id}/keywords",
                params={"api_key": self.api_key}, timeout=10
            )
            if resp.status_code == 200:
                return [k.get('name', '').lower() for k in resp.json().get('keywords', [])]
        except Exception as e:
            _log.warning(f"TMDB keywords error: {e}")
        return []

    def get_episodes(self, tmdb_id, season=1):
        """Fetch episode titles for a TV show season."""
        if not self.api_key:
            return {}
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{TMDB_API_BASE}/tv/{tmdb_id}/season/{season}",
                params={"api_key": self.api_key}, timeout=10
            )
            if resp.status_code == 200:
                ep_map = {}
                for ep in resp.json().get('episodes', []):
                    ep_map[str(ep.get('episode_number'))] = ep.get('name', '')
                return ep_map
        except Exception as e:
            _log.warning(f"TMDB episodes error: {e}")
        return {}

    def get_english_title(self, tmdb_id):
        details = self.get_details(tmdb_id)
        if details:
            return details.get('name') or details.get('title')
        return None


# ============================================================
# Trakt
# ============================================================

class TraktClient(MediaSource):
    SOURCE_NAME = "trakt"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.client_id = cfg.get('api_keys', {}).get('trakt', '').strip()
        if self.client_id:
            self.session.headers.update({
                'trakt-api-key': self.client_id,
                'trakt-api-version': '2',
                'Content-Type': 'application/json',
            })

    def search(self, query, media_type="tv"):
        if not self.client_id:
            return []
        trakt_type = "show" if media_type in ("tv", "cartoons") else "movie"
        try:
            resp = self.retry.execute(
                self.session.get, f"{TRAKT_API_BASE}/search/{trakt_type}",
                params={"query": query, "limit": 10}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json():
                obj = item.get(trakt_type, {})
                results.append(self._std_result(
                    source='trakt',
                    source_id=obj.get('ids', {}).get('trakt'),
                    title=obj.get('title', ''),
                    title_english=None,
                    media_type=media_type,
                    year=obj.get('year'),
                    ids=obj.get('ids', {}),
                ))
            return results
        except Exception as e:
            _log.warning(f"Trakt search error: {e}")
        return []

    def get_details(self, trakt_id):
        if not self.client_id:
            return None
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{TRAKT_API_BASE}/shows/{trakt_id}",
                params={"extended": "full"}, timeout=10
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None


# ============================================================
# TVmaze (free, no auth)
# ============================================================

class TVmazeClient(MediaSource):
    SOURCE_NAME = "tvmaze"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.limiter = RateLimiter(min_interval=0.5)

    def search(self, query, media_type="tv"):
        self.limiter.wait()
        try:
            resp = self.retry.execute(
                self.session.get, f"{TVMAZE_API_BASE}/search/shows",
                params={"q": query}, timeout=10
            )
            if resp.status_code != 200:
                return []
            results = []
            for item in resp.json():
                show = item.get('show', {})
                year = None
                if show.get('premiered'):
                    year = show['premiered'][:4]
                results.append(self._std_result(
                    source='tvmaze',
                    source_id=show.get('id'),
                    title=show.get('name', ''),
                    title_english=None,
                    media_type='tv',
                    year=year,
                    tvmaze_id=show.get('id'),
                ))
            return results
        except Exception as e:
            _log.warning(f"TVmaze search error: {e}")
        return []

    def get_episodes(self, tvmaze_id, season=1):
        """Fetch episode titles from TVmaze."""
        self.limiter.wait()
        ep_map = {}
        try:
            resp = self.retry.execute(
                self.session.get,
                f"{TVMAZE_API_BASE}/shows/{tvmaze_id}/episodes",
                timeout=10
            )
            if resp.status_code == 200:
                for ep in resp.json():
                    if ep.get('season') == season:
                        ep_map[str(ep.get('number'))] = ep.get('name', '')
        except Exception as e:
            _log.warning(f"TVmaze episodes error: {e}")
        return ep_map

    def get_english_title(self, tvmaze_id):
        self.limiter.wait()
        try:
            resp = self.retry.execute(
                self.session.get, f"{TVMAZE_API_BASE}/shows/{tvmaze_id}",
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json().get('name')
        except Exception:
            pass
        return None


# ============================================================
# OMDb
# ============================================================

class OMDbClient(MediaSource):
    SOURCE_NAME = "omdb"

    def __init__(self, cfg, cache_dir):
        super().__init__(cfg, cache_dir)
        self.api_key = cfg.get('api_keys', {}).get('omdb', '').strip()

    def search(self, query, media_type="movie"):
        if not self.api_key:
            return []
        omdb_type = "movie" if media_type == "movie" else "series"
        try:
            resp = self.retry.execute(
                self.session.get, OMDB_API_BASE,
                params={"apikey": self.api_key, "s": query, "type": omdb_type},
                timeout=10
            )
            if resp.status_code != 200:
                return []
            data = resp.json()
            if data.get('Response') != 'True':
                return []
            results = []
            for item in data.get('Search', []):
                results.append(self._std_result(
                    source='omdb',
                    source_id=item.get('imdbID'),
                    title=item.get('Title', ''),
                    title_english=None,
                    media_type=media_type,
                    year=item.get('Year'),
                ))
            return results
        except Exception as e:
            _log.warning(f"OMDb search error: {e}")
        return []

    def get_details(self, imdb_id):
        if not self.api_key:
            return None
        try:
            resp = self.retry.execute(
                self.session.get, OMDB_API_BASE,
                params={"apikey": self.api_key, "i": imdb_id, "plot": "full"},
                timeout=10
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('Response') == 'True':
                    return data
        except Exception:
            pass
        return None


# ============================================================
# SOURCE REGISTRY + POOL
# ============================================================

SOURCE_REGISTRY = {
    'mal':    MALClient,
    'jikan':  JikanClient,
    'anilist': AniListClient,
    'kitsu':  KitsuClient,
    'tvdb':   TVDBClient,
    'tmdb':   TMDBClient,
    'trakt':  TraktClient,
    'tvmaze': TVmazeClient,
    'omdb':   OMDbClient,
}


class SourcePool:
    """Factory + chain-dispatch for media sources.

    Lazy-instantiates clients on first use. Provides search_chain()
    to replace manual for-loop dispatch in processors.
    """

    def __init__(self, cfg: dict, cache_dir: Path):
        self.cfg = cfg
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._instances = {}

    def get(self, source_name: str) -> MediaSource | None:
        """Get or lazily create a source client by name."""
        if source_name in self._instances:
            return self._instances[source_name]
        cls = SOURCE_REGISTRY.get(source_name)
        if not cls:
            _log.warning(f"Unknown source: {source_name}")
            return None
        instance = cls(self.cfg, self.cache_dir / source_name)
        self._instances[source_name] = instance
        return instance

    def search_chain(self, query: str, source_order: list,
                     media_type: str = "tv", stop_on_results: bool = True) -> list:
        """Search through sources in priority order.

        Args:
            query: Search string
            source_order: List of source names, e.g. ['tvdb', 'tmdb', 'tvmaze']
            media_type: 'tv', 'movie', 'anime', etc.
            stop_on_results: If True, stop after first source returns results

        Returns:
            Combined list of standard result dicts (tagged with 'source')
        """
        all_results = []
        for name in source_order:
            src = self.get(name)
            if not src:
                continue
            try:
                results = src.search(query, media_type=media_type)
                if results:
                    all_results.extend(results)
                    if stop_on_results:
                        break
            except Exception as e:
                _log.warning(f"search_chain: {name} error: {e}")
        return all_results

    def get_details_for(self, source_name: str, source_id):
        """Get details from a specific source."""
        src = self.get(source_name)
        if src:
            return src.get_details(source_id)
        return None

    def get_episodes_chain(self, source_order: list, source_id_map: dict,
                           season: int = 1) -> dict:
        """Fetch episode titles using first successful source.

        Args:
            source_order: Priority list, e.g. ['tvdb', 'tmdb', 'tvmaze']
            source_id_map: {source_name: source_id}, e.g. {'tvdb': '12345', 'tmdb': '678'}
            season: Season number

        Returns:
            {ep_num_str: title_str}
        """
        for name in source_order:
            sid = source_id_map.get(name)
            if sid is None:
                continue
            src = self.get(name)
            if not src:
                continue
            try:
                ep_map = src.get_episodes(sid, season=season)
                if ep_map:
                    return ep_map
            except Exception as e:
                _log.warning(f"get_episodes_chain: {name} error: {e}")
        return {}

    def get_english_title_chain(self, source_order: list, source_id_map: dict) -> str | None:
        """Fetch English title from first successful source."""
        for name in source_order:
            sid = source_id_map.get(name)
            if sid is None:
                continue
            src = self.get(name)
            if not src:
                continue
            try:
                title = src.get_english_title(sid)
                if title:
                    return title
            except Exception:
                pass
        return None

    def get_sequel_chain_from(self, source_order: list, source_id_map: dict) -> list | None:
        """Follow sequel chain from first successful source."""
        for name in source_order:
            sid = source_id_map.get(name)
            if sid is None:
                continue
            src = self.get(name)
            if not src:
                continue
            try:
                chain = src.get_sequel_chain(sid)
                if chain is not None:
                    return chain
            except Exception:
                pass
        return None

    def cleanup_all_caches(self, ttl_hours: int = 24):
        """Clean expired entries from all instantiated source caches."""
        total = 0
        # Also process source dirs that haven't been instantiated
        for subdir in self.cache_dir.iterdir():
            if subdir.is_dir() and subdir.name in SOURCE_REGISTRY:
                src = self.get(subdir.name)
                if src:
                    total += src.cleanup_expired(default_ttl_hours=ttl_hours)
        if total:
            _log.info(f"Cache cleanup: removed {total} expired entries total")
