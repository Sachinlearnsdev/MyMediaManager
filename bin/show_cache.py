#!/usr/bin/env python3
"""
show_cache.py -- Persistent show-name-level cache.

Maps raw filename variants to canonical show identities,
allowing subsequent files to skip API calls entirely.

Features:
- Confidence-gated registration (low-confidence matches don't pollute cache)
- TTL-based expiry (high-confidence = 30d, medium = 7d)
- Genre + country storage for proper routing on cache hits
- Fuzzy matching with short-title protection
"""

import json
import re
from pathlib import Path
from datetime import datetime, timedelta
from difflib import SequenceMatcher


# Minimum confidence to register in cache (below this, files still process but don't cache)
MIN_CACHE_CONFIDENCE = 70

# TTL tiers based on confidence
TTL_HIGH_DAYS = 30    # confidence >= 90
TTL_MEDIUM_DAYS = 7   # confidence 70-89


class ShowCache:
    def __init__(self, cfg):
        cache_cfg = cfg.get('cache', {})
        self.cache_path = Path(cache_cfg.get('show_cache_file', 'cache/show_cache.json'))
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()

    def _load(self) -> dict:
        if self.cache_path.exists():
            try:
                return json.loads(self.cache_path.read_text(encoding='utf-8'))
            except Exception:
                pass
        return {"_meta": {"version": 2, "last_updated": ""}, "shows": {}}

    def save(self):
        self._data["_meta"]["last_updated"] = datetime.now().isoformat()
        self.cache_path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )

    @staticmethod
    def _make_slug(name: str) -> str:
        slug = re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')
        return slug

    @staticmethod
    def _calc_expiry(confidence: int) -> str:
        """Calculate expiry timestamp based on confidence tier."""
        if confidence >= 90:
            expires = datetime.now() + timedelta(days=TTL_HIGH_DAYS)
        else:
            expires = datetime.now() + timedelta(days=TTL_MEDIUM_DAYS)
        return expires.isoformat()

    def _is_expired(self, entry: dict) -> bool:
        """Check if a cache entry has expired."""
        expires_at = entry.get("expires_at")
        if not expires_at:
            return False  # Legacy entries without expiry don't expire
        try:
            return datetime.now() > datetime.fromisoformat(expires_at)
        except (ValueError, TypeError):
            return False

    def lookup(self, raw_name: str) -> dict | None:
        """
        Checks all cached shows' raw_variants for an exact match.
        Returns the show entry dict if found, else None.
        Skips expired entries (returns None so caller re-queries API).
        """
        normalized = raw_name.lower().strip()
        for slug, entry in self._data["shows"].items():
            for variant in entry.get("raw_variants", []):
                if variant.lower().strip() == normalized:
                    if self._is_expired(entry):
                        return None  # Expired — force re-query
                    entry["last_hit"] = datetime.now().isoformat()
                    entry["hit_count"] = entry.get("hit_count", 0) + 1
                    return entry
        return None

    def lookup_fuzzy(self, raw_name: str, threshold: float = 0.90) -> dict | None:
        """
        Fuzzy matching against known variants.
        Only used as a secondary check if exact lookup fails.

        Safety: Short titles (<=6 chars) require exact match only
        to prevent "House" matching "Houses" or "The Flash" matching "The Clash".
        """
        normalized = raw_name.lower().strip()
        # Short titles: fuzzy matching is too risky
        if len(normalized) <= 6:
            return None
        best_match = None
        best_ratio = 0.0
        for slug, entry in self._data["shows"].items():
            if self._is_expired(entry):
                continue
            for variant in entry.get("raw_variants", []):
                ratio = SequenceMatcher(None, variant.lower(), normalized).ratio()
                if ratio > best_ratio and ratio >= threshold:
                    best_ratio = ratio
                    best_match = entry
        if best_match:
            best_match["last_hit"] = datetime.now().isoformat()
            best_match["hit_count"] = best_match.get("hit_count", 0) + 1
        return best_match

    def lookup_anime_base(self, title: str) -> dict | None:
        """Find a cached anime entry whose canonical_name is a word-prefix of the given title.

        Used by the classifier to normalize season-specific anime titles
        back to the base show name. E.g., if "Haikyu!!" is cached and
        query is "Haikyu!! Karasuno High vs Shiratorizawa Academy",
        returns the "Haikyu!!" entry.
        """
        def _words(text):
            return re.sub(r'[^a-z0-9 ]+', ' ', text.lower()).split()

        title_words = _words(title)
        if len(title_words) < 2:
            return None  # Need at least 2 words to have a prefix + suffix

        best_match = None
        best_word_count = 0

        for slug, entry in self._data["shows"].items():
            if self._is_expired(entry):
                continue
            if entry.get("media_type") != "anime":
                continue

            canonical_words = _words(entry["canonical_name"])
            if not canonical_words or len(canonical_words) >= len(title_words):
                continue  # Skip if canonical is same length or longer

            # Check if canonical words are a prefix of title words
            if title_words[:len(canonical_words)] == canonical_words:
                if len(canonical_words) > best_word_count:
                    best_word_count = len(canonical_words)
                    best_match = entry

        return best_match

    def register(self, canonical_name: str, media_type: str, api_ids: dict,
                 raw_name: str = None, confidence: int = 0,
                 season_data: dict = None,
                 genres: list = None, country: str = None) -> str | None:
        """
        Register a show in the cache. Returns the slug, or None if
        confidence is below the minimum caching threshold.

        Low-confidence matches (below MIN_CACHE_CONFIDENCE) are NOT cached
        to prevent cache poisoning from borderline matches.
        """
        if confidence < MIN_CACHE_CONFIDENCE:
            return None  # Don't pollute cache with low-confidence matches

        slug = self._make_slug(canonical_name)
        now = datetime.now().isoformat()

        if slug not in self._data["shows"]:
            self._data["shows"][slug] = {
                "canonical_name": canonical_name,
                "media_type": media_type,
                "api_ids": api_ids,
                "genres": genres or [],
                "country": country or "",
                "raw_variants": [],
                "first_seen": now,
                "last_hit": now,
                "hit_count": 1,
                "confidence_at_match": confidence,
                "expires_at": self._calc_expiry(confidence),
            }
        else:
            existing = self._data["shows"][slug]
            # Update api_ids if new ones provided
            if api_ids:
                existing.setdefault("api_ids", {})
                existing["api_ids"].update(api_ids)
            # Update confidence and refresh TTL if higher confidence
            if confidence > existing.get("confidence_at_match", 0):
                existing["confidence_at_match"] = confidence
                existing["expires_at"] = self._calc_expiry(confidence)
            # Update genres if provided and entry had none
            if genres and not existing.get("genres"):
                existing["genres"] = genres
            # Update country if provided and entry had none
            if country and not existing.get("country"):
                existing["country"] = country
            # Refresh expiry on hit (keeps active shows alive)
            existing["last_hit"] = now
            if self._is_expired(existing):
                existing["expires_at"] = self._calc_expiry(
                    existing.get("confidence_at_match", confidence)
                )

        if season_data:
            self._data["shows"][slug].setdefault("seasons", {})
            self._data["shows"][slug]["seasons"].update(season_data)

        if raw_name:
            self.add_variant(slug, raw_name)

        self.save()
        return slug

    def add_variant(self, slug: str, raw_name: str):
        """Add a raw filename variant to an existing show entry."""
        if slug not in self._data["shows"]:
            return
        variants = self._data["shows"][slug]["raw_variants"]
        normalized = raw_name.strip()
        if normalized and normalized not in variants:
            variants.append(normalized)

    def update_season(self, slug: str, season_num: int, season_info: dict):
        """Add or update season-specific data for anime."""
        if slug not in self._data["shows"]:
            return
        entry = self._data["shows"][slug]
        entry.setdefault("seasons", {})
        entry["seasons"][str(season_num)] = season_info
        self.save()

    def get_show(self, slug: str) -> dict | None:
        """Get a show entry by slug."""
        return self._data["shows"].get(slug)
