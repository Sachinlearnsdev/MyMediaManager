#!/usr/bin/env python3
"""
noise_learner.py -- Adaptive noise pattern learning (v3).

After a successful API match, compares the raw filename tokens against the
canonical title to identify release group names and other noise words.
Stores learned patterns per category (video/music/books) for future use
in structpilot cleaning.

Safety mechanisms:
  - Protected words list (articles, content words, Roman numerals, etc.)
  - Title-word tracking: any word confirmed as part of a real title gets
    permanent protection across all categories.
  - 3+ different shows/artists threshold before promotion.
  - Position-aware application: structpilot only strips trailing tokens.
  - User approval workflow: promoted patterns start as "pending" and can
    be approved or rejected with a reason from the web panel.
  - Master toggle: learning always runs, but application to filename
    cleaning is off by default and user-controlled.
"""

import json
import re
from pathlib import Path
from datetime import datetime, timezone

CATEGORIES = ("video", "music", "books")
PROMOTION_THRESHOLD = 3  # Number of different shows/artists before promotion

PROTECTED_WORDS = [
    # Articles and prepositions
    "the", "a", "an", "of", "and", "in", "to", "for", "is",
    "on", "at", "by", "or", "no", "vs",
    # Generic content words
    "part", "season", "episode", "movie", "film", "special",
    # Roman numerals
    "ii", "iii", "iv", "vi", "vii", "viii", "ix", "xi",
    # Ordinals
    "2nd", "3rd", "4th", "5th", "6th", "7th", "8th", "9th", "10th",
    # OVA/ONA/OAD markers
    "ova", "oad", "ona",
    # Anime/show-specific content
    "academy", "school", "high", "university", "college",
    "festival", "tournament", "championship", "games",
    "palace", "kingdom", "empire", "city", "town", "village",
    "club", "society", "guild", "organization", "team",
    "shrine", "temple", "church", "monastery",
    "versus", "vs.",
    "arc", "saga", "chapter",
]


def _empty_v3() -> dict:
    """Return a fresh v3 data structure."""
    return {
        "_meta": {"version": 3, "last_updated": "", "apply_enabled": False},
        "categories": {
            cat: {"learned_patterns": [], "candidate_patterns": {}}
            for cat in CATEGORIES
        },
        "title_words": {},
        "protected_words": list(PROTECTED_WORDS),
    }


class NoiseLearner:
    def __init__(self, cfg):
        cache_cfg = cfg.get('cache', {})
        self.noise_path = Path(cache_cfg.get('noise_learned_file', 'cache/learned_noise.json'))
        self.noise_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()
        # Runtime lookups per category
        self._known = {}      # {category: set(lowercase patterns)}
        self._candidates = {} # {category: {token: {shows: set, ...}}}
        self._rebuild_lookup()

    # ── Load / Save / Migration ──────────────────────────────────────

    def _load(self) -> dict:
        if self.noise_path.exists():
            try:
                data = json.loads(self.noise_path.read_text(encoding='utf-8'))
                version = data.get("_meta", {}).get("version", 1)
                if version < 3:
                    data = self._migrate_to_v3(data)
                return data
            except Exception:
                pass
        return _empty_v3()

    def _migrate_to_v3(self, old: dict) -> dict:
        """Migrate v1/v2 flat structure into v3 category-isolated structure."""
        new = _empty_v3()
        # Preserve protected_words if customized
        if old.get("protected_words"):
            new["protected_words"] = old["protected_words"]
        # Move all existing patterns into video category
        video = new["categories"]["video"]
        for p in old.get("learned_patterns", []):
            p.setdefault("status", "pending")
            p.setdefault("approved_at", None)
            p.setdefault("rejected_reason", None)
            p.setdefault("source", "auto")
            video["learned_patterns"].append(p)
        for token, cand in old.get("candidate_patterns", {}).items():
            if isinstance(cand.get("shows"), set):
                cand["shows"] = list(cand["shows"])
            video["candidate_patterns"][token] = cand
        return new

    def _rebuild_lookup(self):
        """Build runtime lookup dicts from persisted data."""
        self._known = {}
        self._candidates = {}
        for cat in CATEGORIES:
            cat_data = self._data["categories"].get(cat, {})
            self._known[cat] = {
                p["pattern"].lower() for p in cat_data.get("learned_patterns", [])
            }
            self._candidates[cat] = {}
            for token, v in cat_data.get("candidate_patterns", {}).items():
                v["shows"] = set(v.get("shows", []))
                self._candidates[cat][token.lower()] = v

    def save(self):
        """Persist data to disk. Converts sets to lists for JSON."""
        self._data["_meta"]["last_updated"] = datetime.now(timezone.utc).isoformat()
        # Ensure candidate shows are lists for JSON serialization
        for cat in CATEGORIES:
            cat_data = self._data["categories"].get(cat, {})
            for token, cand in cat_data.get("candidate_patterns", {}).items():
                if isinstance(cand.get("shows"), set):
                    cand["shows"] = list(cand["shows"])
        self.noise_path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )

    # ── Toggle ───────────────────────────────────────────────────────

    def is_apply_enabled(self) -> bool:
        return self._data["_meta"].get("apply_enabled", False)

    def set_apply_enabled(self, enabled: bool):
        self._data["_meta"]["apply_enabled"] = bool(enabled)
        self.save()

    # ── Protection ───────────────────────────────────────────────────

    def _is_protected(self, word: str) -> bool:
        """Returns True if word should NOT be learned as noise."""
        w = word.lower().strip()
        if len(w) < 3:
            return True
        if w.isdigit():
            return True
        if w in self._data.get("protected_words", []):
            return True
        # Title words are permanently protected
        if w in self._data.get("title_words", {}):
            return True
        # Ordinal suffixes (1st, 2nd, etc.)
        if re.match(r'^\d+(st|nd|rd|th)$', w, re.I):
            return True
        # Roman numerals
        if re.match(r'^[ivxlcdm]+$', w, re.I) and len(w) <= 4:
            return True
        return False

    def _record_title_words(self, canonical_title: str, show_name: str):
        """Record every word in the canonical title for permanent protection."""
        title_words = self._data.setdefault("title_words", {})
        tokens = re.findall(r'[a-zA-Z][a-zA-Z0-9.-]+', canonical_title)
        for t in tokens:
            w = t.lower()
            if len(w) < 3:
                continue
            shows = title_words.setdefault(w, [])
            if show_name and show_name not in shows:
                shows.append(show_name)
                # Cap at 10 examples per word
                title_words[w] = shows[-10:]

    # ── Learning (always active) ─────────────────────────────────────

    def learn_from_match(self, raw_filename: str, canonical_title: str,
                         original_filename: str = "", show_name: str = "",
                         category: str = "video"):
        """
        Compare raw filename tokens against canonical title.
        Tokens in raw but NOT in title are noise candidates.
        Always learns regardless of apply_enabled toggle.

        Args:
            raw_filename: The cleaned filename (after structpilot cleaning)
            canonical_title: The API-confirmed title
            original_filename: The original unmodified filename (for examples)
            show_name: The canonical show/artist name (for multi-file tracking)
            category: "video", "music", or "books"
        """
        if category not in CATEGORIES:
            category = "video"
        if not show_name:
            show_name = canonical_title

        # Record title words for permanent protection
        self._record_title_words(canonical_title, show_name)

        # Tokenize both
        raw_tokens = set(re.findall(r'[a-zA-Z][a-zA-Z0-9.-]+', raw_filename))
        title_tokens = set(re.findall(r'[a-zA-Z][a-zA-Z0-9.-]+', canonical_title))
        title_lower = {t.lower() for t in title_tokens}

        # Find noise candidates
        noise_candidates = [
            t for t in raw_tokens
            if t.lower() not in title_lower
            and not self._is_protected(t)
        ]

        changed = False
        for token in noise_candidates:
            if self._record_candidate(token, show_name,
                                      original_filename or raw_filename, category):
                changed = True

        if changed:
            self.save()

    def _record_candidate(self, token: str, show_name: str,
                          example: str, category: str) -> bool:
        """Record a candidate and check promotion threshold."""
        token_lower = token.lower()

        if token_lower in self._known.get(category, set()):
            # Already active — update hit count
            cat_data = self._data["categories"][category]
            for p in cat_data["learned_patterns"]:
                if p["pattern"].lower() == token_lower:
                    p["hit_count"] = p.get("hit_count", 0) + 1
                    if show_name not in p.get("shows", []):
                        p["shows"].append(show_name)
                    break
            return True

        candidates = self._candidates.setdefault(category, {})
        cat_data = self._data["categories"].setdefault(category, {
            "learned_patterns": [], "candidate_patterns": {}
        })
        data_cands = cat_data.setdefault("candidate_patterns", {})

        if token_lower not in candidates:
            candidates[token_lower] = {
                "shows": {show_name},
                "hit_count": 1,
                "examples": [example] if example else []
            }
            data_cands[token_lower] = {
                "shows": [show_name],
                "hit_count": 1,
                "examples": [example] if example else []
            }
        else:
            cand = candidates[token_lower]
            cand["shows"].add(show_name)
            cand["hit_count"] = cand.get("hit_count", 0) + 1
            if example and example not in cand.get("examples", []):
                cand.setdefault("examples", []).append(example)
                cand["examples"] = cand["examples"][-3:]

            data_cands[token_lower] = {
                "shows": list(cand["shows"]),
                "hit_count": cand["hit_count"],
                "examples": cand.get("examples", [])
            }

        # Check promotion threshold
        if len(candidates[token_lower]["shows"]) >= PROMOTION_THRESHOLD:
            return self._promote_to_active(token, category)

        return True

    def _promote_to_active(self, token: str, category: str) -> bool:
        """Promote candidate to active learned pattern with status='pending'."""
        token_lower = token.lower()
        candidates = self._candidates.get(category, {})
        if token_lower not in candidates:
            return False

        cand = candidates[token_lower]
        now = datetime.now(timezone.utc).isoformat()

        cat_data = self._data["categories"][category]
        cat_data["learned_patterns"].append({
            "pattern": token,
            "regex": rf"\b{re.escape(token)}\b",
            "source": "auto",
            "status": "pending",
            "first_seen": now,
            "hit_count": cand.get("hit_count", 1),
            "shows": list(cand["shows"]),
            "examples": cand.get("examples", [])[-3:],
            "approved_at": None,
            "rejected_at": None,
            "rejection_reason": None,
        })

        # Remove from candidates
        del candidates[token_lower]
        cat_data.get("candidate_patterns", {}).pop(token_lower, None)

        self._known.setdefault(category, set()).add(token_lower)
        return True

    # ── Pattern Retrieval ────────────────────────────────────────────

    def get_active_patterns(self, category: str = "video") -> list:
        """Returns regex strings for patterns that should be applied.
        Only returns non-rejected patterns when apply_enabled is True."""
        if not self.is_apply_enabled():
            return []
        cat_data = self._data["categories"].get(category, {})
        return [
            p["regex"] for p in cat_data.get("learned_patterns", [])
            if p.get("status") != "rejected"
        ]

    # ── User Management API (web panel) ──────────────────────────────

    def approve_pattern(self, pattern: str, category: str) -> bool:
        cat_data = self._data["categories"].get(category, {})
        for p in cat_data.get("learned_patterns", []):
            if p["pattern"].lower() == pattern.lower():
                p["status"] = "approved"
                p["approved_at"] = datetime.now(timezone.utc).isoformat()
                p["rejected_at"] = None
                p["rejection_reason"] = None
                self.save()
                return True
        return False

    def reject_pattern(self, pattern: str, category: str, reason: str = "") -> bool:
        cat_data = self._data["categories"].get(category, {})
        for p in cat_data.get("learned_patterns", []):
            if p["pattern"].lower() == pattern.lower():
                p["status"] = "rejected"
                p["rejected_at"] = datetime.now(timezone.utc).isoformat()
                p["rejection_reason"] = reason
                self.save()
                return True
        return False

    def add_manual_pattern(self, pattern: str, category: str) -> bool:
        """Add a user-defined noise pattern (auto-approved)."""
        if category not in CATEGORIES:
            return False
        pattern = pattern.strip()
        if not pattern or len(pattern) < 2:
            return False

        cat_data = self._data["categories"][category]
        # Check for duplicate
        for p in cat_data["learned_patterns"]:
            if p["pattern"].lower() == pattern.lower():
                return False

        now = datetime.now(timezone.utc).isoformat()
        cat_data["learned_patterns"].append({
            "pattern": pattern,
            "regex": rf"\b{re.escape(pattern)}\b",
            "source": "manual",
            "status": "approved",
            "first_seen": now,
            "hit_count": 0,
            "shows": [],
            "examples": [],
            "approved_at": now,
            "rejected_at": None,
            "rejection_reason": None,
        })
        self._known.setdefault(category, set()).add(pattern.lower())
        self.save()
        return True

    def delete_pattern(self, pattern: str, category: str) -> bool:
        """Remove a pattern entirely (learned or candidate)."""
        cat_data = self._data["categories"].get(category, {})

        # Check learned patterns
        patterns = cat_data.get("learned_patterns", [])
        for i, p in enumerate(patterns):
            if p["pattern"].lower() == pattern.lower():
                patterns.pop(i)
                self._known.get(category, set()).discard(pattern.lower())
                self.save()
                return True

        # Check candidates
        cands = cat_data.get("candidate_patterns", {})
        token_lower = pattern.lower()
        if token_lower in cands:
            del cands[token_lower]
            self._candidates.get(category, {}).pop(token_lower, None)
            self.save()
            return True

        return False

    def get_all_data(self) -> dict:
        """Return full data for web panel display."""
        # Deep copy to avoid mutation; convert any leftover sets
        result = json.loads(json.dumps(self._data, default=list))
        return result

    def get_stats(self) -> dict:
        """Aggregate statistics per category."""
        stats = {"total": 0, "pending": 0, "approved": 0, "rejected": 0, "candidates": 0}
        per_category = {}
        for cat in CATEGORIES:
            cat_data = self._data["categories"].get(cat, {})
            learned = cat_data.get("learned_patterns", [])
            cands = cat_data.get("candidate_patterns", {})
            cs = {
                "total": len(learned),
                "pending": sum(1 for p in learned if p.get("status") == "pending"),
                "approved": sum(1 for p in learned if p.get("status") == "approved"),
                "rejected": sum(1 for p in learned if p.get("status") == "rejected"),
                "candidates": len(cands),
            }
            per_category[cat] = cs
            for k in stats:
                stats[k] += cs[k]
        stats["per_category"] = per_category
        stats["apply_enabled"] = self.is_apply_enabled()
        stats["title_words_count"] = len(self._data.get("title_words", {}))
        return stats
