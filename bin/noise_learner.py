#!/usr/bin/env python3
"""
noise_learner.py -- Adaptive noise pattern learning.

After a successful API match, compares the raw filename tokens against the
canonical title to identify release group names and other noise words.
Stores learned patterns for future use in structpilot cleaning.

Safety: Will NOT learn words that appear in protected_words or that are
purely numeric, shorter than 3 characters, or are common title words.
"""

import json
import re
from pathlib import Path
from datetime import datetime


class NoiseLearner:
    def __init__(self, cfg):
        cache_cfg = cfg.get('cache', {})
        self.noise_path = Path(cache_cfg.get('noise_learned_file', 'cache/learned_noise.json'))
        self.noise_path.parent.mkdir(parents=True, exist_ok=True)
        self._data = self._load()
        self._known_patterns = set()  # Active learned patterns
        self._candidate_patterns = {}  # Patterns waiting for threshold
        self._rebuild_lookup()

    def _load(self) -> dict:
        if self.noise_path.exists():
            try:
                data = json.loads(self.noise_path.read_text(encoding='utf-8'))
                # Migrate from v1 if needed
                if data.get("_meta", {}).get("version") == 1:
                    data["_meta"]["version"] = 2
                    data["candidate_patterns"] = {}
                return data
            except Exception:
                pass
        return {
            "_meta": {"version": 2, "last_updated": ""},
            "learned_patterns": [],
            "candidate_patterns": {},
            "protected_words": [
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
                # Anime/show-specific content (expanded protection list)
                "academy", "school", "high", "university", "college",
                "festival", "tournament", "championship", "games",
                "palace", "kingdom", "empire", "city", "town", "village",
                "club", "society", "guild", "organization", "team",
                "shrine", "temple", "church", "monastery",
                "vs", "versus", "vs.",
                "arc", "saga", "chapter"
            ]
        }

    def _rebuild_lookup(self):
        self._known_patterns = {
            p["pattern"].lower() for p in self._data["learned_patterns"]
        }
        self._candidate_patterns = {}
        for p, v in self._data.get("candidate_patterns", {}).items():
            v["shows"] = set(v.get("shows", []))  # JSON stores as list, runtime needs set
            self._candidate_patterns[p.lower()] = v

    def save(self):
        self._data["_meta"]["last_updated"] = datetime.now().isoformat()
        self.noise_path.write_text(
            json.dumps(self._data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )

    def get_all_patterns(self) -> list:
        """Returns compiled regex patterns for use in cleaning."""
        return [p["regex"] for p in self._data["learned_patterns"]]

    def _is_protected(self, word: str) -> bool:
        """Returns True if word should NOT be learned as noise."""
        w = word.lower().strip()
        if len(w) < 3:
            return True
        if w.isdigit():
            return True
        if w in self._data.get("protected_words", []):
            return True
        # Numbers with ordinal suffixes (1st, 2nd, etc)
        if re.match(r'^\d+(st|nd|rd|th)$', w, re.I):
            return True
        # Roman numerals
        if re.match(r'^[ivxlcdm]+$', w, re.I) and len(w) <= 4:
            return True
        return False

    def learn_from_match(self, raw_filename: str, canonical_title: str,
                         original_filename: str = "", show_name: str = ""):
        """
        Compare raw (cleaned) filename against canonical title.
        Any remaining tokens not in the title are potential noise.

        Only activates as actual noise if seen across 3+ DIFFERENT shows.

        Args:
            raw_filename: The cleaned filename (after structpilot cleaning)
            canonical_title: The API-confirmed title
            original_filename: The original unmodified filename (for examples)
            show_name: The canonical show name (for multi-file tracking)
        """
        if not show_name:
            show_name = canonical_title

        # Normalize both
        raw_tokens = set(re.findall(r'[a-zA-Z][a-zA-Z0-9.-]+', raw_filename))
        title_tokens = set(re.findall(r'[a-zA-Z][a-zA-Z0-9.-]+', canonical_title))

        # Find tokens in raw that are NOT in the title
        title_lower = {t.lower() for t in title_tokens}
        noise_candidates = [
            t for t in raw_tokens
            if t.lower() not in title_lower
            and not self._is_protected(t)
        ]

        changed = False
        for token in noise_candidates:
            if self._record_candidate(token, show_name, original_filename or raw_filename):
                changed = True

        if changed:
            self.save()

    def _record_candidate(self, token: str, show_name: str, example: str) -> bool:
        """Record a candidate pattern and check if it should be promoted to active noise.

        Returns True if data changed (candidate recorded or promoted to active).
        """
        token_lower = token.lower()

        # Check if already an active pattern
        if token_lower in self._known_patterns:
            return False  # Already active, don't duplicate

        # Initialize or update candidate
        if token_lower not in self._candidate_patterns:
            self._candidate_patterns[token_lower] = {
                "shows": set([show_name]),
                "hit_count": 1,
                "examples": [example] if example else []
            }
            self._data["candidate_patterns"][token_lower] = {
                "shows": list(set([show_name])),
                "hit_count": 1,
                "examples": [example] if example else []
            }
            changed = True
        else:
            cand = self._candidate_patterns[token_lower]
            cand["shows"].add(show_name)
            cand["hit_count"] = cand.get("hit_count", 0) + 1

            # Update in data dict too
            self._data["candidate_patterns"][token_lower]["shows"] = list(cand["shows"])
            self._data["candidate_patterns"][token_lower]["hit_count"] = cand["hit_count"]

            if example and example not in cand.get("examples", []):
                cand.setdefault("examples", []).append(example)
                cand["examples"] = cand["examples"][-3:]  # Keep last 3
                self._data["candidate_patterns"][token_lower]["examples"] = cand["examples"]

            changed = True

        # Check if should be promoted to active noise (3+ shows)
        shows_count = len(self._candidate_patterns[token_lower]["shows"])
        if shows_count >= 3:
            return self._promote_to_active(token)

        return changed

    def _promote_to_active(self, token: str) -> bool:
        """Promote a candidate pattern to active learned noise when threshold is crossed."""
        token_lower = token.lower()
        if token_lower not in self._candidate_patterns:
            return False

        cand = self._candidate_patterns[token_lower]

        # Move from candidate to learned patterns
        shows_list = list(cand["shows"])
        self._data["learned_patterns"].append({
            "pattern": token,
            "regex": rf"\b{re.escape(token)}\b",
            "source": "auto",
            "first_seen": datetime.now().isoformat(),
            "hit_count": cand.get("hit_count", 1),
            "shows": shows_list,
            "examples": cand.get("examples", [])[-3:]
        })

        # Remove from candidates
        del self._candidate_patterns[token_lower]
        del self._data["candidate_patterns"][token_lower]

        # Update lookup
        self._known_patterns.add(token_lower)

        return True
