#!/usr/bin/env python3
"""
noise_manager.py -- Web API wrapper for the noise learner.
Provides methods for the web panel to view, approve, reject,
and manage learned noise patterns.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bin.noise_learner import NoiseLearner


class NoiseManager:
    def __init__(self, config: dict):
        self.learner = NoiseLearner(config)

    def get_overview(self) -> dict:
        """Return all data + stats for the web panel."""
        data = self.learner.get_all_data()
        stats = self.learner.get_stats()
        return {"data": data, "stats": stats}

    def toggle_apply(self, enabled: bool) -> dict:
        self.learner.set_apply_enabled(enabled)
        return {"apply_enabled": self.learner.is_apply_enabled()}

    def approve(self, pattern: str, category: str) -> dict:
        ok = self.learner.approve_pattern(pattern, category)
        return {"success": ok, "pattern": pattern, "category": category}

    def reject(self, pattern: str, category: str, reason: str = "") -> dict:
        ok = self.learner.reject_pattern(pattern, category, reason)
        return {"success": ok, "pattern": pattern, "category": category}

    def add_manual(self, pattern: str, category: str) -> dict:
        ok = self.learner.add_manual_pattern(pattern, category)
        return {"success": ok, "pattern": pattern, "category": category}

    def delete(self, pattern: str, category: str) -> dict:
        ok = self.learner.delete_pattern(pattern, category)
        return {"success": ok, "pattern": pattern, "category": category}

    def get_stats(self) -> dict:
        return self.learner.get_stats()
