#!/usr/bin/env python3
"""
api_stats.py -- Parse log files to extract meaningful pipeline statistics.

Groups stats by processor type (anime, tv, cartoons, movies, classifier)
with accurate pattern matching based on actual log output.
"""

import json
import re
from pathlib import Path


# Map log filename prefixes to processor labels
PROCESSOR_MAP = {
    "animeproc":          "anime",
    "tvproc":             "tv",
    "cartoonsproc":       "cartoons",
    "movieproc":          "movies",
    "realityproc":        "reality",
    "talkshowproc":       "talkshow",
    "documentariesproc":  "documentaries",
    "contentclassifier":  "classifier",
}

# Per-processor success/fail/cache patterns (matched against actual log output)
PROCESSOR_PATTERNS = {
    "anime": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'No match for|Error processing', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "tv": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Error processing|Moving to Review', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "cartoons": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Error processing|Moving to Review', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "movies": {
        "processed": re.compile(r'MOVED ->', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Loop Error', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "reality": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Error processing|Moving to Review', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "talkshow": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Error processing|Moving to Review', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "documentaries": {
        "processed": re.compile(r'Library Import:', re.IGNORECASE),
        "failed":    re.compile(r'Low confidence|Error processing|Moving to Review', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Processing:', re.IGNORECASE),
    },
    "classifier": {
        "processed": re.compile(r'\[SUCCESS\] Routed:', re.IGNORECASE),
        "failed":    re.compile(r'Review:', re.IGNORECASE),
        "cached":    re.compile(r'Cache hit:', re.IGNORECASE),
        "started":   re.compile(r'Classifying:', re.IGNORECASE),
    },
}

# Classifier routing patterns (to count what went where)
CLASSIFIER_ROUTE_PATTERNS = {
    "to_anime":    re.compile(r'Routed:.*-> Anime', re.IGNORECASE),
    "to_tv":       re.compile(r'Routed:.*-> TV', re.IGNORECASE),
    "to_cartoons":       re.compile(r'Routed:.*-> Cartoons', re.IGNORECASE),
    "to_reality":        re.compile(r'Routed:.*-> Reality', re.IGNORECASE),
    "to_talkshow":       re.compile(r'Routed:.*-> TalkShows', re.IGNORECASE),
    "to_documentaries":  re.compile(r'Routed:.*-> Documentaries', re.IGNORECASE),
}


class APIStats:
    def __init__(self, config: dict):
        self.config = config
        self.log_dir = Path(config.get('logging', {}).get('path', 'logs'))
        self.cache_cfg = config.get('cache', {})

    def get_stats(self) -> dict:
        """Get comprehensive stats from log parsing and cache inspection."""
        processor_stats = self._parse_processor_logs()
        return {
            "processors": processor_stats,
            "overview": self._build_overview(processor_stats),
            "cache": self._get_cache_stats(),
            "library": self._get_library_stats(),
        }

    def _parse_processor_logs(self) -> dict:
        """Parse log files grouped by processor type."""
        stats = {}
        for proc_type in PROCESSOR_PATTERNS:
            stats[proc_type] = {
                "processed": 0, "failed": 0, "cached": 0,
                "started": 0, "sessions": 0,
            }

        # Also track classifier routing
        stats["classifier"]["to_anime"] = 0
        stats["classifier"]["to_tv"] = 0
        stats["classifier"]["to_cartoons"] = 0
        stats["classifier"]["to_reality"] = 0
        stats["classifier"]["to_talkshow"] = 0
        stats["classifier"]["to_documentaries"] = 0

        if not self.log_dir.exists():
            return stats

        for log_file in sorted(self.log_dir.iterdir()):
            if not log_file.name.endswith('.log'):
                continue

            # Determine processor type from filename
            proc_type = None
            for prefix, ptype in PROCESSOR_MAP.items():
                if log_file.name.startswith(prefix):
                    proc_type = ptype
                    break
            if not proc_type:
                continue

            patterns = PROCESSOR_PATTERNS.get(proc_type)
            if not patterns:
                continue

            try:
                text = log_file.read_text(encoding='utf-8', errors='replace')
            except OSError:
                continue

            stats[proc_type]["sessions"] += 1
            for key, pattern in patterns.items():
                stats[proc_type][key] += len(pattern.findall(text))

            # Classifier routing breakdown
            if proc_type == "classifier":
                for route_key, route_pat in CLASSIFIER_ROUTE_PATTERNS.items():
                    stats["classifier"][route_key] += len(route_pat.findall(text))

        return stats

    def _build_overview(self, proc_stats: dict) -> dict:
        """Build top-level overview from processor stats."""
        total_processed = sum(p["processed"] for p in proc_stats.values())
        total_failed = sum(p["failed"] for p in proc_stats.values())
        total_cached = sum(p["cached"] for p in proc_stats.values())
        total_started = sum(p["started"] for p in proc_stats.values())

        # Don't double-count classifier + downstream processors
        # Classifier routes files to processors, so "processed" from both would double-count.
        # Use: classifier.processed = files classified, processor.processed = files imported to library
        library_imports = sum(
            proc_stats[t]["processed"] for t in ["anime", "tv", "cartoons", "movies", "reality", "talkshow", "documentaries"]
        )
        classified = proc_stats["classifier"]["processed"]

        success_rate = 0
        if total_started > 0:
            success_rate = round((total_processed / total_started) * 100, 1)

        cache_rate = 0
        if total_started > 0:
            cache_rate = round((total_cached / total_started) * 100, 1)

        return {
            "library_imports": library_imports,
            "classified": classified,
            "total_failed": total_failed,
            "total_cached": total_cached,
            "success_rate": success_rate,
            "cache_hit_rate": cache_rate,
        }

    def _get_cache_stats(self) -> dict:
        """Get statistics about cached data."""
        stats = {}

        # Show cache
        sc_path = Path(self.cache_cfg.get('show_cache_file', ''))
        if sc_path.exists():
            try:
                data = json.loads(sc_path.read_text(encoding='utf-8'))
                shows = data.get('shows', {})
                total_variants = sum(
                    len(e.get('raw_variants', [])) for e in shows.values()
                )
                total_hits = sum(
                    e.get('hit_count', 0) for e in shows.values()
                )
                stats["show_cache"] = {
                    "entries": len(shows),
                    "variants": total_variants,
                    "total_hits": total_hits,
                    "size_kb": round(sc_path.stat().st_size / 1024, 1),
                }
            except Exception:
                stats["show_cache"] = {"entries": 0, "variants": 0, "total_hits": 0, "size_kb": 0}
        else:
            stats["show_cache"] = {"entries": 0, "variants": 0, "total_hits": 0, "size_kb": 0}

        # API response caches (now includes classifier subdirs)
        cache_root = Path(self.cache_cfg.get('root', 'cache'))
        total_cache_files = 0
        total_cache_kb = 0
        for subdir in ["tv", "anime", "movies", "cartoons", "classifier", "reality", "talkshow", "documentaries"]:
            d = cache_root / subdir
            if d.exists():
                try:
                    files = [f for f in d.rglob('*') if f.is_file()]
                    total_size = sum(f.stat().st_size for f in files)
                    count = len(files)
                    stats[f"api_cache_{subdir}"] = {
                        "files": count,
                        "size_kb": round(total_size / 1024, 1),
                    }
                    total_cache_files += count
                    total_cache_kb += total_size / 1024
                except OSError:
                    stats[f"api_cache_{subdir}"] = {"files": 0, "size_kb": 0}
            else:
                stats[f"api_cache_{subdir}"] = {"files": 0, "size_kb": 0}

        stats["api_cache_total"] = {
            "files": total_cache_files,
            "size_kb": round(total_cache_kb, 1),
        }

        # Noise learner
        nl_path = Path(self.cache_cfg.get('noise_learned_file', ''))
        if nl_path.exists():
            try:
                data = json.loads(nl_path.read_text(encoding='utf-8'))
                stats["noise_learner"] = {
                    "patterns": len(data) if isinstance(data, (dict, list)) else 0,
                    "size_kb": round(nl_path.stat().st_size / 1024, 1),
                }
            except Exception:
                stats["noise_learner"] = {"patterns": 0, "size_kb": 0}
        else:
            stats["noise_learner"] = {"patterns": 0, "size_kb": 0}

        return stats

    def _get_library_stats(self) -> dict:
        """Get show/movie counts from the show cache, grouped by media type."""
        sc_path = Path(self.cache_cfg.get('show_cache_file', ''))
        by_type = {"anime": 0, "tv": 0, "cartoons": 0, "movie": 0, "reality": 0, "talkshow": 0, "documentaries": 0}
        top_shows = []

        if sc_path.exists():
            try:
                data = json.loads(sc_path.read_text(encoding='utf-8'))
                shows = data.get('shows', {})
                for entry in shows.values():
                    mt = entry.get('media_type', 'unknown')
                    if mt in by_type:
                        by_type[mt] += 1

                # Top shows by hit count
                sorted_shows = sorted(
                    shows.values(),
                    key=lambda x: x.get('hit_count', 0),
                    reverse=True
                )
                for s in sorted_shows[:10]:
                    top_shows.append({
                        "name": s.get("canonical_name", "?"),
                        "type": s.get("media_type", "?"),
                        "hits": s.get("hit_count", 0),
                        "seasons": len(s.get("seasons", {})),
                    })
            except Exception:
                pass

        return {
            "by_type": by_type,
            "total": sum(by_type.values()),
            "top_shows": top_shows,
        }

    def get_cache_details(self) -> dict:
        """Get detailed cache entries for browsing in the UI."""
        result = {"show_cache": [], "api_caches": {}}

        # Show cache entries
        sc_path = Path(self.cache_cfg.get('show_cache_file', ''))
        if sc_path.exists():
            try:
                data = json.loads(sc_path.read_text(encoding='utf-8'))
                shows = data.get('shows', {})
                for slug, entry in shows.items():
                    result["show_cache"].append({
                        "slug": slug,
                        "name": entry.get("canonical_name", slug),
                        "type": entry.get("media_type", "unknown"),
                        "variants": entry.get("raw_variants", []),
                        "hits": entry.get("hit_count", 0),
                        "confidence": entry.get("confidence_at_match", 0),
                        "seasons": len(entry.get("seasons", {})),
                        "api_ids": entry.get("api_ids", {}),
                        "first_seen": entry.get("first_seen", ""),
                        "last_hit": entry.get("last_hit", ""),
                    })
                result["show_cache"].sort(key=lambda x: x["name"].lower())
            except Exception:
                pass

        # API cache per-source breakdown
        cache_root = Path(self.cache_cfg.get('root', 'cache'))
        for subdir in cache_root.iterdir() if cache_root.exists() else []:
            if subdir.is_dir() and not subdir.name.startswith('.'):
                files = list(subdir.rglob('*'))
                real_files = [f for f in files if f.is_file()]
                total_size = sum(f.stat().st_size for f in real_files) if real_files else 0
                # Check for consolidated cache
                consolidated = subdir / "_consolidated.json"
                entries = 0
                if consolidated.exists():
                    try:
                        cdata = json.loads(consolidated.read_text(encoding='utf-8'))
                        entries = len(cdata)
                    except Exception:
                        pass
                result["api_caches"][subdir.name] = {
                    "files": len(real_files),
                    "entries": entries,
                    "size_kb": round(total_size / 1024, 1),
                }

        return result
