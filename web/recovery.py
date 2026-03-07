#!/usr/bin/env python3
"""
recovery.py -- System recovery tools: flush stuck files, clear caches, nuclear reset.
"""

import shutil
import logging
from pathlib import Path
from datetime import datetime


logger = logging.getLogger('recovery')


class RecoveryManager:
    def __init__(self, config: dict, process_manager):
        self.config = config
        self.pm = process_manager
        self._setup_logger()

    def _setup_logger(self):
        log_dir = Path(self.config.get('logging', {}).get('path', 'logs'))
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_dir / 'recovery.log', encoding='utf-8')
        handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s'))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    def flush_stuck(self, pipeline: str = "both") -> dict:
        """Move files from Processing back to system Drop (re-enter pipeline)."""
        moved = []
        pipelines = self._get_pipelines(pipeline)
        for name, paths in pipelines.items():
            processing = Path(paths['processing'])
            drop = Path(paths['system_drop'])
            if not processing.exists():
                continue
            for item in processing.iterdir():
                if item.name.startswith('.'):
                    continue
                dest = drop / item.name
                try:
                    shutil.move(str(item), str(dest))
                    moved.append({"file": item.name, "pipeline": name})
                    logger.info(f"FLUSH: {item.name} -> {drop}")
                except Exception as e:
                    logger.error(f"FLUSH FAILED: {item.name}: {e}")
        return {"action": "flush_stuck", "moved": moved, "count": len(moved)}

    def retry_failed(self, pipeline: str = "both") -> dict:
        """Move files from Failed back to system Drop."""
        moved = []
        pipelines = self._get_pipelines(pipeline)
        for name, paths in pipelines.items():
            failed = Path(paths['failed'])
            drop = Path(paths['system_drop'])
            if not failed.exists():
                continue
            for item in failed.iterdir():
                if item.name.startswith('.'):
                    continue
                dest = drop / item.name
                try:
                    shutil.move(str(item), str(dest))
                    moved.append({"file": item.name, "pipeline": name})
                    logger.info(f"RETRY FAILED: {item.name} -> {drop}")
                except Exception as e:
                    logger.error(f"RETRY FAILED ERROR: {item.name}: {e}")
        return {"action": "retry_failed", "moved": moved, "count": len(moved)}

    def retry_review(self, pipeline: str = "series") -> dict:
        """Move review files back for re-classification."""
        moved = []
        paths = self.config.get('paths', {})

        if pipeline in ("series", "both"):
            rev = Path(paths.get('series_pipeline', {}).get('review', ''))
            ident = Path(paths.get('series_pipeline', {}).get('staged', {}).get('identify', ''))
            if rev.exists() and ident.exists():
                for item in rev.iterdir():
                    if item.name.startswith('.'):
                        continue
                    try:
                        shutil.move(str(item), str(ident / item.name))
                        moved.append({"file": item.name, "pipeline": "series"})
                        logger.info(f"RETRY REVIEW: {item.name} -> {ident}")
                    except Exception as e:
                        logger.error(f"RETRY REVIEW ERROR: {item.name}: {e}")

        if pipeline in ("movies", "both"):
            rev = Path(paths.get('movie_pipeline', {}).get('review', ''))
            drop = Path(paths.get('movie_pipeline', {}).get('system_drop', ''))
            if rev.exists() and drop.exists():
                for item in rev.iterdir():
                    if item.name.startswith('.'):
                        continue
                    try:
                        shutil.move(str(item), str(drop / item.name))
                        moved.append({"file": item.name, "pipeline": "movies"})
                        logger.info(f"RETRY REVIEW: {item.name} -> {drop}")
                    except Exception as e:
                        logger.error(f"RETRY REVIEW ERROR: {item.name}: {e}")

        return {"action": "retry_review", "moved": moved, "count": len(moved)}

    def nuclear_reset(self) -> dict:
        """Stop everything, backup Work/, recreate empty structure, restart."""
        logger.info("=== NUCLEAR RESET INITIATED ===")

        # 1. Stop all services
        self.pm.stop_all()

        # 2. Backup .Work/
        data_root = Path(self.config['paths']['roots']['data'])
        manager_root = Path(self.config['paths']['roots']['manager'])
        work_root = data_root / ".Work"
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = manager_root / f"recovery_backup_{timestamp}"

        backed_up = False
        if work_root.exists() and any(work_root.rglob('*')):
            try:
                shutil.copytree(str(work_root), str(backup_dir / ".Work"))
                shutil.rmtree(str(work_root))
                backed_up = True
                logger.info(f".Work/ backed up to {backup_dir}")
            except Exception as e:
                logger.error(f"Backup failed: {e}")
                return {"error": str(e)}

        # 3. Recreate infrastructure
        infra_errors = self.pm.init_infrastructure()
        if infra_errors:
            logger.error(f"Infrastructure errors: {infra_errors}")
            return {"error": "Permission issues", "issues": infra_errors}

        # 4. Restart
        self.pm.start_all()
        logger.info("=== NUCLEAR RESET COMPLETE ===")

        return {
            "action": "nuclear_reset",
            "backup_path": str(backup_dir) if backed_up else None,
            "backed_up": backed_up,
        }

    def clear_cache(self, cache_type: str = "all") -> dict:
        """Clear specified caches."""
        cleared = []
        cache_cfg = self.config.get('cache', {})

        if cache_type in ("show_cache", "all"):
            sc = Path(cache_cfg.get('show_cache_file', ''))
            if sc.exists():
                sc.unlink()
                cleared.append("show_cache.json")
                logger.info("Cleared show_cache.json")

        if cache_type in ("noise", "all"):
            nl = Path(cache_cfg.get('noise_learned_file', ''))
            if nl.exists():
                nl.unlink()
                cleared.append("learned_noise.json")
                logger.info("Cleared learned_noise.json")

        if cache_type in ("api", "all"):
            cache_root = Path(cache_cfg.get('root', 'cache'))
            for subdir in ["tv", "anime", "movies", "cartoons", "classifier", "reality", "talkshow", "documentaries"]:
                d = cache_root / subdir
                if d.exists():
                    shutil.rmtree(str(d))
                    d.mkdir()
                    cleared.append(f"cache/{subdir}/")
                    logger.info(f"Cleared cache/{subdir}/")

        return {"action": "clear_cache", "type": cache_type, "cleared": cleared}

    def clean_trash(self) -> dict:
        """Delete all files in Trash folder."""
        trash_root = Path(self.config.get('paths', {}).get('trash_root', 'Trash'))
        deleted = 0
        if trash_root.exists():
            for item in trash_root.rglob('*'):
                if item.is_file():
                    try:
                        item.unlink()
                        deleted += 1
                    except Exception:
                        pass
            # Remove empty subdirectories
            for d in sorted(trash_root.rglob('*'), reverse=True):
                if d.is_dir():
                    try:
                        d.rmdir()
                    except Exception:
                        pass
            logger.info(f"Cleaned trash: {deleted} files deleted")
        return {"action": "clean_trash", "deleted": deleted}

    def clean_review(self) -> dict:
        """Delete all files in Review folders."""
        paths = self.config.get('paths', {})
        deleted = 0
        for pipeline_key in ('series_pipeline', 'movie_pipeline'):
            rev = Path(paths.get(pipeline_key, {}).get('review', ''))
            if rev.exists():
                for item in rev.iterdir():
                    if item.name.startswith('.'):
                        continue
                    try:
                        if item.is_dir():
                            shutil.rmtree(str(item))
                        else:
                            item.unlink()
                        deleted += 1
                    except Exception:
                        pass
        logger.info(f"Cleaned review: {deleted} items deleted")
        return {"action": "clean_review", "deleted": deleted}

    def auto_cleanup(self) -> dict:
        """Scheduled cleanup: delete old trash and review files based on age settings."""
        import time as _time
        tuning = self.config.get('tuning', {})
        trash_max_days = tuning.get('trash_max_age_days', 7)
        rev_max_days = tuning.get('review_max_age_days', 14)
        now = _time.time()
        results = {"trash_deleted": 0, "review_deleted": 0}

        if trash_max_days > 0:
            trash_root = Path(self.config.get('paths', {}).get('trash_root', 'Trash'))
            if trash_root.exists():
                for item in trash_root.rglob('*'):
                    if item.is_file():
                        try:
                            age_days = (now - item.stat().st_mtime) / 86400
                            if age_days > trash_max_days:
                                item.unlink()
                                results["trash_deleted"] += 1
                        except Exception:
                            pass

        if rev_max_days > 0:
            paths = self.config.get('paths', {})
            for pipeline_key in ('series_pipeline', 'movie_pipeline'):
                rev = Path(paths.get(pipeline_key, {}).get('review', ''))
                if rev.exists():
                    for item in rev.iterdir():
                        if item.name.startswith('.'):
                            continue
                        try:
                            age_days = (now - item.stat().st_mtime) / 86400
                            if age_days > rev_max_days:
                                if item.is_dir():
                                    shutil.rmtree(str(item))
                                else:
                                    item.unlink()
                                results["review_deleted"] += 1
                        except Exception:
                            pass

        total = results["trash_deleted"] + results["review_deleted"]
        if total > 0:
            logger.info(f"Auto-cleanup: {results['trash_deleted']} trash, {results['review_deleted']} review deleted")
        return results

    def get_stuck_files(self) -> dict:
        """List files currently in processing/failed/review folders."""
        result = {"processing": [], "failed": [], "review": []}
        paths = self.config.get('paths', {})

        for pipeline_key in ('series_pipeline', 'movie_pipeline'):
            pipeline = paths.get(pipeline_key, {})
            for folder_type in ('processing', 'failed'):
                folder = Path(pipeline.get(folder_type, ''))
                if folder.exists():
                    for item in folder.iterdir():
                        if not item.name.startswith('.'):
                            result[folder_type].append({
                                "name": item.name,
                                "pipeline": pipeline_key.replace('_pipeline', ''),
                                "path": str(item),
                            })
            rev = Path(pipeline.get('review', ''))
            if rev.exists():
                for item in rev.iterdir():
                    if not item.name.startswith('.'):
                        result["review"].append({
                            "name": item.name,
                            "pipeline": pipeline_key.replace('_pipeline', ''),
                            "path": str(item),
                        })
        return result

    def _get_pipelines(self, pipeline: str) -> dict:
        paths = self.config.get('paths', {})
        result = {}
        if pipeline in ("series", "both"):
            result["series"] = paths.get('series_pipeline', {})
        if pipeline in ("movies", "both"):
            result["movies"] = paths.get('movie_pipeline', {})
        return result
