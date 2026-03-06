import hashlib
import logging
import os
import time
from pathlib import Path

from src import config

logger = logging.getLogger(__name__)

CACHE_DIR = config.PROJECT_ROOT / "data" / "cache"


class FileCache:
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.ttl = config.CACHE_TTL.get(source_name, 24 * 3600)
        self.source_dir = CACHE_DIR / source_name
        self.source_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, identifier: str) -> str:
        return hashlib.md5(identifier.encode()).hexdigest()[:12]

    def _raw_path(self, identifier: str, ext: str = ".bin") -> Path:
        key = self._cache_key(identifier)
        return self.source_dir / f"raw_{key}{ext}"

    def _parsed_path(self, identifier: str) -> Path:
        key = self._cache_key(identifier)
        return self.source_dir / f"parsed_{key}.csv"

    def _is_fresh(self, path: Path) -> bool:
        if not path.exists():
            return False
        age = time.time() - path.stat().st_mtime
        return age < self.ttl

    def get_raw(self, identifier: str, ext: str = ".bin", force_refresh: bool = False) -> bytes | None:
        path = self._raw_path(identifier, ext)
        if force_refresh or not self._is_fresh(path):
            return None
        logger.debug("%s: Cache hit (raw) for %s", self.source_name, identifier)
        return path.read_bytes()

    def put_raw(self, identifier: str, data: bytes, ext: str = ".bin") -> Path:
        path = self._raw_path(identifier, ext)
        path.write_bytes(data)
        logger.debug("%s: Cached raw data for %s", self.source_name, identifier)
        return path

    def get_parsed(self, identifier: str, force_refresh: bool = False):
        import pandas as pd
        path = self._parsed_path(identifier)
        if force_refresh or not self._is_fresh(path):
            return None
        logger.debug("%s: Cache hit (parsed) for %s", self.source_name, identifier)
        return pd.read_csv(path, parse_dates=["date"])

    def put_parsed(self, identifier: str, df) -> Path:
        path = self._parsed_path(identifier)
        df.to_csv(path, index=False)
        logger.debug("%s: Cached parsed data for %s", self.source_name, identifier)
        return path
