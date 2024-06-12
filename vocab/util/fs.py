import os
import re

from vocab.config import cache_path


def get_cached_version(id: str, version: str) -> str | None:
    folder = os.path.join(cache_path, id)
    if os.path.exists(folder):
        for filename in os.listdir(folder):
            if filename.startswith(version):
                return os.path.join(folder, filename)
    return None
