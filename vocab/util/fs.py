import os

from vocab.config import root_path, cache_rel_path


def get_cached_version(id: str, version: str) -> str | None:
    folder = str(os.path.join(root_path, cache_rel_path, id))
    if os.path.exists(folder):
        for filename in os.listdir(folder):
            if filename.startswith(version):
                return os.path.join(folder, filename)
    return None
