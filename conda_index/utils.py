import hashlib
import os
from contextlib import contextmanager

from conda.base.constants import (
    CONDA_PACKAGE_EXTENSION_V1,
    CONDA_PACKAGE_EXTENSION_V2,
    CONDA_PACKAGE_EXTENSIONS,
)
from conda.base.constants import PLATFORM_DIRECTORIES as DEFAULT_SUBDIRS

DEFAULT_SUBDIRS = set(DEFAULT_SUBDIRS)


def LoggingContext(*args, **kwargs):
    @contextmanager
    def log_context():
        """no-op logging context"""
        yield

    return log_context()


# multithreaded checksums
from conda_package_handling.utils import checksums

from .utils_build import (
    ensure_list,
    get_lock,
    merge_or_update_dict,
    move_with_fallback,
    try_acquire_locks,
)


def file_contents_match(pathA, pathB):
    """
    Return True if pathA and pathB have identical contents.
    """
    if os.stat(pathA).st_size != os.stat(pathB).st_size:
        return False

    hashes = []
    for path in (pathA, pathB):
        hashfunc = hashlib.blake2b()
        with open(path, "rb") as data:
            while block := data.read(1 << 18):
                hashfunc.update(block)
        hashes.append(hashfunc.digest())
    return hashes[0] == hashes[1]
