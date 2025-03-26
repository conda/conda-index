import filecmp
import hashlib
from concurrent.futures.thread import ThreadPoolExecutor

from conda.base.constants import (  # noqa: F401
    CONDA_PACKAGE_EXTENSION_V1,
    CONDA_PACKAGE_EXTENSION_V2,
    CONDA_PACKAGE_EXTENSIONS,
)
from conda.base.constants import PLATFORM_DIRECTORIES as DEFAULT_SUBDIRS

DEFAULT_SUBDIRS = set(DEFAULT_SUBDIRS)


# multithreaded checksums


def _checksum(fd, algorithm, buffersize=65536):
    hash_impl = getattr(hashlib, algorithm)()
    for block in iter(lambda: fd.read(buffersize), b""):
        hash_impl.update(block)
    return hash_impl.hexdigest()


def checksum(fn, algorithm, buffersize=1 << 18):
    """
    Calculate a checksum for a filename (not an open file).
    """
    with open(fn, "rb") as fd:
        return _checksum(fd, algorithm, buffersize)


def checksums(fn, algorithms, buffersize=1 << 18):
    """
    Calculate multiple checksums for a filename in parallel.
    """
    with ThreadPoolExecutor(max_workers=len(algorithms)) as e:
        # take care not to share hash_impl between threads
        results = [
            e.submit(checksum, fn, algorithm, buffersize) for algorithm in algorithms
        ]
    return [result.result() for result in results]


from .utils_build import (  # noqa: F401
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

    return filecmp.cmp(pathA, pathB, shallow=False)


def human_bytes(n):
    """
    Return the number of bytes n in more human readable form.

    Examples:
        >>> human_bytes(42)
        '42 B'
        >>> human_bytes(1042)
        '1 KB'
        >>> human_bytes(10004242)
        '9.5 MB'
        >>> human_bytes(100000004242)
        '93.13 GB'
    """
    if n < 1024:
        return "%d B" % n
    k = n / 1024
    if k < 1024:
        return "%d KB" % round(k)
    m = k / 1024
    if m < 1024:
        return f"{m:.1f} MB"
    g = m / 1024
    return f"{g:.2f} GB"
