from contextlib import contextmanager
from conda.base.constants import (
    CONDA_PACKAGE_EXTENSIONS,
    CONDA_PACKAGE_EXTENSION_V1,
    CONDA_PACKAGE_EXTENSION_V2,
)

from conda.base.constants import PLATFORM_DIRECTORIES as DEFAULT_SUBDIRS

DEFAULT_SUBDIRS = set(DEFAULT_SUBDIRS)


def LoggingContext(*args, **kwargs):
    @contextmanager
    def log_context():
        """no-op logging context"""
        yield

    return log_context()


# import from base definition where practical
from conda_build.utils import (
    try_acquire_locks,
    get_lock,
    move_with_fallback,
    merge_or_update_dict,
    sha256_checksum,  # multithreaded sha+md5 checksum is available
    md5_file,
    ensure_list,
)
