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


# import from base definition where practical
from conda_build.utils import (
    sha256_checksum,  # multithreaded sha+md5 checksum is available
)
from conda_build.utils import (
    ensure_list,
    get_lock,
    md5_file,
    merge_or_update_dict,
    move_with_fallback,
    try_acquire_locks,
)
