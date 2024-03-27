"""
Minimal (just what conda-index uses) filesystem abstraction.

Allows `fsspec <https://filesystem-spec.readthedocs.io/>`_ to be used to index
remote repositories, without making it a required dependency.
"""

from __future__ import annotations

import os
import os.path
import typing
from dataclasses import dataclass
from numbers import Number
from pathlib import Path

if typing.TYPE_CHECKING:
    from fsspec import AbstractFileSystem

# Maybe use this
"""The DirFileSystem is a filesystem-wrapper. It assumes every path it is
dealing with is relative to the path. After performing the necessary paths
operation it delegates everything to the wrapped filesystem."""

# use fsspec.core.url_to_fs("maybe::chained::http:///...")
# returns (implementation, more_normal_url)
# use urljoin or urlsplit or plain split('/')...
# .. works for relative urls: urlparse.parse.urljoin("file:///spam/eggs/woo", "../bla") returns file:///spam/bla

# In [7]: fs.unstrip_protocol('foo/bar')
# Out[7]: 'file:///base/path/conda-index/foo/bar'

# In [9]: fsspec.core.url_to_fs("/spam/eggs")
# Out[9]: (<fsspec.implementations.local.LocalFileSystem at 0x102a6cf40>, '/spam/eggs')

# See also https://conda.anaconda.org/<repo name>/noarch/ e.g. for smaller
# channels; https://conda.anaconda.org/<repo name>/ parent will redirect
# elsewhere

# Note fsspec uses / as a path separator on all platforms


@dataclass
class FileInfo:
    """
    Filename and a bit of stat information.
    """

    fn: str
    st_mtime: Number
    st_size: Number


class MinimalFS:
    """
    Filesystem API as needed by conda-index, for fsspec compatibility.
    """

    def open(self, path: str, mode: str = "rb"):
        return Path(path).open(mode)

    def stat(self, path: str):
        st_result = os.stat(path)
        return {
            "size": st_result.st_size,
            "mtime": st_result.st_mtime,
        }

    def join(self, *paths):
        return os.path.join(*paths)

    def listdir(self, path):
        # XXX change pathsep to / to mimic fsspec
        return os.listdir(path)


class FsspecFS(MinimalFS):
    fsspec_fs: AbstractFileSystem

    def __init__(self, fsspec_fs):
        self.fsspec_fs = fsspec_fs

    def open(self, path: str, mode: str = "rb"):
        return self.fsspec_fs.open(path, mode)

    def stat(self, path: str):
        return self.fsspec_fs.stat(path)

    def join(self, *paths):
        # XXX
        return "/".join(p.rstrip("/") for p in paths)

    def listdir(self, path: str):
        return [
            listing["name"] for listing in self.fsspec_fs.listdir(path, details=False)
        ]
