"""
fsspec as an optional dependency.
"""

from __future__ import annotations

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

import os
import os.path
import typing
from dataclasses import dataclass
from pathlib import Path

if typing.TYPE_CHECKING:
    from fsspec import AbstractFileSystem


def get_filesystem(url_or_path):
    if not "://" in url_or_path:
        from fsspec.implementations.local import LocalFileSystem

        # a place to put our doesn't-depend-on-fsspec implementation, unless we
        # decide to switch modes in the class.
        return FsspecFS(LocalFileSystem()), url_or_path
    import fsspec.core

    fs, url = fsspec.core.url_to_fs(url_or_path)
    return FsspecFS(fs), url


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
        try:
            return "/".join(p.rstrip("/") for p in paths)
        except AttributeError:
            pass

    def listdir(self, path: str):
        return [
            listing["name"] for listing in self.fsspec_fs.listdir(path, details=False)
        ]
