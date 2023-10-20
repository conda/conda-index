"""
fsspec as an optional dependency.
"""

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

import os, os.path
from pathlib import Path


def get_filesystem(url_or_path):
    if not "://" in url_or_path:
        from fsspec.implementations.local import LocalFileSystem

        # a place to put our doesn't-depend-on-fsspec implementation, unless we
        # decide to switch modes in the class.
        return (LocalFileSystem(), url_or_path)
    import fsspec.core

    return fsspec.core.url_to_fs(url_or_path)


class MinimalFS:
    """
    Filesystem API as needed by conda-index, for fsspec compatibility.
    """

    def open(self, path: str | Path, mode: str = "r"):
        return Path(path).open(mode)

    def stat(self, path: str | Path):
        return os.stat(path)

    def join(self, *paths):
        return os.path.join(*paths)
