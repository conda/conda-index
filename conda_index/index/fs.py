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

# urllib.request.pathname2url(pathname) may be useful

def get_filesystem(url_or_path):
    if not "://" in url_or_path:
        from fsspec.implementations.local import LocalFileSystem

        return (LocalFileSystem(), url_or_path)
    import fsspec.core

    return fsspec.core.url_to_fs("file:///")
