"""
Test sqlitecache
"""

from pathlib import Path
from conda_index.index.sqlitecache import CondaIndexCache


def test_cache_extract_without_stat_result(index_data):
    """
    Exercise CondaIndexCache.
    """
    pkg_dir = Path(index_data, "packages")

    cache = CondaIndexCache(pkg_dir, "noarch")

    # this function is normally called with a cached stat_result, but for legacy
    # reasons it can check stat() itself.
    cache._extract_to_cache(
        cache.channel_root,
        cache.subdir,
        pkg_dir / "noarch" / "run_exports_versions-2.0-h39de5ba_0.tar.bz2",
        stat_result=None
    )
