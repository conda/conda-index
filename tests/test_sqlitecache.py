"""
Test sqlitecache
"""

from pathlib import Path

from conda_index.index.common import connect
from conda_index.index.convert_cache import (
    convert_cache,
    create,
    extract_cache_filesystem,
    merge_index_cache,
    migrate,
)
from conda_index.index.sqlitecache import CondaIndexCache
from conda_index.utils import DEFAULT_SUBDIRS


def test_cache_extract_without_stat_result(index_data):
    """
    Exercise CondaIndexCache.
    """
    pkg_dir = Path(index_data, "packages")

    CondaIndexCache.db # run a getter decorator

    cache = CondaIndexCache(pkg_dir, "noarch")

    # this function is normally called with a cached stat_result, but for legacy
    # reasons it can check stat() itself.
    cache._extract_to_cache(
        cache.channel_root,
        cache.subdir,
        pkg_dir / "noarch" / "run_exports_versions-2.0-h39de5ba_0.tar.bz2",
        stat_result=None,
    )


def test_convert_legacy_cache(tmp_path):
    """
    conda-index will automatically convert a many-small-files cache to a
    database, without re-extracting packages.
    """
    legacy_cache = Path(__file__).parent / "index_data" / "legacy_cache" / ".cache"
    new_database = tmp_path / "converted.db"
    conn = connect(new_database)
    with conn:
        create(conn)
        migrate(conn)
    convert_cache(conn, extract_cache_filesystem(legacy_cache))
    assert conn.execute("SELECT COUNT(*) FROM index_json").fetchone()[0] == 2


def test_merge_index_cache(tmp_path):
    """
    Merge multiple caches into one for data mining. Not used by normal index
    process.
    """
    for subdir in DEFAULT_SUBDIRS:
        db_path = tmp_path / subdir / ".cache" / "cache.db"
        db_path.parent.mkdir(parents=True)
        if "-32" in subdir:
            # exclude a couple to improve code coverage
            continue

        with connect(db_path) as conn:
            create(conn)
            # / here triggers code coverage in migrate function
            conn.execute(
                f"INSERT INTO index_json (path, index_json) VALUES ('prefix/{subdir}.conda', '{{}}')"
            )
            migrate(conn)

    merge_index_cache(tmp_path)

    seen_subdirs = set()
    with connect("merged.db") as conn:
        for row in conn.execute("SELECT path FROM index_json"):
            channel, subdir, _ = row[0].split("/")
            assert channel == tmp_path.name
            seen_subdirs.add(subdir)
