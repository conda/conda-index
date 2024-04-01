"""
Test sqlitecache
"""

import json
import sqlite3
import tarfile
from io import BytesIO
from pathlib import Path

import pytest

from conda_index.index.common import connect
from conda_index.index.convert_cache import (
    convert_cache,
    create,
    extract_cache_filesystem,
    merge_index_cache,
    migrate,
)
from conda_index.index.sqlitecache import (
    CondaIndexCache,
    _cache_post_install_details,
    _cache_recipe,
    _clear_newline_chars,
)
from conda_index.utils import DEFAULT_SUBDIRS


def test_cache_extract_without_stat_result(index_data):
    """
    Exercise CondaIndexCache.
    """
    pkg_dir = Path(index_data, "packages")

    CondaIndexCache.db  # run a getter decorator

    cache = CondaIndexCache(pkg_dir, "noarch")

    # this function is normally called with a cached stat_result, but for legacy
    # reasons it can check stat() itself.
    cache._extract_to_cache(
        cache.channel_root,
        cache.subdir,
        pkg_dir / "noarch" / "run_exports_versions-2.0-h39de5ba_0.tar.bz2",
        stat_result=None,
    )


def test_cache_unusual_files(tmp_path):
    """
    Cover error when metadata happens to be a device file; cover cache mtime
    fallback; cover missing paths metadata.
    """

    (tmp_path / "noarch").mkdir()
    tar = tmp_path / "noarch" / "devicefile.tar.bz2"

    with tarfile.open(tar, mode="w:bz2") as t:
        # icon file, though empty, to trigger cache-icon code. Before index.json
        # which doesn't mention icons.
        icon = tarfile.TarInfo(name="info/icon.png")
        t.addfile(icon)
        # index.json required to finish cache function
        index = tarfile.TarInfo(name="info/index.json")
        index.size = 2
        t.addfile(index, BytesIO(b"{}"))
        # device file named after metadata, to trigger an error handler
        tarinfo = tarfile.TarInfo(name="info/paths.json")
        tarinfo.type = tarfile.CHRTYPE
        t.addfile(tarinfo)
        # will be checked by sqlite parser only
        about = tarfile.TarInfo(name="info/about.json")
        about.size = 8
        t.addfile(about, BytesIO(b"not json"))

    cache = CondaIndexCache(tmp_path, "noarch")

    with pytest.raises(sqlite3.OperationalError):
        # XXX a malformed about.json might halt index processing?
        cache._extract_to_cache(cache.channel_root, cache.subdir, tar.name)

    (tmp_path / "noarch" / "found").touch()

    # coverage for fallback "try to stat file if not in cache"
    found = cache.load_all_from_cache("found")
    assert found["mtime"] > 0
    assert cache.load_all_from_cache("notfound") == {}


def test_cache_source_as_list(tmp_path):
    """
    Cover fallback when source is a list and not a dict.
    """

    (tmp_path / "noarch").mkdir()
    tar = tmp_path / "noarch" / "source-as-list.tar.bz2"

    with tarfile.open(tar, mode="w:bz2") as t:
        # index.json required to finish cache function
        index = tarfile.TarInfo(name="info/index.json")
        index_data = b'{"source":["a", "b"]}'
        index.size = len(index_data)
        t.addfile(index, BytesIO(index_data))
    cache = CondaIndexCache(tmp_path, "noarch")

    # argument is unused now but was required previously
    cache.save_fs_state(tmp_path / "noarch")
    cache._extract_to_cache(cache.channel_root, cache.subdir, tar.name)

    # test load_all_from_cache still works without local file, mtime saved in
    # save_fs_state()
    tar.unlink()

    # may or may not a correct "source" but it preserves the input
    found = cache.load_all_from_cache(tar.name)
    assert found["source"] == ["a", "b"]
    assert found["mtime"] > 0


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


def test_description_as_list():
    """
    Rarely, the description could be a list. Or theoretically something that
    can't be replaced into a string at all.
    """
    record = {"description": ["a", "list\n", "description"], "number": 0}
    _clear_newline_chars(record, "description")
    assert record["description"] == "alist description"
    _clear_newline_chars(record, "number")  # coverage


def test_cache_recipe_build_not_json_serializable():
    """
    Odd feature that drops build portion when recipe is not json serializable.
    """
    yaml_not_json = b"""
requirements:
    build:
        - 2024-04-01
"""
    strange_recipe = BytesIO(yaml_not_json)
    altered = _cache_recipe(strange_recipe)
    assert altered == '{"requirements": {}}'


def test_cache_post_install_details():
    details = {
        "paths": [
            {"_path": "a", "prefix_placeholder": "x", "file_mode": "binary"},
            {"_path": "b", "file_mode": "text"},
            {"_path": "etc/conda/activate.d", "file_mode": "text"},
        ]
    }
    _cache_post_install_details(json.dumps(details))
