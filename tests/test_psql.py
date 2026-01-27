"""
Test ability to index off diverse filesystems using fsspec.
"""

from pathlib import Path

import pytest

from conda_index.index import ChannelIndex

try:
    from sqlalchemy import select

    from conda_index.postgres import model
    from conda_index.postgres.cache import PsqlCache
except ImportError:
    PsqlCache = None
    model = None
    select = None


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql(tmp_path: Path, index_data: Path, postgresql_database):
    """
    Test that conda-index can store its cache in postgresql.
    """
    channel_root = index_data / "packages"
    output = tmp_path / "output"  # default same as channel
    output.mkdir()

    test_subdirs = ("noarch", "osx-64")

    assert PsqlCache

    channel_index = ChannelIndex(
        channel_root,
        channel_name="psql",
        output_root=output,
        subdirs=test_subdirs,  # listdir('tests/index_data/packages')?
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=False,
        compact_json=True,
        cache_class=PsqlCache,
        cache_kwargs={"db_url": postgresql_database.url},
        write_shards=True,
        write_monolithic=True,
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )

    print("Done")


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_store_fs_state_update_only_true(tmp_path: Path, postgresql_database):
    assert PsqlCache
    cache = PsqlCache(
        tmp_path,
        "noarch",
        update_only=True,
        db_url=postgresql_database.url,
    )

    foo = cache.database_path("foo-1.0-0.conda")
    stale = cache.database_path("stale-1.0-0.conda")
    bar = cache.database_path("bar-1.0-0.conda")

    stat = model.Stat.__table__
    with cache.engine.begin() as connection:
        connection.execute(
            stat.insert(),
            [
                {"stage": "fs", "path": foo, "mtime": 1, "size": 10},
                {"stage": "fs", "path": stale, "mtime": 1, "size": 11},
            ],
        )

    listdir_stat = [
        {"path": foo, "mtime": 2, "size": 20},
        {"path": bar, "mtime": 3, "size": 30},
    ]

    cache.store_fs_state(listdir_stat)

    with cache.engine.connect() as connection:
        rows = connection.execute(
            select(stat.c.path, stat.c.mtime, stat.c.size)
            .where(stat.c.stage == "fs")
            .where(stat.c.path.startswith(cache.database_prefix, autoescape=True))
            .order_by(stat.c.path)
        ).all()

    found = {row.path: (row.mtime, row.size) for row in rows}
    assert found == {foo: (2, 20), bar: (3, 30), stale: (1, 11)}


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_store_fs_state_update_only_false(tmp_path: Path, postgresql_database):
    assert PsqlCache
    cache = PsqlCache(
        tmp_path,
        "noarch",
        update_only=False,
        db_url=postgresql_database.url,
    )

    foo = cache.database_path("foo-1.0-0.conda")
    stale = cache.database_path("stale-1.0-0.conda")
    bar = cache.database_path("bar-1.0-0.conda")

    stat = model.Stat.__table__
    with cache.engine.begin() as connection:
        connection.execute(
            stat.insert(),
            [
                {"stage": "fs", "path": foo, "mtime": 1, "size": 10},
                {"stage": "fs", "path": stale, "mtime": 1, "size": 11},
            ],
        )

    listdir_stat = [
        {"path": foo, "mtime": 2, "size": 20},
        {"path": bar, "mtime": 3, "size": 30},
    ]

    cache.store_fs_state(listdir_stat)

    with cache.engine.connect() as connection:
        rows = connection.execute(
            select(stat.c.path, stat.c.mtime, stat.c.size)
            .where(stat.c.stage == "fs")
            .where(stat.c.path.startswith(cache.database_prefix, autoescape=True))
            .order_by(stat.c.path)
        ).all()

    found = {row.path: (row.mtime, row.size) for row in rows}
    assert found == {foo: (2, 20), bar: (3, 30)}
