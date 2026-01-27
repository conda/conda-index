"""
Test ability to index off diverse filesystems using fsspec.
"""

import json
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


class _DummyConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[object, dict | None]] = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        return []


class _DummyBegin:
    def __init__(self, connection: _DummyConnection) -> None:
        self.connection = connection

    def __enter__(self) -> _DummyConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _DummyEngine:
    def __init__(self, connection: _DummyConnection) -> None:
        self.connection = connection

    def begin(self) -> _DummyBegin:
        return _DummyBegin(self.connection)


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_cache_engine_is_cached(tmp_path: Path, monkeypatch):
    assert PsqlCache
    import conda_index.postgres.cache as cache_module

    monkeypatch.setattr(cache_module, "_engine", None)

    calls = {"create_engine": 0, "create": 0}
    engine_value = object()

    def fake_create_engine(url, echo=False):
        calls["create_engine"] += 1
        return engine_value

    def fake_model_create(engine):
        calls["create"] += 1

    monkeypatch.setattr(cache_module.sqlalchemy, "create_engine", fake_create_engine)
    monkeypatch.setattr(cache_module.model, "create", fake_model_create)

    cache = PsqlCache(tmp_path, "noarch", db_url="postgresql://example")
    first = cache.engine
    second = cache.engine

    assert first is engine_value
    assert second is engine_value
    assert calls == {"create_engine": 1, "create": 1}


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_cache_getstate_omits_engine(tmp_path: Path):
    assert PsqlCache
    cache = PsqlCache(tmp_path, "noarch", db_url="postgresql://example")
    cache.engine = object()
    state = cache.__getstate__()
    assert "engine" not in state


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_cache_database_prefix_root(tmp_path: Path):
    assert PsqlCache
    cache = PsqlCache(tmp_path, "noarch", db_url="postgresql://example")
    cache.subdir = ""
    assert cache.database_prefix == f"{cache.channel_id}/_ROOT/"


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_cache_invalid_channel_id(tmp_path: Path):
    assert PsqlCache
    cache_dir = tmp_path / ".cache"
    cache_dir.mkdir()
    (cache_dir / "cache.json").write_text(json.dumps({"channel_id": "bad-id"}))
    with pytest.raises(ValueError):
        PsqlCache(tmp_path, "noarch", db_url="postgresql://example")


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_store_fs_state_calls_update_only(tmp_path: Path):
    assert PsqlCache
    cache = PsqlCache(
        tmp_path, "noarch", update_only=True, db_url="postgresql://example"
    )
    connection = _DummyConnection()
    cache.engine = _DummyEngine(connection)

    listdir_stat = [
        {"path": cache.database_path("foo-1.0-0.conda"), "mtime": 1, "size": 10},
        {"path": cache.database_path("bar-1.0-0.conda"), "mtime": 2, "size": 20},
    ]

    cache.store_fs_state(listdir_stat)

    assert len(connection.calls) == 2
    insert_params = [params for _, params in connection.calls if params]
    assert all(param["stage"] == "fs" for param in insert_params)


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql_store_fs_state_calls_delete(tmp_path: Path):
    assert PsqlCache
    cache = PsqlCache(
        tmp_path,
        "noarch",
        update_only=False,
        db_url="postgresql://example",
    )
    connection = _DummyConnection()
    cache.engine = _DummyEngine(connection)

    listdir_stat = [
        {"path": cache.database_path("foo-1.0-0.conda"), "mtime": 1, "size": 10},
        {"path": cache.database_path("bar-1.0-0.conda"), "mtime": 2, "size": 20},
    ]

    cache.store_fs_state(listdir_stat)

    assert len(connection.calls) == 3
    assert connection.calls[0][1] is None
    insert_params = [params for _, params in connection.calls[1:] if params]
    assert all(param["stage"] == "fs" for param in insert_params)


@pytest.mark.skipif(model is None, reason="Could not import postgres model")
def test_psql_model_create_calls_metadata(monkeypatch):
    called = {}

    def fake_create_all(engine):
        called["engine"] = engine

    monkeypatch.setattr(model.metadata_obj, "create_all", fake_create_all)
    engine = object()
    model.create(engine)
    assert called["engine"] is engine


def test_psql_bad_channel_id(tmp_path: Path):
    """
    Error if channel_id doesn't match a pattern.
    """
    assert PsqlCache
    db_filename = tmp_path / ".cache" / "cache.json"
    db_filename.parent.mkdir()
    db_filename.write_text('{"channel_id": "%"}')
    with pytest.raises(ValueError, match="invalid channel_id"):
        PsqlCache(
            tmp_path,
            "noarch",
            update_only=False,
            db_url="",  # doesn't eagerly connect
        )
