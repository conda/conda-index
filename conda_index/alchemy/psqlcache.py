"""
Use sqlalchemy+postgresql instead of sqlite.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator

import sqlalchemy
from psycopg2 import OperationalError
from sqlalchemy import cte, join, or_, select

from conda_index.index import sqlitecache
from conda_index.index.fs import MinimalFS
from conda_index.index.sqlitecache import (
    ICON_PATH,
    PATH_TO_TABLE,
    TABLE_NO_CACHE,
    ChangedPackage,
    cacher,
)

from . import model

log = logging.getLogger(__name__)


class PsqlCache(sqlitecache.CondaIndexCache):
    def __init__(
        self,
        channel_root: Path | str,
        subdir: str,
        *,
        fs: MinimalFS | None = None,
        channel_url: str | None = None,
        upstream_stage: str = "fs",
        db_url="postgresql://conda_index_test@localhost/conda_index_test",
    ):
        super().__init__(
            channel_root,
            subdir,
            fs=fs,
            channel_url=channel_url,
            upstream_stage=upstream_stage,
        )
        self.db_filename = self.channel_root / ".cache" / "cache.json"
        self.db_url = db_url

        # XXX do we want to use the fs abstraction here? Or should we finally
        # separate package, database, output directories?
        if not self.db_filename.exists():
            self.db_filename.parent.mkdir(parents=True)
            self.db_filename.write_text(json.dumps({"channel_id": os.urandom(8).hex()}))
            self.cache_is_brand_new = True
        else:
            self.cache_is_brand_new = False

        self.channel_id = json.loads(self.db_filename.read_text())["channel_id"]

    def __getstate__(self):
        """
        Remove db connection when pickled.
        """
        return {k: self.__dict__[k] for k in self.__dict__ if k not in ("db", "engine")}

    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        # If recording information about the channel_root, use '_ROOT' for nice
        # prefix searches
        return f"{self.channel_id}/{self.subdir or '_ROOT'}/"

    @cacher
    def db(self):
        engine = self.engine
        conn = engine.raw_connection()  # semi-compatible with existing SQL?
        return ConnectionWrapper(conn)

    @cacher
    def engine(self):
        engine = sqlalchemy.create_engine(self.db_url, echo=True)
        model.create(engine)
        return engine

    def convert(self, force=False):
        """
        Load filesystem cache into sqlite.
        """
        # or call model.create(engine) here?
        log.warning(f"{self.__class__}.convert() is not implemented")

    def store_fs_state(self, listdir_stat: Iterator[dict[str, Any]]):
        """
        Write {path, mtime, size} into database.
        """
        with self.engine.begin() as connection:
            # always stage='fs', not custom upstream_stage which would be
            # handled in a subclass
            stat = model.Stat.__table__
            # mypy doesn't know these types
            connection.execute(
                stat.delete().where(stat.c.path.like(self.database_path_like))
            )
            for item in listdir_stat:
                connection.execute(stat.insert(), {**item, "stage": "fs"})

    def store(
        self,
        fn: str,
        size: int,
        mtime,
        members: dict[str, str | bytes],
        index_json: dict,
    ):
        """
        Write cache for a single package to database.
        """
        database_path = self.database_path(fn)
        with self.engine.begin() as connection:
            for have_path in members:
                table = PATH_TO_TABLE[have_path]
                if table in TABLE_NO_CACHE or table == "index_json":
                    continue  # not cached, or for index_json cached at end

                table_obj = model.Base.metadata.tables[table]

                parameters = {"path": database_path, "data": members.get(have_path)}
                if have_path == ICON_PATH:
                    query = """
                                INSERT OR REPLACE into icon (path, icon_png)
                                VALUES (:path, :data)
                                """
                elif parameters["data"] is not None:
                    query = f"""
                                INSERT OR REPLACE INTO {table} (path, {table})
                                VALUES (:path, json(:data))
                                """
                # Could delete from all metadata tables that we didn't just see.
                try:
                    connection.execute(query, parameters)
                except OperationalError:  # e.g. malformed json.
                    log.exception("table=%s parameters=%s", table, parameters)
                    # XXX delete from cache
                    raise

            # sqlite json() function removes whitespace and ensures valid json
            connection.execute(
                "INSERT OR REPLACE INTO index_json (path, index_json) VALUES (:path, json(:index_json))",
                {"path": database_path, "index_json": json.dumps(index_json)},
            )

            self.store_index_json_stat(
                database_path, mtime, size, index_json
            )  # we don't need this return value; it will be queried back out to generate repodata

    def changed_packages(self) -> list[ChangedPackage]:  # XXX or FileInfo dataclass
        """
        Compare upstream to 'indexed' state.

        Return packages in upstream that are changed or missing compared to 'indexed'.
        """

        stat_table = model.Stat.__table__
        stat_fs = cte(
            select(stat_table).where(stat_table.c.stage == self.upstream_stage),
            "stat_fs",
        )
        stat_indexed = cte(
            select(stat_table).where(stat_table.c.stage == "indexed"),
            "stat_indexed",
        )

        query = (
            select(stat_fs)
            .select_from(
                join(
                    stat_fs,
                    stat_indexed,
                    stat_fs.c.path == stat_indexed.c.path,
                    isouter=True,
                )
            )
            .where(stat_fs.c.path.like(self.database_path_like))
            .where(
                or_(
                    stat_fs.c.mtime != stat_indexed.c.mtime,
                    stat_fs.c.size != stat_indexed.c.size,
                    stat_indexed.c.path == None,  # noqa: E711
                )
            )
        )

        with self.engine.begin() as connection:
            return [
                dict(path=row.path, size=row.size, mtime=row.mtime)
                for row in connection.execute(query)
            ]  # type: ignore


class ConnectionWrapper:
    """
    sqlite-style connection.
    """

    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.conn.rollback()

    def execute(self, *args, **kwargs):
        return self.cursor.execute(*args, **kwargs)
