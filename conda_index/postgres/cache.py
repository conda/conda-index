"""
Use sqlalchemy+postgresql instead of sqlite.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Iterator

import sqlalchemy
from psycopg2 import OperationalError
from sqlalchemy import Connection, cte, join, or_, select
from sqlalchemy.dialects.postgresql import insert

from conda_index.index.cache import BaseCondaIndexCache, clear_newline_chars
from conda_index.index.fs import MinimalFS
from conda_index.index.sqlitecache import (
    ICON_PATH,
    PATH_TO_TABLE,
    TABLE_NO_CACHE,
    ChangedPackage,
    cacher,
    pack_record,
)

from . import model

log = logging.getLogger(__name__)

# prevent SQL LIKE abuse
CHANNEL_ID_PATTERN = r"^[a-zA-Z0-9]*$"

# XXX convert based on streaming "blob of json's to put in store()"

_engine = None


class PsqlCache(BaseCondaIndexCache):
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

        # each on-disk location gets a unique (random) prefix in the shared database
        if not self.db_filename.exists():
            self.db_filename.parent.mkdir(parents=True)
            self.db_filename.write_text(json.dumps({"channel_id": os.urandom(8).hex()}))
            self.cache_is_brand_new = True
        else:
            self.cache_is_brand_new = False

        self.channel_id = json.loads(self.db_filename.read_text())["channel_id"]
        if not re.match(CHANNEL_ID_PATTERN, self.channel_id):
            raise ValueError(
                f'{self.db_filename} contains invalid channel_id="{self.channel_id}"'
            )

    def __getstate__(self):
        """
        Remove db connection when pickled.
        """
        return {k: self.__dict__[k] for k in self.__dict__ if k not in ("engine",)}

    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        # If recording information about the channel_root, use '_ROOT' for nice
        # prefix searches
        return f"{self.channel_id}/{self.subdir or '_ROOT'}/"

    @cacher
    def engine(self):
        # Per-process module-scoped engine cache is one way to solve ProcessPool
        # "too many connections" issue
        global _engine
        if _engine:
            return _engine
        engine = sqlalchemy.create_engine(self.db_url, echo=False)
        model.create(engine)
        _engine = engine
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
        connection: Connection
        with self.engine.begin() as connection:
            stat = model.Stat.__table__
            connection.execute(
                stat.delete()
                .where(stat.c.stage == "fs")
                .where(stat.c.path.startswith(self.database_prefix, autoescape=True))
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
        connection: Connection
        with self.engine.begin() as connection:
            for have_path in members:
                table: str = PATH_TO_TABLE[have_path]
                if table in TABLE_NO_CACHE or table == "index_json":
                    continue  # not cached, or for index_json cached at end

                table_obj = model.Base.metadata.tables[table]
                data_column = {"icon": "icon_png"}.get(table, table)
                parameters = {
                    "path": database_path,
                    data_column: members.get(have_path),
                }

                if have_path == ICON_PATH:
                    # not parsed as json
                    pass
                elif parameters[table] is not None:
                    # There will be an extra json.dumps() on the way to the
                    # database. May not be convenient to pass json text
                    # directly into the database's json parser for a small
                    # gain.
                    parameters[table] = json.loads(parameters[table])

                insert_obj = insert(table_obj)
                query = insert_obj.values(**parameters).on_conflict_do_update(
                    index_elements=[table_obj.c.path],
                    set_={data_column: insert_obj.excluded[data_column]},
                )
                # Could delete from all metadata tables that we didn't just see.
                try:
                    connection.execute(query)
                except OperationalError:  # e.g. malformed json.
                    log.exception("table=%s parameters=%s", table, parameters)
                    raise

            table = "index_json"
            index_json_table = model.Base.metadata.tables[table]
            insert_obj = insert(index_json_table)
            connection.execute(
                (
                    insert(index_json_table)
                    .values(path=database_path, index_json=index_json)
                    .on_conflict_do_update(
                        index_elements=[table_obj.c.path],
                        set_={table: insert_obj.excluded.index_json},
                    )  # it will cast to jsonb automatically
                )
            )

            stat_table = model.Base.metadata.tables["stat"]
            values = {
                "mtime": mtime,
                "size": size,
                "sha256": index_json["sha256"],
                "md5": index_json["md5"],
            }
            connection.execute(
                insert(stat_table)
                .values({"path": database_path, "stage": "indexed", **values})
                .on_conflict_do_update(
                    index_elements=[stat_table.c.path, stat_table.c.stage], set_=values
                )
            )

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
            .where(stat_fs.c.path.startswith(self.database_prefix, autoescape=True))
            .where(
                or_(
                    stat_fs.c.mtime != stat_indexed.c.mtime,
                    stat_fs.c.size != stat_indexed.c.size,
                    stat_indexed.c.path == None,  # noqa: E711
                )
            )
        )

        connection: Connection
        with self.engine.begin() as connection:
            return [
                dict(path=row.path, size=row.size, mtime=row.mtime)
                for row in connection.execute(query)
            ]  # type: ignore

    def indexed_shards(self, desired: set | None = None, *, pack_record=pack_record):
        """
        Yield (package name, all packages with that name) from database ordered
        by name, path i.o.w. filename.

        :param desired: If not None, set of desired package names.
        :param pack_record: Function passed each record, returning a modified record. Override to change the default hex to bytes hash conversions.
        """
        index_json_table = model.Base.metadata.tables["index_json"]
        stat_table = model.Base.metadata.tables["stat"]

        query = (
            select(
                index_json_table.c.name,
                index_json_table.c.path,
                index_json_table.c.index_json,
            )
            .select_from(
                join(
                    index_json_table,
                    stat_table,
                    index_json_table.c.path == stat_table.c.path,
                )
            )
            .where(stat_table.c.stage == self.upstream_stage)
            .order_by(
                index_json_table.c.name,
                index_json_table.c.path,
            )
        )

        connection: Connection
        with self.engine.begin() as connection:
            for name, rows in itertools.groupby(
                connection.execute(query),
                lambda k: k.name,
            ):
                shard = {"packages": {}, "packages.conda": {}}
                for row in rows:
                    name, path, record = row
                    path = self.plain_path(path)
                    if not path.endswith((".tar.bz2", ".conda")):
                        log.warning("%s doesn't look like a conda package", path)
                        continue
                    key = "packages" if path.endswith(".tar.bz2") else "packages.conda"
                    # This will be passed to the patch function, which we hope
                    # does not look for hex hash values.
                    shard[key][path] = pack_record(record)

                if not desired or name in desired:
                    yield (name, shard)

    def indexed_packages(self):
        """
        Return "packages" and "packages.conda" values from the cache.
        """
        packages = {}
        packages_conda = {}

        def nopack_record(record):
            return record

        for _, shard in self.indexed_shards(pack_record=nopack_record):
            packages.update(shard["packages"])
            packages_conda.update(shard["packages.conda"])

        return packages, packages_conda

    def load_all_from_cache(self, fn: str):
        """
        Load package data into a single dict for channeldata.

        :param fn: filename from channeldata.json; can be missing from database.
        """
        # XXX called in parallel by ChannelIndex(), easily exceeds postgresql connection limit
        connection: Connection
        with self.engine.begin() as connection:
            try:
                stat_table = model.Base.metadata.tables["stat"]
                row = connection.execute(
                    select(stat_table).where(
                        stat_table.c.stage == self.upstream_stage
                        and stat_table.c.path == self.database_path(fn)
                    )
                ).first()
                if not row:
                    raise TypeError()
                mtime = row.mtime
            except TypeError:  # .fetchone() was None
                log.warning("%s mtime not found in cache", fn)
                return {}

            tables = model.Base.metadata.tables

            index_json = tables["index_json"]
            about = tables["about"]
            post_install = tables["post_install"]
            recipe = tables["recipe"]
            run_exports = tables["run_exports"]

            # This method reads up pretty much all of the cached metadata, except
            # for paths. It all gets dumped into a single map.

            BIG_JOIN = (
                index_json.join(
                    about,
                    isouter=True,
                    onclause=index_json.c.path == about.c.path,
                )
                .join(
                    post_install,
                    isouter=True,
                    onclause=index_json.c.path == post_install.c.path,
                )
                .join(
                    recipe,
                    isouter=True,
                    onclause=index_json.c.path == recipe.c.path,
                )
                .join(
                    run_exports,
                    isouter=True,
                    onclause=index_json.c.path == run_exports.c.path,
                )
            )

            row = connection.execute(
                select(BIG_JOIN).where(index_json.c.path == self.database_path(fn))
            ).first()

            if row is None:
                return {}

            data = {}
            try:
                # This order matches the old implementation. clobber recipe, about fields with index_json.
                for column in ("recipe", "about", "post_install", "index_json"):
                    if column_data := getattr(row, column):  # is not null or empty
                        if not isinstance(column_data, dict):  # pragma: no cover
                            log.warning(
                                f"scalar {column_data} found in {column} for {fn}"
                            )
                            continue
                        data.update(column_data)
            except IndexError:
                row = None

            data["mtime"] = mtime

            source = data.get("source", {})
            try:
                data.update({"source_" + k: v for k, v in source.items()})
            except AttributeError:
                # sometimes source is a  list instead of a dict
                pass
            clear_newline_chars(data, "description")
            clear_newline_chars(data, "summary")

            # if run_exports was NULL / empty string, 'loads' the empty object
            data["run_exports"] = getattr(row, "run_exports", {}) if row else {}

        return data

    def run_exports(self):
        """
        Query returning run_exports data, to be formatted by
        ChannelIndex.build_run_exports_data()
        """
        stat = model.Base.metadata.tables["stat"]
        run_exports = model.Base.metadata.tables["run_exports"]
        query = stat.join(run_exports, onclause=stat.c.path == run_exports.c.path)
        connection: Connection
        with self.engine.begin() as connection:
            for row in connection.execute(
                select(query).where(stat.c.stage == self.upstream_stage)
            ):
                yield (self.plain_path(row.path), json.dumps(row.run_exports))
