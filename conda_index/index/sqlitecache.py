"""
cache conda indexing metadata in sqlite.
"""

from __future__ import annotations

import itertools
import json
import logging
import os
import sqlite3
from os.path import join
from pathlib import Path
from typing import Any, Iterator

import msgpack

from ..utils import CONDA_PACKAGE_EXTENSION_V1, CONDA_PACKAGE_EXTENSION_V2
from . import common, convert_cache
from .cache import BaseCondaIndexCache, ChangedPackage, cacher
from .cache import clear_newline_chars as _clear_newline_chars
from .fs import MinimalFS

log = logging.getLogger(__name__)

INDEX_JSON_PATH = "info/index.json"
ICON_PATH = "info/icon.png"
PATHS_PATH = "info/paths.json"

TABLE_TO_PATH = {
    "index_json": INDEX_JSON_PATH,
    "about": "info/about.json",
    "paths": PATHS_PATH,
    # will use the first one encountered
    "recipe": (
        "info/recipe/meta.yaml",
        "info/recipe/meta.yaml.rendered",
        "info/meta.yaml",
    ),
    # run_exports is rare but used. see e.g. gstreamer.
    # prevents 90% of early tar.bz2 exits.
    # also found in meta.yaml['build']['run_exports']
    "run_exports": "info/run_exports.json",
    "post_install": "info/post_install.json",  # computed
    "icon": ICON_PATH,  # very rare, 16 conda-forge packages
    # recipe_log: always {} in old version of cache
}

PATH_TO_TABLE = {}

for k, v in TABLE_TO_PATH.items():
    if isinstance(v, str):
        PATH_TO_TABLE[v] = k
    else:
        for path in v:
            PATH_TO_TABLE[path] = k

# read, but not saved for later
TABLE_NO_CACHE = {
    "paths",
}

# saved to cache, not found in package
COMPUTED = {"info/post_install.json"}


class CondaIndexCache(BaseCondaIndexCache):
    def __init__(
        self,
        channel_root: Path | str,
        subdir: str,
        *,
        fs: MinimalFS | None = None,
        channel_url: str | None = None,
        upstream_stage: str = "fs",
    ):
        """
        channel_root: directory containing platform subdir's, e.g. /clones/conda-forge
        subdir: platform subdir, e.g. 'linux-64'
        fs: MinimalFS (designed to wrap fsspec.spec.AbstractFileSystem); optional.
        channel_url: base url if fs is used; optional.
        upstream_stage: stage from 'stat' table used to track available packages. Default is 'fs'.
        """

        super().__init__(
            channel_root,
            subdir,
            fs=fs,
            channel_url=channel_url,
            upstream_stage=upstream_stage,
        )

        self.db_filename = Path(self.cache_dir, "cache.db")
        self.cache_is_brand_new = not self.db_filename.exists()

        log.debug(
            f"CondaIndexCache channel_root={channel_root}, subdir={subdir} db_filename={self.db_filename} cache_is_brand_new={self.cache_is_brand_new}"
        )

    def __getstate__(self):
        """
        Remove db connection when pickled.
        """
        return {k: self.__dict__[k] for k in self.__dict__ if k != "db"}

    def __setstate__(self, d):
        self.__dict__ = d

    @cacher
    def db(self) -> sqlite3.Connection:
        """
        Connection to our sqlite3 database.
        """
        conn = common.connect(str(self.db_filename))
        with conn:
            convert_cache.create(conn)
            convert_cache.migrate(conn)
        return conn

    def close(self):
        """
        Remove and close @cached_property self.db
        """
        db = self.__dict__.pop("db", None)
        if db:
            db.close()

    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        return ""

    @property
    def database_path_like(self):
        """
        Pass to LIKE to filter paths belonging to this subdir only.
        """
        return self.database_prefix + "%"

    def database_path(self, fn):
        return f"{self.database_prefix}{fn}"

    def plain_path(self, path):
        """
        path with any database-specfic prefix stripped off.
        """
        return path.rsplit("/", 1)[-1]

    def convert(self, force=False):
        """
        Load filesystem cache into sqlite.
        """
        # if this is interrupted, we may have to re-extract the missing files
        if self.cache_is_brand_new or force:
            convert_cache.convert_cache(
                self.db,
                convert_cache.extract_cache_filesystem(self.cache_dir),
            )
            with self.db:
                convert_cache.remove_prefix(self.db)
            # prepare to be sent to other thread
            self.close()

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
        with self.db:
            for have_path in members:
                table = PATH_TO_TABLE[have_path]
                if table in TABLE_NO_CACHE or table == "index_json":
                    continue  # not cached, or for index_json cached at end

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
                    self.db.execute(query, parameters)
                except sqlite3.OperationalError:  # e.g. malformed json.
                    log.exception("table=%s parameters=%s", table, parameters)
                    # XXX delete from cache
                    raise

            # sqlite json() function removes whitespace and ensures valid json
            self.db.execute(
                "INSERT OR REPLACE INTO index_json (path, index_json) VALUES (:path, json(:index_json))",
                {"path": database_path, "index_json": json.dumps(index_json)},
            )

            self.store_index_json_stat(
                database_path, mtime, size, index_json
            )  # we don't need this return value; it will be queried back out to generate repodata

    def load_all_from_cache(self, fn):
        subdir_path = self.subdir_path

        try:
            # recent stat information must exist here...
            mtime = self.db.execute(
                "SELECT mtime FROM stat WHERE stage=:upstream_stage AND path=:path",
                {"upstream_stage": self.upstream_stage, "path": self.database_path(fn)},
            ).fetchone()[0]
        except TypeError:  # .fetchone() was None
            log.warning("%s mtime not found in cache", fn)
            try:
                mtime = os.stat(join(subdir_path, fn)).st_mtime
            except FileNotFoundError:
                # don't call if it won't be found...
                log.warning("%s not found in load_all_from_cache", fn)
                return {}

        # This method reads up pretty much all of the cached metadata, except
        # for paths. It all gets dumped into a single map.

        UNHOLY_UNION = """
        SELECT
            index_json,
            about,
            post_install,
            recipe,
            run_exports
            -- icon_png
        FROM
            index_json
            LEFT JOIN about USING (path)
            LEFT JOIN post_install USING (path)
            LEFT JOIN recipe USING (path)
            LEFT JOIN run_exports USING (path)
            -- LEFT JOIN icon USING (path)
        WHERE
            index_json.path = :path
        LIMIT 2
        """  # each table must USING (path) or will cross join

        rows = self.db.execute(
            UNHOLY_UNION, {"path": self.database_path(fn)}
        ).fetchall()
        assert len(rows) < 2

        data = {}
        try:
            row = rows[0]
            # this order matches the old implementation. clobber recipe, about fields with index_json.
            for column in ("recipe", "about", "post_install", "index_json"):
                if row[column]:  # is not null or empty
                    data.update(json.loads(row[column]))
        except IndexError:
            row = None

        data["mtime"] = mtime

        source = data.get("source", {})
        try:
            data.update({"source_" + k: v for k, v in source.items()})
        except AttributeError:
            # sometimes source is a  list instead of a dict
            pass
        _clear_newline_chars(data, "description")
        _clear_newline_chars(data, "summary")

        # if run_exports was NULL / empty string, 'loads' the empty object
        data["run_exports"] = json.loads(row["run_exports"] or "{}") if row else {}

        return data

    def store_fs_state(self, listdir_stat: Iterator[dict[str, Any]]):
        with self.db:
            # always stage='fs', not custom upstream_stage which would be
            # handled in a subclass
            self.db.execute(
                "DELETE FROM stat WHERE stage='fs' AND path like :path_like",
                {"path_like": self.database_path_like},
            )
            self.db.executemany(
                """
            INSERT INTO STAT (stage, path, mtime, size)
            VALUES ('fs', :path, :mtime, :size)
            """,
                listdir_stat,
            )

    def changed_packages(self) -> list[ChangedPackage]:
        """
        Compare upstream to 'indexed' state.

        Return packages in upstream that are changed or missing compared to 'indexed'.
        """
        query = self.db.execute(
            """
            WITH
            fs AS
                ( SELECT path, mtime, size, sha256, md5 FROM stat WHERE stage = :upstream_stage ),
            cached AS
                ( SELECT path, mtime, size, sha256, md5 FROM stat WHERE stage = 'indexed' )

            SELECT fs.path, fs.mtime, fs.size, fs.sha256, fs.md5,
                cached.mtime as cached_mtime, cached.size as cached_size, cached.sha256 as cached_sha256, cached.md5 as cached_md5

            FROM fs LEFT JOIN cached USING (path)

            WHERE fs.path LIKE :path_like AND
                (fs.mtime != cached.mtime OR fs.size != cached.size OR cached.path IS NULL)
            """,
            {
                "path_like": self.database_path_like,
                "upstream_stage": self.upstream_stage,
            },
        )

        return query

    def indexed_packages(self):
        """
        Return "packages" and "packages.conda" values from the cache.
        """
        new_repodata_packages = {}
        new_repodata_conda_packages = {}

        # load cached packages
        for row in self.db.execute(
            """
            SELECT path, index_json FROM stat JOIN index_json USING (path)
            WHERE stat.stage = ?
            ORDER BY path
            """,
            (self.upstream_stage,),
        ):
            path, index_json = row
            index_json = json.loads(index_json)
            if path.endswith(CONDA_PACKAGE_EXTENSION_V1):
                new_repodata_packages[path] = index_json
            elif path.endswith(CONDA_PACKAGE_EXTENSION_V2):
                new_repodata_conda_packages[path] = index_json
            else:
                log.warning("%s doesn't look like a conda package", path)

        return new_repodata_packages, new_repodata_conda_packages

    def indexed_shards(self, desired: set | None = None):
        """
        Yield (package name, all packages with that name) from database ordered
        by name, path i.o.w. filename.

        :desired: If not None, set of desired package names.
        """
        for name, rows in itertools.groupby(
            self.db.execute(
                """SELECT index_json.name, path, index_json
                FROM stat JOIN index_json USING (path) WHERE stat.stage = ?
                ORDER BY index_json.name, index_json.path""",
                (self.upstream_stage,),
            ),
            lambda k: k[0],
        ):
            shard = {"packages": {}, "packages.conda": {}}
            for row in rows:
                name, path, index_json = row
                if not path.endswith((".tar.bz2", ".conda")):
                    log.warning("%s doesn't look like a conda package", path)
                    continue
                record = json.loads(index_json)
                key = "packages" if path.endswith(".tar.bz2") else "packages.conda"
                # we may have to pack later for patch functions that look for
                # hex hashes
                shard[key][path] = pack_record(record)

            if not desired or name in desired:
                yield (name, shard)

    def store_index_json_stat(self, database_path, mtime, size, index_json):
        self.db.execute(
            """INSERT OR REPLACE INTO stat (stage, path, mtime, size, sha256, md5)
                VALUES ('indexed', ?, ?, ?, ?, ?)""",
            (database_path, mtime, size, index_json["sha256"], index_json["md5"]),
        )

    def run_exports(self):
        """
        Query returning run_exports data, to be formatted by
        ChannelIndex.build_run_exports_data()
        """
        for path, run_exports in self.db.execute(
            """
            SELECT path, run_exports FROM stat
            LEFT JOIN run_exports USING (path)
            WHERE stat.stage = ?
            ORDER BY path
            """,
            (self.upstream_stage,),
        ):
            yield (path, json.loads(run_exports or "{}"))


def pack_record(record):
    """
    Convert hex checksums to bytes.
    """
    if sha256 := record.get("sha256"):
        record["sha256"] = bytes.fromhex(sha256)
    if md5 := record.get("md5"):
        record["md5"] = bytes.fromhex(md5)
    return record


def packb_typed(o: Any) -> bytes:
    """
    Sidestep lack of typing in msgpack.
    """
    return msgpack.packb(o)  # type: ignore
