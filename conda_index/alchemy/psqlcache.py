"""
Use sqlalchemy+postgresql instead of sqlite.
"""

import json
import logging
import os
from pathlib import Path

import sqlalchemy

from conda_index.index import sqlitecache
from conda_index.index.fs import FileInfo, MinimalFS
from conda_index.index.sqlitecache import (
    COMPUTED,
    INDEX_JSON_PATH,
    PATH_TO_TABLE,
    TABLE_NO_CACHE,
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

        self.channel_id = json.loads(self.db_filename.read_text())["channel_id"]

    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        # If recording information about the channel_root, use '_ROOT' for nice
        # prefix searches
        return f"{self.channel_id}/{self.subdir or '_ROOT'}/"

    def db(self):
        engine = sqlalchemy.create_engine(self.db_url)
        model.create(engine)
        return engine.raw_connection()  # semi-compatible with existing SQL?

    def convert(self, force=False):
        """
        Load filesystem cache into sqlite.
        """
        # or call model.create(engine) here?
        log.warning(f"{self.__class__}.convert() is not implemented")
