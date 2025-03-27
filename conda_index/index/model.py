"""
SQLAlchemy model for conda-index, following the pattern in convert_cache.py

Used for psqlcache mode instead of low-dependencies sqlite mode.
"""

from __future__ import annotations


from sqlalchemy import (
    JSON,
    TEXT,
    Column,
    LargeBinary,
    Table,
)
from sqlalchemy.orm import DeclarativeBase

TABLE_NAMES = {
    "about",
    "icon",
    "index_json",
    "post_install",
    "recipe",
    "run_exports",
}

# stat table


class Base(DeclarativeBase):
    pass


metadata_obj = Base.metadata

for table in TABLE_NAMES:
    if table == "icon":
        data_column = Column("icon_png", LargeBinary)
    else:
        data_column = Column(table, JSON)  # or JSONB for postgresql?
    Table(table, metadata_obj, Column("path", TEXT, primary_key=True), data_column)

# TODO express stat table as Table or mapped class:
# conn.execute(
#     """CREATE TABLE IF NOT EXISTS stat (
#             stage TEXT NOT NULL DEFAULT 'indexed',
#             path TEXT NOT NULL,
#             mtime NUMBER,
#             size INTEGER,
#             sha256 TEXT,
#             md5 TEXT,
#             last_modified TEXT,
#             etag TEXT
#         )"""
# )


def create(engine):
    """
    Create schema. Safe to call on every connection.
    """
    metadata_obj.create_all(engine)
