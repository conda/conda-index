"""
SQLAlchemy model for conda-index, following the pattern in convert_cache.py

Used for psqlcache mode instead of low-dependencies sqlite mode.
"""

from __future__ import annotations

from sqlalchemy import JSON, TEXT, Column, Integer, LargeBinary, Table
from sqlalchemy.orm import DeclarativeBase, mapped_column

TABLE_NAMES = {
    "about",
    "icon",
    "index_json",
    "post_install",
    "recipe",
    "run_exports",
}


class Base(DeclarativeBase):
    pass


metadata_obj = Base.metadata

for table in TABLE_NAMES:
    if table == "icon":
        data_column = Column("icon_png", LargeBinary)
    else:
        data_column = Column(table, JSON)  # or JSONB for postgresql?
    Table(table, metadata_obj, Column("path", TEXT, primary_key=True), data_column)


class Stat(Base):
    __tablename__ = "stat"

    stage = mapped_column(TEXT, default="indexed", nullable=False, primary_key=True)
    path = mapped_column(TEXT, nullable=False, primary_key=True)
    mtime = mapped_column(Integer)
    size = mapped_column(Integer)
    sha256 = mapped_column(TEXT)
    md5 = mapped_column(TEXT)
    last_modified = mapped_column(TEXT)
    etag = mapped_column(TEXT)


def create(engine):
    """
    Create schema. Safe to call on every connection.
    """
    metadata_obj.create_all(engine)
