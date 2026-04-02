"""
Tests for abstact BaseCondaIndexCache.
"""

from __future__ import annotations

from typing import Any, Iterator

from conda_index.index import cache
from conda_index.utils import CONDA_PACKAGE_EXTENSIONS


class DummyCache(cache.BaseCondaIndexCache):
    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        return super().database_prefix + "test/"

    def convert(self):
        return super().convert()

    def store(
        self,
        fn: str,
        size: int,
        mtime,
        members: dict[str, str | bytes],
        index_json: dict,
    ):
        raise NotImplementedError

    def load_all_from_cache(self, fn):
        raise NotImplementedError

    def store_fs_state(self, listdir_stat: Iterator[dict[str, Any]]):
        raise NotImplementedError

    def changed_packages(self) -> list[cache.ChangedPackage]:
        raise NotImplementedError

    def indexed_packages(self) -> cache.IndexedPackages:
        raise NotImplementedError

    def indexed_shards_2(
        self,
        desired: set[str] | None = None,
        *,
        pack_record=None,
    ) -> Iterator[cache.IndexedShard]:
        raise NotImplementedError

    def run_exports(self) -> Iterator[tuple[str, dict]]:
        raise NotImplementedError


def test_cache(tmp_path):
    """
    Code coverage.
    """

    c = DummyCache(
        str(tmp_path),
        "linux-64",
        package_extensions=CONDA_PACKAGE_EXTENSIONS + (".whl",),
    )

    package = "foo.conda"
    db_path = c.database_path(package)
    assert db_path.startswith(c.database_prefix)
    assert c.plain_path(db_path) == package

    c.convert()
    c.close()

    assert c.package_section_for_path("file.whl") == "packages.whl"
