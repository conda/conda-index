from typing import Any

from conda_index.index import cache


class TestCache(cache.BaseCondaIndexCache):
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

    def store_fs_state(self, listdir_stat: cache.Iterator[dict[str, Any]]):
        raise NotImplementedError

    def changed_packages(self) -> list[cache.ChangedPackage]:
        raise NotImplementedError

    def indexed_packages(self) -> tuple[dict, dict]:
        raise NotImplementedError

    def indexed_shards(self, desired: set | None = None):
        raise NotImplementedError

    def run_exports(self) -> cache.Iterator[tuple[str, dict]]:
        raise NotImplementedError


def test_cache(tmp_path):
    """
    Code coverage.
    """

    c = TestCache(str(tmp_path), "linux-64")

    package = "foo.conda"
    db_path = c.database_path(package)
    assert db_path.startswith(c.database_prefix)
    assert c.plain_path(db_path) == package

    c.convert()
    c.close()
