#!/usr/bin/env python

from pathlib import Path
import sys
import split_repo
import zstandard
import msgpack

SUBDIRS = "noarch", "linux-64"


def main(folder=Path("/tmp/shards2")):
    repo_url = "https://conda.anaconda.org/conda-forge"
    for subdir in SUBDIRS:
        split_repo.split_repo_file(
            repo_url,
            subdir,
            folder,
            repodata=folder / subdir / "repodata.json",
            run_exports=Path(folder / subdir / "run_exports.json"),
        )


class ShardReader(dict):
    """
    Load shards from file on dict access.
    """

    shards_base: Path

    @classmethod
    def from_path(cls, repodata: Path):
        shards_index = msgpack.loads(zstandard.decompress(repodata.read_bytes()))
        assert isinstance(shards_index, dict)
        shards = cls(shards_index["shards"])
        if "shards_base_url" in shards_index["info"]:
            shards.shards_base = (
                repodata.parent / shards_index["info"]["shards_base_url"]
            )
        else:
            shards.shards_base = repodata.parent
        shards_index["shards"] = shards
        return shards_index

    def __getitem__(self, key):
        shard_id = super().__getitem__(key)
        if isinstance(shard_id, bytes):  # not loaded yet
            shard_path = (self.shards_base / shard_id.hex()).with_suffix(".msgpack.zst")
            item = msgpack.loads(zstandard.decompress(shard_path.read_bytes()))
            self[key] = item
            return item
        return shard_id


def load_shards():
    shards = ShardReader.from_path(
        Path("/tmp/shards/linux-64/repodata_shards.msgpack.zst")
    )

    shards2 = ShardReader.from_path(
        Path("/tmp/shards2/linux-64/repodata_shards.msgpack.zst")
    )

    return shards, shards2


if __name__ == "__main__":
    try:
        folder = Path(sys.argv[1])
    except IndexError:
        folder = Path("/tmp/shards2")
    main(folder)

    shards, shards2 = load_shards()

    print("conda-index info", shards["info"])
    print("split_repo info", shards2["info"])

    print("Keys", shards.keys(), "split_repo keys", shards2.keys())

    same = True
    for key in shards["shards"]:
        if shards["shards"][key] != shards2["shards"][key]:
            print("!=", key)
            same = False

    print("All shards are the same" if same else "Some shards are different")
