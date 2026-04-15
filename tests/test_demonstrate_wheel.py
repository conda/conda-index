"""
Show how an API user might use ChannelIndex() to generate .whl repodata.
"""

import json
from pathlib import Path

from conda_index.index import ChannelIndex
from conda_index.utils import CONDA_PACKAGE_EXTENSIONS, METADATA_UPSTREAM_STAGE, LOCAL_FILE_UPSTREAM_STAGE

HERE = Path(__file__).parent


def test_demonstrate_wheel(tmp_path: Path):
    """
    Write v3 draft format .whl repodata to tmp_path.
    """
    channel_index = ChannelIndex(
        tmp_path,
        "haswheels",  # channel name if different than last segment of tmp_path
        repodata_v3=True,
        # update_only=True,
        # save_fs_state=False,
        upstream_stages=[METADATA_UPSTREAM_STAGE, LOCAL_FILE_UPSTREAM_STAGE],
        write_current_repodata=False,
        cache_kwargs={"package_extensions": CONDA_PACKAGE_EXTENSIONS + (".whl",)},
    )
    cache = channel_index.cache_for_subdir("noarch", stage=METADATA_UPSTREAM_STAGE)

    input = json.loads((HERE / "demonstrate_wheel.json").read_text())
    wheels = {
        f"{path}.whl": repodata for (path, repodata) in input["v3"]["whl"].items()
    }

    # Define the set all packages that will be included in repodata.json, or add
    # packages and leave existing packages if ChannelIndex.update_only == True.
    # This updates the list of packages in the "upstream" state in the Stat()
    # table. Cached package metadata (stat table where state = 'indexed',
    # index_json table, etc.) is retained even if those package names are no
    # longer included in the repodata.json output.

    def listdir_like():
        for path, repodata in wheels.items():
            yield {
                "path": cache.database_path(path),
                "stage": METADATA_UPSTREAM_STAGE,
                "size": repodata["size"],
                "mtime": repodata.get(
                    "timestamp", 1
                ),  # timestamp missing from generate.py wheel repodata
            }

    cache.store_fs_state(listdir_like())

    # Has to be in stat JOIN index_json to appear in repodat
    for path, repodata in wheels.items():
        # must contain sha256 and md5 keys but values may be None
        assert "sha256" in repodata
        if "md5" not in repodata:
            repodata["md5"] = None
        # pretend we have a package with index.json but no other info/ files
        cache.store(
            cache.database_path(path),
            repodata["size"],
            repodata.get("timestamp", 1),
            {},
            repodata,
        )

    # packages from database
    packages = cache.indexed_packages()
    assert len(packages.packages_whl) == len(wheels)

    # repodata.json without repodata patches applied. Saved to
    # repodata_from_packages in full index() method, but in this case there are
    # no patches.
    repodata_json = channel_index.index_subdir("noarch")
    assert "v3" in repodata_json

    # Write complete repodata to output path for all detected subdirs. Normal
    # conda-index API users would only call this method. Since we passed
    # update_only=True, save_fs_state=False to ChannelIndex, this skips
    # extracting packages from channel_root and only outputs metadata from the
    # database.
    channel_index.index(None)

    assert list(p.name for p in tmp_path.iterdir()) == ["noarch"]
    assert (tmp_path / "noarch" / "repodata.json").exists()

    output = json.loads((tmp_path / "noarch" / "repodata.json").read_text())

    # other details differ
    assert output["v3"]["whl"] == input["v3"]["whl"]
