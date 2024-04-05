"""
Test ability to index off diverse filesystems using fsspec.
"""

import json

import fsspec.core
import pytest

from conda_index.index import ChannelIndex
from conda_index.index.fs import FsspecFS


def test_fsspec(tmp_path, http_package_server):
    """
    Test that conda-index can directly index remote files.
    """
    channel_root = tmp_path / "channel"  # used for sqlite cache in this mode
    channel_root.mkdir()
    output = tmp_path / "output"  # default same as channel
    output.mkdir()

    host, port = http_package_server.socket.getsockname()
    channel_url = f"http://{host}:{port}/"

    # e.g. http://127.0.0.1:54963/osx-64/

    test_subdirs = ("noarch", "osx-64")

    fs, url = fsspec.core.url_to_fs(
        f"simplecache::{channel_url}",
        simplecache={"cache_storage": str(tmp_path / "fsspec-cache")},
    )

    # channel_url, fs both required
    with pytest.raises(TypeError):
        ChannelIndex(channel_root, "channel", channel_url="yes", fs=None)

    channel_index = ChannelIndex(
        channel_root,
        channel_name="fsspec-channel",
        output_root=output,
        subdirs=test_subdirs,  # listdir('tests/index_data/packages')?
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=False,
        compact_json=True,
        channel_url=url,
        fs=FsspecFS(fs),
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )

    for subdir in test_subdirs:
        repodata = json.loads((output / subdir / "repodata.json").read_text())
        assert len(repodata["packages"]) + len(repodata["packages.conda"])
