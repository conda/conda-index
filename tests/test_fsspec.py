"""
Test ability to index off diverse filesystems using fsspec.
"""

import os.path
from conda_index.index import ChannelIndex


def test_fsspec(tmp_path, http_package_server):
    output = (tmp_path / "output").mkdir()

    host, port = http_package_server.socket.getsockname()
    base = f"http://{host}:{port}/"

    # e.g. http://127.0.0.1:54963/osx-64/

    channel_index = ChannelIndex(
        base,
        channel_name="fsspec-channel",
        output_root=output,
        subdirs=("noarch", "linux-64"),
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=False,
        compact_json=True,
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )
