"""
Test ability to index off diverse filesystems using fsspec.
"""

from conda_index.index import ChannelIndex


def test_fsspec(tmp_path, http_package_server):
    """
    Test that conda-index can directly index remote files.
    """
    channel = tmp_path / "channel"  # used for sqlite cache in this mode
    channel.mkdir()
    output = tmp_path / "output"  # default same as channel
    output.mkdir()

    host, port = http_package_server.socket.getsockname()
    channel_url = f"http://{host}:{port}/"

    # e.g. http://127.0.0.1:54963/osx-64/

    channel_index = ChannelIndex(
        channel,
        channel_name="fsspec-channel",
        output_root=output,
        subdirs=("noarch", "linux-64"),
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=False,
        compact_json=True,
        channel_url=channel_url,
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )

    print("Indexed")
