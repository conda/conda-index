"""
Sharded repodata from conda-index's small test repository.
"""

from pathlib import Path

from .. import yaml
from . import logutil
from .shards import ChannelIndexShards

if __name__ == "__main__":
    logutil.configure()

    rss = False
    channeldata = False
    current_index_versions_file = None
    patch_generator = None
    dir = Path(__file__).parents[2] / "tests" / "index_data" / "packages"
    output = dir.parent / "shards"
    assert dir.exists(), dir
    channel_index = ChannelIndexShards(
        dir.expanduser(),
        channel_name=dir.name,
        output_root=output,
        subdirs=None,
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=True,
        compact_json=True,
        base_url=None,
    )

    current_index_versions = None
    if current_index_versions_file:
        with open(current_index_versions_file) as f:
            current_index_versions = yaml.safe_load(f)

    channel_index.index(
        patch_generator=patch_generator,  # or will use outdated .py patch functions
        current_index_versions=current_index_versions,
        progress=False,  # clone is a batch job
    )

    if channeldata:  # about 2 1/2 minutes for conda-forge
        # XXX wants to read repodata.json not shards
        channel_index.update_channeldata(rss=rss)
