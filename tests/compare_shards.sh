#!/bin/sh
# Generate sharded repodata for comparison with "prefix-dev sharded repodata"


python3 -m conda_index --write-shards --write-monolithic --upstream-stage=clone --no-update-cache --patch-generator ~/miniconda3/pkgs/conda-forge-repodata-patches-20240401.20.33.07-hd8ed1ab_1.conda  --output /tmp/shards ~/prog/conda-test-data/conda-forge

mkdir -p /tmp/shards2/noarch
mkdir -p /tmp/shards2/linux-64

cd /tmp/shards2
for i in noarch linux-64; do
    # hardlink
    rm $i/repodata*
    ln /tmp/shards/$i/repodata*json* $i/
done

cd -

python3 compare_shards.py
