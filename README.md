# conda-index

`conda-index` creates conda channels from a collection of conda packages.

The `conda_index` command operates on a channel directory. A channel directory
can't be named after a supported platform and must contain a `noarch`
subdirectory. It will usually contain other subdirectories named for conda's
supported platforms `linux-64`, `win-64`, `osx-64`, etc. Place packages into
their corresponding subdirectories. Then run conda-index to extract metadata
from these packages to generate `index.html`, `repodata.json` etc. with
summaries of the packages' metadata. `conda` uses the metadata to solve
dependencies before doing an install.

By default the metadata is output to the same directory tree as the channel
directory but it can be output to a separate tree with the `--output <output>`
parameter. The metadata cache is placed with the packages, in `.cache` folders
under each platform subdirectory.

After `conda-index` has finished, its output can be used as a channel `conda
install -c file:///path/to/output ...` or would typically be placed on a web
server.

## Run normally

```sh
python -m conda_index <path to channel directory>
```
An equivalent `conda` subcommand, `conda index`, is also available.

## Run for debugging

```sh
python -m conda_index --verbose --threads=1 <path to channel directory>
```

## Contributing

```sh
conda create -n conda-index "python >=3.9" conda conda-build "pip >=22"

git clone https://github.com/conda/conda-index.git
pip install -e conda-index[test]

cd conda-index
pytest
```

## Parallelism

This version of conda-index continues indexing packages from other subdirs while
the main thread is writing `repodata.json`.

All `current_repodata.json` are generated in parallel. This may use a lot of ram
if `repodata.json` has tens of thousands of entries.
