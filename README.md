# conda-index

conda index, formerly part of conda-build. Create `repodata.json` for
collections of conda packages.

The `conda_index` command operates on a channel directory. A channel directory
contains a `noarch` subdirectory at a minimum and will almost always contain
other subdirectories named for conda's supported platforms `linux-64`, `win-64`,
`osx-64`, etc. A channel directory cannot have the same name as a supported
platform. Place packages into the same platform subdirectory each archive was
built for. Conda-index extracts metadata from these packages to generate
`index.html`, `repodata.json` etc. with summaries of the packages' metadata.
Then conda uses the metadata to solve dependencies before doing an install.

By default, the metadata is output to the same directory tree as the channel
directory, but it can be output to a separate tree with the `--output <output>`
parameter. The metadata cache is always placed with the packages, in `.cache`
folders under each platform subdirectory.

After conda-index has finished, its output can be used as a channel `conda
install -c file:///path/to/output ...` or it would typically be placed on a web
server.

## Run normally

```sh
python -m conda_index <path to channel directory>
```

Note `conda index` (instead of `python -m conda_index`) may find legacy
`conda-build index`.

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

## Summary of changes from the previous `conda-build index` version

* Approximately 2.2x faster conda package extraction, by extracting just the
  metadata to streams instead of extracting packages to a temporary directory;
  closes the package early if all metadata has been found.

* No longer read existing `repodata.json`. Always load from cache.

* Uses a sqlite metadata cache that is orders of magnitude faster than the old
  many-tiny-files cache.

* The first time `conda index` runs, it will convert the existing file-based
  `.cache` to a sqlite3 database `.cache/cache.db`. This takes about ten minutes
  per subdir for conda-forge. (If this is interrupted, delete `cache.db` to
  start over, or packages will be re-extracted into the cache.) `sqlite3` must
  be compiled with the JSON1 extension. JSON1 is built into SQLite by default as
  of SQLite version 3.38.0 (2022-02-22).

* Each subdir `osx-64`, `linux-64` etc. has its own `cache.db`; conda-forge’s
  1.2T osx-64 subdir has a single 2.4GB `cache.db`. Storing the cache in fewer
  files saves time since there is a per-file wait to open each of the
  many tiny `.json` files in old-style `.cache/`.

* `cache.db` is highly compressible, like the text metadata. 2.4G → zstd → 88M

* No longer cache `paths.json` (only used to create `post_install.json` and not
  referenced later in the indexing process). Saves 90% disk space in `.cache`.

* Updated Python and dependency requirements.

* Mercilessly cull less-used features.

* Format with `black`

## Parallelism

This version of conda-index continues indexing packages from other subdirs while
the main thread is writing a repodata.json.

All `current_repodata.json` are generated in parallel. This may use a lot of ram
if `repodata.json` has tens of thousands of entries.
