# conda-index
conda index, formerly part of conda-build. Create `repodata.json` for
collections of conda packages.

## Run normally

```sh
python -m conda_index <path to channel directory>
```

## Run with extra logs

```sh
python -m conda_index.index --verbose --no-progress --threads=1 <path to channel directory>
```

## Contributing

```sh
# pip >=22 is required for pip install -e conda-index
conda create -n conda-index python=3.10 conda conda-build "pip >=22"
# in a parent directory
git clone https://github.com/conda-incubator/conda-index.git
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