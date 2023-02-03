[miniconda3]: https://docs.conda.io/projects/continuumio-conda/en/latest/user-guide/install/index.html
[anaconda]: https://docs.anaconda.com/anaconda/install/

# conda-index
conda index, formerly part of conda-build. Create `repodata.json` for
collections of conda packages.

## Getting started

### Prerequisites
If you don't have `conda` installed yet, please start by downloading
the latest version of [anaconda] or [miniconda3].

Also remember to initialize `conda` and activate a `conda` environment.

```bash
@barabo ➜ /workspaces/conda-index (main) $ conda init bash

@barabo ➜ /workspaces/conda-index (main) $ conda activate

@barabo ➜ /workspaces/conda-index (main) $ bash  # if prompted to restart bash
(base) @barabo ➜ /workspaces/conda-index (main) $
```

In the example above, the default prompt has changed to show which `conda`
environment I'm using `(base)`.

### Using your local packages for testing
Once you have `conda` installed and updated, you should find a collection of
cached packages in your `conda` installation directory, which should also be
set as an environment variable: `${CONDA_DIR}`.

```sh
(base) @barabo ➜ /workspaces/conda-index (main) $ ls -1 ${CONDA_DIR}/pkgs/*.conda | head
/opt/conda/pkgs/ca-certificates-2023.01.10-h06a4308_0.conda
/opt/conda/pkgs/certifi-2022.12.7-py310h06a4308_0.conda
/opt/conda/pkgs/cffi-1.15.1-py310h5eee18b_3.conda
/opt/conda/pkgs/conda-23.1.0-py310h06a4308_0.conda
/opt/conda/pkgs/conda-package-handling-2.0.2-py310h06a4308_0.conda
/opt/conda/pkgs/conda-package-streaming-0.7.0-py310h06a4308_0.conda
/opt/conda/pkgs/cryptography-38.0.4-py310h9ce1e76_0.conda
/opt/conda/pkgs/idna-3.4-py310h06a4308_0.conda
/opt/conda/pkgs/ld_impl_linux-64-2.38-h1181459_1.conda
/opt/conda/pkgs/libffi-3.4.2-h6a678d5_6.conda
```

## Run normally

```sh
python -m conda_index <path to channel directory>
```

Note `conda index` may find legacy `conda-build index` instead.

## Run for debugging

```sh
python -m conda_index --verbose --threads=1 <path to channel directory>
```

## Contributing

```sh
conda create -n conda-index "python >=3.9" conda conda-build pip
# pip >=22 is required for pip install -e conda-index
pip install --upgrade pip # if pip < 22
# in a parent directory
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
