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
/opt/conda/pkgs/bazel-5.2.0-h6a678d5_0.conda
/opt/conda/pkgs/beautifulsoup4-4.11.1-py310h06a4308_0.conda
/opt/conda/pkgs/ca-certificates-2023.01.10-h06a4308_0.conda
/opt/conda/pkgs/certifi-2022.12.7-py310h06a4308_0.conda
/opt/conda/pkgs/cffi-1.15.1-py310h5eee18b_3.conda
/opt/conda/pkgs/chardet-4.0.0-py310h06a4308_1003.conda
/opt/conda/pkgs/conda-23.1.0-py310h06a4308_0.conda
/opt/conda/pkgs/conda-build-3.23.3-py310h06a4308_0.conda
/opt/conda/pkgs/conda-package-handling-2.0.2-py310h06a4308_0.conda
/opt/conda/pkgs/conda-package-streaming-0.7.0-py310h06a4308_0.conda
```

To use these packages as a starting point, you can make your current working
directory look like a channel by giving it a valid conda "subdir" such as
`linux-64`, `win-64`, `osx-64`, or `noarch`.

Symlink the `${CONDA_DIR}/pkgs -> ./noarch` to get started quickly.

```sh
(base) @barabo ➜ /workspaces/conda-index (main) $ ln -s ${CONDA_DIR}/pkgs ./noarch
```

Run `conda-index` locally to generate an index in `./channel`, which also
creates a cache database in your `./noarch/.cache` directory.

```sh
(base) @barabo ➜ /workspaces/conda-index (main) $ python -m conda_index --output ./channel --channeldata --rss .
2023-02-05T06:08:29 Migrate database
2023-02-05T06:08:29 CONVERT .cache
2023-02-05T06:08:29 Migrate database
2023-02-05T06:08:32 noarch cached 462.6 MB from 70 packages at 134.8 MB/second
2023-02-05T06:08:33 Subdir: noarch Gathering repodata
2023-02-05T06:08:33 noarch Writing pre-patch repodata
2023-02-05T06:08:33 noarch Applying patch instructions
2023-02-05T06:08:33 noarch Writing patched repodata
2023-02-05T06:08:33 noarch Building current_repodata subset
2023-02-05T06:08:33 noarch Writing current_repodata subset
2023-02-05T06:08:33 noarch Writing index HTML
2023-02-05T06:08:33 Completed noarch
2023-02-05T06:08:33 Channeldata subdir: noarch
2023-02-05T06:08:33 Build RSS
2023-02-05T06:08:33 Built RSS

(base) @barabo ➜ /workspaces/conda-index (main) $ tree channel/
channel/
├── channeldata.json
├── index.html
├── noarch
│   ├── current_repodata.json
│   ├── index.html
│   ├── repodata_from_packages.json
│   └── repodata.json
└── rss.xml

(base) @barabo ➜ /workspaces/conda-index (main) $ sqlite3 ./noarch/.cache/cache.db 'select * from stat where stage = "fs" limit 10;'
fs|_openmp_mutex-5.1-1_gnu.conda|1675456864|21315||||
fs|bazel-5.2.0-h6a678d5_0.conda|1675576622|41517338||||
fs|beautifulsoup4-4.11.1-py310h06a4308_0.conda|1675464564|192154||||
fs|ca-certificates-2023.01.10-h06a4308_0.conda|1675456864|122761||||
fs|certifi-2022.12.7-py310h06a4308_0.conda|1675456864|153593||||
fs|cffi-1.15.1-py310h5eee18b_3.conda|1675456866|248975||||
fs|chardet-4.0.0-py310h06a4308_1003.conda|1675464563|206485||||
fs|conda-23.1.0-py310h06a4308_0.conda|1675456864|976062||||
fs|conda-build-3.23.3-py310h06a4308_0.conda|1675464563|593969||||
fs|conda-package-handling-2.0.2-py310h06a4308_0.conda|1675456864|273679||||
```

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

Favor `python -m conda_index` over `conda index`, because `conda index` may
resolve to the legacy `conda-build index` instead, which is deprecated by
this module.

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
