[//]: # (current developments)

## 0.7.0 (2025-10-13)

### Enhancements

* Add postgresql as a supported database backend in addition to sqlite (#199)
* Show error when --no-write-monolithic is combined with --current-repodata, --run-exports, or --channeldata (#224)
* Add html title popup with dependencies for each build to index.html (#205), thanks @jtroe
* "--html-dependencies/--no-html-dependencies" flag toggles popups. (#218)

### Docs

* Include narrative documentation for `python -m conda_index --db postgresql ...` in Sphinx (https://conda.github.io/conda-index/) (#219)

### Other

* Update "conda index" command plugin to avoid re-exported type (#227)

### Contributors

* @dholth
* @jtroe
* @ryanskeith



## 0.6.1 (2025-05-22)

### Enhancements

* Added support for Python 3.13 in the CI test matrix and updated related
  configurations. (#203)

### Bug fixes

* In sharded repodata, set `base_url` and `shards_base_url` to `""` instead of
  leaving them undefined, for pixi compatibility. (#209)

### Other

* Add database-independent base class for (sqlite specific) CondaIndexCache.
  Return parsed data instead of str in `run_exports()`. (#206)
* Update sqlite3 create_function() arguments for "positional-only in Python
  3.15" warning. (#211)


## 0.6.0 (2025-03-27)

### Enhancements

* Add `--channeldata/--no-channeldata` flag to toggle generating channeldata.
* Add sharded repodata (repodata split into separate files per package name).

### Other

* Remove [WAL mode](https://www.sqlite.org/wal.html) from database create
  script, in case `conda-index` is used on a network file system. Note WAL mode
  is persistent, `PRAGMA journal_mode=DELETE` can be used to convert a WAL
  database back to a rollback journal mode. (#177)
* Separate current_repodata generation into own file, raising
  possibility of "doesn't depend on conda" mode.
* Update tests to account for conda-build removals. (#180)
* Publish new `conda-index` releases on PyPI automatically. (#195)

See also https://github.com/conda/conda-index/releases/tag/0.6.0

## 0.5.0 (2024-06-07)

### Enhancements

* Add experimental `python -m conda_index.json2jlap` script to run after
  indexing, to create `repodata.jlap` patch sets for incremental repodata
  downloads. (#125)
* Add `--current-repodata/--no-current-repodata` flags to control whether
  `current_repodata.json` is generated. (#139)
* Add support for CEP-15 ``base_url`` to host packages separate from repodata.
  (#150)
* Support fsspec (in the API only) to index any fsspec-supported remote
  filesystem. Also enables the input packages folder to be separate from the
  cache and output folders. (#143)

### Bug fixes

* Move `run_exports.json` query into cache, instead of directly using SQL in
  `ChannelIndex`. (#163)
* Create parents when creating `<subdir>/.cache` (#166)

### Other

* Approach 100% code coverage in test suite; reformat with ruff. (#145)
* Update CI configuration to test on more platforms (#142)
* Drop support for Python 3.7; support Python 3.8+ only. (#130)

### Contributors

* @dholth
* @jezdez
* @conda-bot



## 0.4.0 (2024-01-29)

### Enhancements

* Add --compact-json/--no-compact-json option, default to compact. (#120)
* Add an `index` subcommand using conda's new subcommand plugin hook, allowing
  `conda index` instead of `python -m conda_index`. Note the [CLI has
  changed](https://conda.github.io/conda-index/cli.html) compared to old
  `conda-index`. When `conda-build < 24.1.0` is installed, the older
  `conda-index` code will still be used instead of this plugin. (#81 via #131)

### Bug fixes

* Check size in addition to mtime when deciding which packages to
  index. (#108)
* Update cached index.json, not just stat values, for
  changed packages that are already indexed. (#108)

### Other

* Improve test coverage (#123)
* Apply `ruff --fix`; reformat code; syntax cleanup (#128)

## 0.3.0 (2023-09-21)

### Enhancements

* Add `--run-exports` to generate CEP-12 compliant `run_exports.json` documents
  for each subdir. (#102 via #110)
* Don't pretty-print `repodata.json` by default, saving time and space. (#111)

### Docs

* Improve documentation.

### Deprecations

* Require conda >= 4.14 (or any of the >= 22.x.y calver releases)
