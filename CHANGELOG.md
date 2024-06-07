[//]: # (current developments)

## 0.5.0 (2024-06-07)

### Enhancements

* Add experimental `python -m conda_index.json2jlap` script to run after
  indexing, to create `repodata.jlap` patch sets for incremental repodata
  downloads. (#125)
* Add support for CEP-15 ``base_url`` to host packages separate from repodata.
  (#150)
* Support fsspec (in the API only) to index any fsspec-supported remote
  filesystem. Also enables the input packages folder to be separate from the
  cache and output folders. (#143)

### Bug fixes

* Move `run_exports.json` query into cache, instead of directly using SQL in
  `ChannelIndex`. (#163)
* Create parents of `<subdir>/.cache` in `CondaIndexCache`. (#170)

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
