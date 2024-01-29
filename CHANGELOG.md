## 0.4.0 (2024-01-29)

### Enhancements

* Add --compact-json/--no-compact-json option, default to compact. (#120)

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
