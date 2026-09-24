# conda-index

`conda-index` creates conda channels from collections of conda packages. It
extracts package metadata and writes the `repodata.json` and channel metadata
that conda uses for dependency solving.

Place packages in the matching platform subdirectories of a channel directory,
including `noarch`, then create the index:

```console
$ conda index <path-to-channel>
```

## Explore the documentation

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} {octicon}`terminal` Command-line interface
:link: cli
:link-type: doc

Run `conda index` or `python -m conda_index` and look up every option.
:::

:::{grid-item-card} {octicon}`workflow` How indexing works
:link: theory_of_operation
:link-type: doc

Follow package discovery, metadata caching, and repodata generation.
:::

:::{grid-item-card} {octicon}`database` Cache database
:link: database
:link-type: doc

Inspect the SQLite metadata cache schema and sample queries.
:::

:::{grid-item-card} {octicon}`server` PostgreSQL
:link: postgresql
:link-type: doc

Configure a shared PostgreSQL metadata database.
:::

:::{grid-item-card} {octicon}`code` Python API
:link: modules
:link-type: doc

Use `update_index`, `ChannelIndex`, and the filesystem abstraction.
:::

:::{grid-item-card} {octicon}`log` Changelog
:link: changelog
:link-type: doc

See what changed in each conda-index release.
:::

::::

## History

`conda-index` was extracted from `conda-build` and largely rewritten. A summary of changes from the `conda-build index` version:

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

* Reformat code.

```{toctree}
:hidden:
:caption: How-to guides

postgresql
```

```{toctree}
:hidden:
:caption: Reference

cli
database
v3-repodata
modules
```

```{toctree}
:hidden:
:caption: Explanation

theory_of_operation
```

```{toctree}
:hidden:
:caption: Project

changelog
```
