# Theory of Operation

To track whether a package is indexed in the cache or not, conda-index uses a
table named `stat`, with a compound primary key (`stage`, `path`). Think of
packages moving from "upstream" to "downstream" by being duplicated in the
`stat` table for each stage.

The main stages are `'fs'` which is called the `upstream` stage, and
`'indexed'`. `'fs'` means that the artifact is on the filesystem. `'indexed'`
means that the entry already exists in the database (same filename, same
timestamp, same hash), and its package metadata has been extracted to the
`index_json` etc. tables. Paths in `'fs'` but not in `'indexed'` need to be
unpacked to have their metadata added to the database. Paths in `'indexed'` but
not in `'fs'` will be ignored and left out of `repodata.json`.

First, conda-index adds all files in a subdir to the upstream stage which
defaults to `fs`, so each package has an entry `('fs', path, mtime, size, ...)`.
This involves a `listdir()` and `stat()` for each file in the index.

Next, conda-index looks for all `changed_packages()`: paths in the `upstream`
(`fs`) stage that are either missing from or have a different size, mtime than
those in the `indexed` stage.

The `changed_packages()` are examined one by one, and their metadata is stored
as json in various tables in conda-index's database.

Finally, a join between the `upstream` stage, usually `'fs'`, and the
`index_json` table yields `repodata_from_packages.json` without any repodata
patches.

```sql
SELECT path, index_json 
FROM stat JOIN index_json
USING (path) 
WHERE stat.stage = :upstream_stage
```

The steps to create `repodata.json`, including any repodata patches, and to
create `current_repodata.json` with only the latest versions of each package,
are similar to pre-sqlite3 conda-index.

The other cached metadata tables are used to create `channeldata.json`.

# Advanced Techniques

Other techniques are possible, but generally require using the `conda-index` API
and are not fully available from the command line interface.

## "Metadata only" stage

Sometimes it is useful to create an index without unpacking real packages from
the local filesystem; for example, when translating `.whl` package metadata to
conda repodata. As of version `0.12.0`, `conda-index` adds a `md` or metadata
stage to support this mode. The `md` stage doesn't participate in
`changed_packages()` or `conda-index`'s package extraction pipeline. Instead,
the user inserts `stat` table entries and metadata into `conda-index`s database
either directly or by using `conda-index` APIs. Then, the output query is
changed to

```sql
SELECT path, index_json 
FROM stat JOIN index_json 
USING (path) 
WHERE stat.stage in ('fs', 'md')
```

When it's time to output repodata, packages that are in the `fs` or `md` stage,
and also have a row in `index_json`, are included.

## Other Techniques

It is possible to index without calling `stat()` on each package, or without
even having all packages stored on the indexing machine. This can be done by
subclassing `CondexIndexCache()` and replacing the `save_fs_state()` and
`changed_packages()` methods.

Advanced users can use the CLI or the API to run `conda_index` on a partial
local package repository. It is possible to add a few local packages to a much
larger index instead of keeping every package on the machine running
`conda-index`.

For example, by running `python -m conda_index --db postgresql --update-only
[DIR]`, `conda-index` will add or update packages in `[DIR]` to repodata, while
keeping already-indexed packages in the output `repodata.json`. The output
repodata can then be copied to a server that has every package.

If `--update-only` is used, the `stat` table must be altered to remove packages
from `repodata.json`, e.g. `DELETE FROM stat WHERE path =
'<prefix>/<subdir>/package.conda' AND stage = 'fs'`.

When using this option, care must be taken to never run `conda-index` without
`--update-only` or all the "missing" packages will be dropped from the index.
