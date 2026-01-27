# PostgreSQL Support in conda-index

As of `conda-index 0.7.0`, `conda-index` can use a PostgreSQL database.
`conda-index` uses a database to store package metadata, creating repodata from
a query. By default, it will use a sqlite3 database stored alongside the package
files, but it can optionally use PostgreSQL.

The database backend is controlled by the `--db <backend>` and `--db-url`
command line arguments, or the `CONDA_INDEX_DBURL` environment variable replaces
`--db-url`. For example, `python -m conda_index --db postgresql` chooses
PostgreSQL with the default `postgresql:///conda_index` database URL.

To use a PostgreSQL database with `conda-index`, install `conda-index`'s PostgreSQL-specific dependencies into its environment:
```sh
conda install sqlalchemy psycopg2
```

Then, install a local PostgreSQL with conda:
```sh
# Create a local PostgreSQL installation and conda_index database
conda install postgresql
initdb -D conda-index-db
pg_ctl -D conda-index-db -l logfile start
createdb conda_index
```

Finally, run the following command:
```sh
python -m conda_index --db postgresql --db-url postgresql:///conda_index [DIR]
```

`conda_index` stores package metadata in the PostgreSQL database given by a
[SQLAlchemy database
URL](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).

[The
schema](https://github.com/conda/conda-index/blob/main/conda_index/postgres/model.py)
is similar to the one [used for sqlite3](./database), except that while sqlite3
uses a database file per subdirectory, in PostgreSQL all subdirectories are
stored in the same database. `conda_index` creates a random prefix in
`[DIR]/.cache/cache.json` to differentiate this channel from any others that may
be stored in the same PostgreSQL database. Each package name is stored with the
format `<prefix>/<subdir>/<package>.conda` in a single database.

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

Additionally if conda-index is used this way to aggregate a large
`repoadata.json`, and `--update-only` is not used every time, then all packages
not present on the local system will be removed from the database and the output
`repodata.json`.

A future improvement could add flags to toggle each stage of conda-index
(populate list of packages; compare list of packages to indexed packages and
cache any changed metadata; output repodata by querying the database).
