# PostgreSQL Support in conda-index

As of `conda-index 0.7.0`, `conda-index` can use a PostgreSQL database.
`conda-index` uses a database to store package metadata, creating repodata from
a query. By default it will use a sqlite3 database stored alongside the package
files, but it can optionally use PostgreSQL.

The database backend is controlled by the `--db <backend>` and `--db-url`
command line arguments, or the `CONDA_INDEX_DBURL` environment variable replaces
`--db-url`. For example, `python -m conda_index --db postgresql` chooses
PostgreSQL with the default `postgresql:///conda_index` database URL.

Install `conda-index`'s PostgreSQL-specific dependencies into its environment.
```
conda install sqlalchemy psycopg2
```

Install a local PostgreSQL with conda.
```
# Create a local PostgreSQL installation and conda_index database
conda install postgresql
initdb -D conda-index-db
pg_ctl -D conda-index-db -l logfile start
createdb conda_index
```

Now run `python -m conda_index --db postgresql --db-url postgresql:///conda_index [DIR]`

`conda_index` stores package metadata in the PostgreSQL database given by a
[SQLAlchemy database
URL](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls).

[The
schema](https://github.com/conda/conda-index/blob/main/conda_index/postgres/model.py)
is similar to the one used for sqlite3, except that while sqlite3 uses a
database file per subdirectory, in PostgreSQL all subdirectories are stored in
the same database. `conda_index` creates a random prefix in
`[DIR]/cache/cache.json` to differentiate this channel from any others that may
be stored in the same PostgreSQL database. Each package name is stored as e.g.
`<prefix>/<subdir>/<package>.conda` in a single database.

Advanced users can use the CLI or the API to run `conda_index` on a partial
local package repository. It is possible to add a few local packages to a much
larger index instead of keeping every package on the machine running
`conda-index`. For example, by inserting packages into the `stat` table and then
running `python -m conda_index --db postgresql --no-update-cache [DIR]`,
`conda-index` can add or update packages in `[DIR]` to repodata without
necessarily storing either the entire set of packages or the `conda-index`
database on that machine.
