# PostgreSQL Support in conda-index

As of `conda-index 0.7.0`, `conda-index` can use a PostgreSQL database.
`conda-index` uses a database to store package metadata. Once all the metadata
is stored, it creates repodata from a query. By default it uses a sqlite3
database stored alongside the package files, but it can optionally use
PostgreSQL.

The database backend is controlled by the `--db <backend>` and `--db-url`
command line arguments, or the `CONDA_INDEX_DBURL` environment variable replaces
`--db-url`. For example, `python -m conda_index --db postgresql` chooses
PostgreSQL with the default `postgresql:///conda_index` database URL.

To use a PostgreSQL database with `conda-index`, install `conda-index`'s PostgreSQL-specific dependencies into its environment:
```sh
conda install sqlalchemy psycopg2
```

Then, one way to get a PostgreSQL server is to install it with conda:
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
