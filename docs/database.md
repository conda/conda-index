# Database schema

Standalone conda-index uses a per-subdir sqlite database to track package
metadata, unlike the older version which used millions of tiny `.json` files.
The new strategy is much faster because we don't have to pay for many individual
`stat()` or `open()` calls.

The whole schema looks like this:

```sql
<subdir>/.cache % sqlite3 cache.db
SQLite version 3.41.2 2023-03-22 11:56:21
Enter ".help" for usage hints.
sqlite> .schema
CREATE TABLE about (path TEXT PRIMARY KEY, about BLOB);
CREATE TABLE index_json (path TEXT PRIMARY KEY, index_json BLOB);
CREATE TABLE recipe (path TEXT PRIMARY KEY, recipe BLOB);
CREATE TABLE recipe_log (path TEXT PRIMARY KEY, recipe_log BLOB);
CREATE TABLE run_exports (path TEXT PRIMARY KEY, run_exports BLOB);
CREATE TABLE post_install (path TEXT PRIMARY KEY, post_install BLOB);
CREATE TABLE icon (path TEXT PRIMARY KEY, icon_png BLOB);
CREATE TABLE stat (
                stage TEXT NOT NULL DEFAULT 'indexed',
                path TEXT NOT NULL,
                mtime NUMBER,
                size INTEGER,
                sha256 TEXT,
                md5 TEXT,
                last_modified TEXT,
                etag TEXT
            );
CREATE UNIQUE INDEX idx_stat ON stat (path, stage);
CREATE INDEX idx_stat_stage ON stat (stage, path);
```

```sql
sqlite> select stage, path from stat where path like 'libcurl%';
fs|libcurl-7.84.0-hc6d1d07_0.conda
fs|libcurl-7.86.0-h0f1d93c_0.conda
fs|libcurl-7.87.0-h0f1d93c_0.conda
fs|libcurl-7.88.1-h0f1d93c_0.conda
fs|libcurl-7.88.1-h9049daf_0.conda
indexed|libcurl-7.84.0-hc6d1d07_0.conda
indexed|libcurl-7.86.0-h0f1d93c_0.conda
indexed|libcurl-7.87.0-h0f1d93c_0.conda
indexed|libcurl-7.88.1-h0f1d93c_0.conda
indexed|libcurl-7.88.1-h9049daf_0.conda
```

Most of these tables store json-format metadata extracted from each package.

```sql
select * from index_json where path = 'libcurl-7.88.1-h9049daf_0.conda';
'libcurl-7.88.1-h9049daf_0.conda'
'{"build":"h9049daf_0",...,"sha256":"37b8d58c05386ac55d1d8e196c90b92b0a63f3f1fe2fa916bf5ed3e1656d8e14","size":321706}'
```

## Sample queries

Megabytes added per day:

```sql
select
  date(mtime, 'unixepoch') as d,
  printf('%0.2f', sum(size) / 1e6) as MB
from
  stat
group by
  date(mtime, 'unixepoch')
order by
  mtime desc
```