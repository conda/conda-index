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
libcurl-7.88.1-h9049daf_0.conda|{"build":"h9049daf_0","build_number":0,"depends":["krb5 >=1.20.1,<1.21.0a0","libnghttp2 >=1.51.0,<2.0a0","libssh2 >=1.10.0,<2.0a0","libzlib >=1.2.13,<1.3.0a0","openssl >=3.0.8,<4.0a0"],"license":"curl","license_family":"MIT","name":"libcurl","subdir":"osx-arm64","timestamp":1676918523934,"version":"7.88.1","md5":"c86bbee944bb640609670ce722fba9a4","sha256":"37b8d58c05386ac55d1d8e196c90b92b0a63f3f1fe2fa916bf5ed3e1656d8e14","size":321706}
```

To track whether a package is indexed in the cache or not, conda-index uses a
table named `stat`. The main point of this table is to assign a stage value to
each artifact filename; usually `'fs'` which is called the `upstream` stage, and
`'indexed'`. `'fs'` means that the artifact is now available in the set of
packages (assumed by default to be the local filesystem). `'indexed'` means that
the entry already exists in the database (same filename, same timestamp, same
hash), and its package metadata has been extracted to the `index_json` etc.
tables. Paths in `'fs'` but not in `'indexed'` need to be unpacked to have their
metadata added to the database. Paths in `'indexed'` but not in `'fs'` will be
ignored and left out of `repodata.json`.

First, conda-index adds all files in a subdir to the `upstream` stage. This
involves a `listdir()` and `stat()` for each file in the index. The default
`upstream` stage is named `fs`, but this step is designed to be overridden by
subclassing `CondaIndexCache()` and replacing the `save_fs_state()` and
`changed_packages()` methods. By overriding `CondexIndexCache()` it is possible
to index without calling `stat()` on each package, or without even having all
packages stored on the indexing machine.

Next, conda-index looks for all `changed_packages()`: paths in the `upstream`
(`fs`) stage that don't exist in or have a different  modification time than
those in thie `indexed` stage.

Finally, a join between the `upstream` stage, usually `'fs'`, and the
`index_json` table yields a basic `repodata_from_packages.json` without any
repodata patches.

```sql
SELECT path, index_json FROM stat JOIN index_json USING (path) WHERE stat.stage = :upstream_stage
```

The steps to create `repodata.json`, including any repodata patches, and to
create `current_repodata.json` with only the latest versions of each package,
are similar to pre-sqlite3 conda-index.

The other cached metadata tables are used to create `channeldata.json`.
