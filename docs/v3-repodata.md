# V3 Repodata

## Overview

[Repodata v3](https://github.com/conda/ceps/pull/146) is a proposal that
includes a top-level `v3` key and sub-levels named after package extensions
(`.tar.bz2`, `.conda`, `.whl`). `conda-index` can generate `v3` repodata by
using the `--repodata-next` flag:

```bash
conda index /path/to/channel --repodata-next
```

Or via the Python API:

```python
from conda_index.index import ChannelIndex

channel_index = ChannelIndex("/path/to/channel", "my_channel", repodata_v3=True)
channel_index.index(...)
```

## Structure

With v3 repodata enabled, the generated `repodata.json` has the following
structure:

```python
{
  "info": {
    "repodata_revisions": [
      {
        "n_packages": 1,
        "newest": 1758039171969,
        "oldest": 1758039171969,
        "revision": 3
      }
    ],
    "subdir": "noarch"
  },
  "packages": {},
  "packages.conda": {},
  "removed": [],
  "repodata_version": 1,
  "v3": {
    "conda": {
      "zstd-1.5.7-h817c040_0": {
        "build": "h817c040_0",
        "build_number": 0,
        "depends": [...],
        ...
      }
    },
    "tar.bz2": {},
    "whl": {}
  }
}
```

## Run-Exports in Shards (CEP 21)

When using sharded repodata (`--write-shards`), each shard includes
`run_exports` data for its packages. This implements [CEP
21](https://github.com/conda/ceps/blob/main/cep-0021.md). 

Run-exports describe which dependencies a package imposes on packages that link
against it. Including this information directly in the repodata shards helps
build tools to generate correct dependencies for new packages.

In sharded repodata, a `run_exports` field is included inside individual package
records, instead of collecting all `run_exports` in their own section or in a
separate file. 

Run-exports is always included in sharded repodata

### Shard Structure with Run-Exports

Each per-package shard (stored as `<hash>.msgpack.zst`) looks something like this:

```python
{
  "packages.conda": {
    "arrow-c-glib-23.0.1-heb0d9f2_0.conda": {
      "name": "arrow-c-glib",
      "build": "heb0d9f2_0",
      "build_number": 0,
      "version": "23.0.1"
      "depends": [...],
      ...,
      "run_exports": {
        "weak": [
          "arrow-c-glib >=23.0.1,<23.0.2.0a0"
        ]
      },
      ...
    }
  },
}
```

### Run-Exports JSON

A standalone `run_exports.json` file is also written per subdir when
the `--run-exports` flag is passed.