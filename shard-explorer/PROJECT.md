Build an attractive, interactive HTML page intended to replace conda-index's
static `index.html`, which currently renders a full package table and can be
slow for large channels.

Create a single explorer page at `output/index.html`.

The page must browse sharded repodata as defined in `cep-0016.md` and support:

1. **Channel source input**
   - Allow the user to enter a full channel URL, including subdir URLs such as
     `https://prefix.dev/conda-forge/linux-64/`.
   - Assume the target server supports CORS and load data directly from that
     URL.
   - If a subdir is included in the input URL, detect and use it.

2. **Subdir selection**
   - Provide a control to select which subdir to browse (for example
     `linux-64`, `noarch`, etc.).
   - Resolve and load `repodata_shards.msgpack.zst` for the selected subdir.

3. **Shard index browsing**
   - Load package names from `repodata_shards.msgpack.zst`.
   - Render package names efficiently and provide search/filter.
   - Allow the user to render `repodata_shards.msgpack.zst` as JSON.

4. **Package drill-down**
   - Allow selecting a package name to show all available versions/files for
     that package by loading its shard(s).

5. **Record detail drill-down**
   - For a selected filename, highlight key package information aligned with
     `cep-0034.md`.
   - Allow expanding to the full package record rendered as JSON.

Implementation notes:

- Use plain HTML, CSS, and vanilla JavaScript.
- Keep the interface visually polished and readable for large datasets.
- Preserve responsiveness and usability on desktop and mobile.
