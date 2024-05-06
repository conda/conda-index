"""
Test companion json2jlap script, that compares repodata.json with
.cache/repodata.json.last and generates companion patchsets repodata.jlap. This
is generic on json.
"""

import json

from conda_index.cli.json2jlap import json2jlap_one


def test_json2jlap(tmp_path):
    cache_dir = tmp_path / "subdir" / ".cache"
    repodata = tmp_path / "subdir" / "repodata.json"
    jlap_path = repodata.with_suffix(".jlap")
    cache_dir.mkdir(parents=True)
    for n in range(4):
        repodata.write_text(json.dumps({"n": n}))

        json2jlap_one(cache_dir, repodata)

    assert jlap_path.exists()
