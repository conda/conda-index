"""
Test companion json2jlap script, that compares repodata.json with
.cache/repodata.json.last and generates companion patchsets repodata.jlap. This
is generic on json.
"""

import json

from conda_index.cli.json2jlap import json2jlap_one
from conda.gateways.repodata.jlap.core import JLAP, DEFAULT_IV


def test_json2jlap(tmp_path):
    cache_dir = tmp_path / "subdir" / ".cache"
    repodata = tmp_path / "subdir" / "repodata.json"
    jlap_path = repodata.with_suffix(".jlap")
    cache_dir.mkdir(parents=True)
    for n in range(4):
        repodata.write_text(json.dumps({"n": n}))

        json2jlap_one(cache_dir, repodata)

    assert jlap_path.exists()
    jlap = JLAP.from_path(jlap_path)
    lines = len(jlap)
    for i, (_, b, _) in enumerate(jlap):
        if i == 0:
            assert b == DEFAULT_IV.hex()
        elif i == lines - 1:
            assert len(b) == 64
            assert int(b, 16)  # should succeed
        else:
            json.loads(b)  # should succeed
