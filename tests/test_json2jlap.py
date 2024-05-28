"""
Test companion json2jlap script, that compares repodata.json with
.cache/repodata.json.last and generates companion patchsets repodata.jlap. This
is generic on json.
"""

import json

import pytest
from conda.gateways.repodata.jlap.core import DEFAULT_IV, JLAP

from conda_index.json2jlap import json2jlap_one


def test_json2jlap(tmp_path):
    """
    Test basic operation of the patch generator.
    """
    cache_dir = tmp_path / "subdir" / ".cache"
    repodata = tmp_path / "subdir" / "repodata.json"
    jlap_path = repodata.with_suffix(".jlap")
    cache_dir.mkdir(parents=True)
    for n in range(4):
        # change size to avoid testing filesystem timestamp resolution
        repodata.write_text(json.dumps({"n": "n" * n}))

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


@pytest.mark.parametrize("trim_high,trim_low", [[1500, 100], [8192, 1024]])
def test_json2jlap_trim(tmp_path, trim_high, trim_low):
    """
    Test that we can correctly trim jlap when they become too large, so that the
    patchset is still more convenient than re-dowloading the complete file.

    Test against unreasonably small sizes to make sure we don't produce
    degenerate output, and against at-least-preserves-a-few-lines-of-patches
    sizes.

    In practice we've chosen low and high values of 3MB / 10MB.
    """
    cache_dir = tmp_path / "subdir" / ".cache"
    repodata = tmp_path / "subdir" / "repodata.json"
    jlap_path = repodata.with_suffix(".jlap")
    cache_dir.mkdir(parents=True)

    text = "spam" * 32
    grows = {}
    jlap_sizes = []
    for n in range(64):
        grows[f"n{n}"] = text
        repodata.write_text(json.dumps(grows))

        # this will cause it to be trimmed with checksums only, no footer
        json2jlap_one(cache_dir, repodata, trim_high=trim_high, trim_low=trim_low)

        try:
            jlap_sizes.append(jlap_path.stat().st_size)
        except FileNotFoundError:
            assert n == 0

    assert jlap_path.exists()
    jlap = JLAP.from_path(jlap_path)
    lines = len(jlap)
    for i, (_, b, _) in enumerate(jlap):
        if i in (0, lines - 1):
            # trimmed jlap no longer starts with 0's, instead, starts with an
            # intermediate hash of the longer jlap that we now only see parts
            # of.
            assert b != DEFAULT_IV.hex()
            assert len(b) == 64
            assert int(b, 16)  # should succeed
        else:
            json.loads(b)  # should succeed
