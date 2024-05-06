"""
Simple json to jlap "*/repodata.json" -> "*/repodata.jlap tool.

Copy */repodata.json to */.cache/repodata.json.last

Read */repodata.jlap

Diff */repodata.json with */.cache/repodata.json

Write */repodata.jlap

Same for current_repodata.jlap

If output jlap is larger than a set size, remove older diffs.
"""

from __future__ import annotations

import itertools
import json
import logging
import shutil
from hashlib import blake2b
from io import IOBase
from pathlib import Path

import click
import jsonpatch
from conda.gateways.repodata.jlap.core import JLAP, DEFAULT_IV

log = logging.getLogger("__name__")

# attempt to control individual patch size (will fall back to re-downloading
# repodata.json) without serializing to bytes just to measure
PATCH_STEPS_LIMIT = 8192


DIGEST_SIZE = 32


def trim_if_larger(*args):
    raise NotImplementedError()


def hfunc(data: bytes):
    return blake2b(data, digest_size=DIGEST_SIZE)


class HashReader:
    """
    Hash a file while it is being read.
    """

    def __init__(self, fp: IOBase):
        self.fp = fp
        self.hash = blake2b(digest_size=DIGEST_SIZE)

    def read(self, bytes=None):
        data = self.fp.read(bytes)
        self.hash.update(data)
        return data


def hash_and_load(path):
    with path.open("rb") as fp:
        h = HashReader(fp)
        obj = json.load(h)
    return obj, h.hash.digest()


def json2jlap_one(cache: Path, repodata: Path):
    previous_repodata = cache / (repodata.name + ".last")

    jlapfile = (repodata.parent / repodata.name).with_suffix(".jlap")
    if jlapfile.exists():
        patchfile = JLAP.from_path(jlapfile)
        # omit final metadata line
        patches = patchfile.body
    else:
        patches = JLAP.from_lines(
            [DEFAULT_IV.hex().encode("utf-8")] * 2, iv=DEFAULT_IV, verify=True
        )

    if (
        previous_repodata.exists()
        and repodata.stat().st_mtime_ns > previous_repodata.stat().st_mtime_ns
    ):
        current, current_digest = hash_and_load(repodata)
        previous, previous_digest = hash_and_load(previous_repodata)

        jpatch = jsonpatch.make_patch(previous, current)

        # inconvenient to add bytes size limit here; limit number of steps?
        if previous_digest == current_digest:
            log.warn("Skip identical %s", repodata)
        elif len(jpatch.patch) > PATCH_STEPS_LIMIT:
            log.warn("Skip large %s-step patch", len(jpatch.patch))
        else:
            patches.add(
                json.dumps(
                    {
                        "to": current_digest.hex(),
                        "from": previous_digest.hex(),
                        "patch": jpatch.patch,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
            )

        # metadata
        patches.add(
            json.dumps(
                {"url": repodata.name, "latest": current_digest.hex()},
                sort_keys=True,
                separators=(",", ":"),
            )
        )

        patches.terminate()
        patches.write(jlapfile)

    if (
        not previous_repodata.exists()
        or repodata.stat().st_mtime > previous_repodata.stat().st_mtime
    ):
        shutil.copyfile(repodata, previous_repodata)


@click.command()
@click.option("--cache", required=True, help="Cache directory.")
@click.option("--repodata", required=True, help="Repodata directory.")
@click.option(
    "--trim-low",
    required=False,
    default=2**20 * 3,
    show_default=True,
    help="Maximum size after trim.",
)
@click.option(
    "--trim-high",
    required=False,
    default=0,
    show_default=True,
    help="Trim if larger than size; 0 to disable.",
)
def json2jlap(cache, repodata, trim_low, trim_high):
    cache = Path(cache).expanduser()
    repodata = Path(repodata).expanduser()
    repodatas = itertools.chain(
        repodata.glob("*/repodata.json"), repodata.glob("*/current_repodata.json")
    )
    for repodata in repodatas:
        # require conda-index's .cache folder
        cachedir = Path(cache, repodata.parent.name, ".cache")
        if not cachedir.is_dir():
            continue
        json2jlap_one(cachedir, repodata)
        if trim_high > trim_low:
            repodata_jlap = repodata.with_suffix(".jlap")
            if not repodata_jlap.exists():
                continue
            trim_if_larger(trim_high, trim_low, repodata_jlap)


def go():
    logging.basicConfig(
        format="%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        level=logging.INFO,
    )
    json2jlap()


if __name__ == "__main__":
    go()
