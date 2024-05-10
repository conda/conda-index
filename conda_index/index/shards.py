"""
Sharded repodata.
"""

import functools
import hashlib
import itertools
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from os.path import join
from pathlib import Path
from typing import Any

import msgpack
import zstandard

from conda_index.index.sqlitecache import CondaIndexCache

from .. import utils, yaml
from . import (
    CONDA_PACKAGE_EXTENSIONS,
    REPODATA_VERSION,
    RUN_EXPORTS_JSON_FN,
    ChannelIndex,
    _apply_instructions,
    _ensure_valid_channel,
)

log = logging.getLogger(__name__)


def pack_record(record):
    """
    Convert hex checksums to bytes.
    """
    if sha256 := record.get("sha256"):
        record["sha256"] = bytes.fromhex(sha256)
    if md5 := record.get("md5"):
        record["md5"] = bytes.fromhex(md5)
    return record


def packb_typed(o: Any) -> bytes:
    """
    Sidestep lack of typing in msgpack.
    """
    return msgpack.packb(o)  # type: ignore


class ShardedIndexCache(CondaIndexCache):
    def index_shards(self, desired: set | None = None):
        """
        Yield (package name, shard data) for package names in database ordered
        by name, path i.e. filename.

        :desired: If not None, set of desired package names.
        """
        for name, rows in itertools.groupby(
            self.db.execute(
                """SELECT index_json.name, path, index_json
                FROM stat JOIN index_json USING (path) WHERE stat.stage = ?
                ORDER BY index_json.name, index_json.path""",
                (self.upstream_stage,),
            ),
            lambda k: k[0],
        ):
            shard = {"packages": {}, "packages.conda": {}}
            for row in rows:
                name, path, index_json = row
                if not path.endswith((".tar.bz2", ".conda")):
                    log.warn("%s doesn't look like a conda package", path)
                    continue
                record = json.loads(index_json)
                key = "packages" if path.endswith(".tar.bz2") else "packages.conda"
                # we may have to pack later for patch functions that look for
                # hex hashes
                shard[key][path] = pack_record(record)

            yield (name, shard)


class ChannelIndexShards(ChannelIndex):
    """
    Sharded repodata per CEP proposal.
    """

    def __init__(self, *args, cache_class=ShardedIndexCache, **kwargs):
        super().__init__(*args, cache_class=cache_class, **kwargs)

    def index(
        self,
        patch_generator,
        verbose=False,
        progress=False,
        current_index_versions=None,
    ):
        """
        Re-index all changed packages under ``self.channel_root``.
        """

        subdirs = self.detect_subdirs()

        # Lock local channel.
        with utils.try_acquire_locks([utils.get_lock(self.channel_root)], timeout=900):
            # begin non-stop "extract packages into cache";
            # extract_subdir_to_cache manages subprocesses. Keeps cores busy
            # during write/patch/update channeldata steps.
            def extract_subdirs_to_cache():
                executor = ThreadPoolExecutor(max_workers=1)

                def extract_args():
                    for subdir in subdirs:
                        # .cache is currently in channel_root not output_root
                        _ensure_valid_channel(self.channel_root, subdir)
                        subdir_path = join(self.channel_root, subdir)
                        yield (subdir, verbose, progress, subdir_path)

                def extract_wrapper(args: tuple):
                    # runs in thread
                    subdir, verbose, progress, subdir_path = args
                    cache = self.cache_for_subdir(subdir)
                    return self.extract_subdir_to_cache(
                        subdir, verbose, progress, subdir_path, cache
                    )

                # map() gives results in order passed, not in order of
                # completion. If using multiple threads, switch to
                # submit() / as_completed().
                return executor.map(extract_wrapper, extract_args())

            # Collect repodata from packages, save to
            # REPODATA_FROM_PKGS_JSON_FN file
            with self.thread_executor_factory() as index_process:
                futures = [
                    index_process.submit(
                        functools.partial(
                            self.index_prepared_subdir,
                            subdir=subdir,
                            verbose=verbose,
                            progress=progress,
                            patch_generator=patch_generator,
                            current_index_versions=current_index_versions,
                        )
                    )
                    for subdir in extract_subdirs_to_cache()
                ]
                # limited API to support DummyExecutor
                for future in futures:
                    result = future.result()
                    log.info(f"Completed {result}")

    def index_prepared_subdir(
        self,
        subdir: str,
        verbose: bool,
        progress: bool,
        patch_generator,
        current_index_versions,
    ):
        """
        Create repodata_from_packages, then apply any patches to create repodata.json.
        """
        log.info("Subdir: %s Gathering repodata", subdir)

        shards_from_packages = self.index_subdir(
            subdir, verbose=verbose, progress=progress
        )

        log.info("%s Writing pre-patch shards", subdir)
        unpatched_path = self.channel_root / subdir / "repodata_shards.msgpack.zst"
        self._maybe_write(
            unpatched_path, zstandard.compress(packb_typed(shards_from_packages))
        )  # type: ignore

        # Apply patch instructions.
        log.info("%s Applying patch instructions", subdir)
        patched_repodata, _ = self._patch_repodata(
            subdir, shards_from_packages, patch_generator
        )

        # Save patched and augmented repodata. If the contents
        # of repodata have changed, write a new repodata.json.
        # Create associated index.html.

        log.info("%s Writing patched repodata", subdir)

        pass  # XXX

        log.info("%s Building current_repodata subset", subdir)

        log.debug("%s no current_repodata", subdir)

        if self.write_run_exports:
            log.info("%s Building run_exports data", subdir)
            run_exports_data = self.build_run_exports_data(subdir)

            log.info("%s Writing run_exports.json", subdir)
            self._write_repodata(
                subdir,
                run_exports_data,
                json_filename=RUN_EXPORTS_JSON_FN,
            )

        log.info("%s skip index HTML", subdir)

        log.debug("%s finish", subdir)

        return subdir

    def index_subdir(self, subdir, verbose=False, progress=False):
        """
        Return repodata from the cache without reading old repodata.json

        Must call `extract_subdir_to_cache()` first or will be outdated.
        """

        cache: ShardedIndexCache = self.cache_for_subdir(subdir)  # type: ignore

        log.debug("Building repodata for %s/%s", self.channel_name, subdir)

        shards = {}

        shards_index = {
            "info": {
                "subdir": subdir,
            },
            "repodata_version": REPODATA_VERSION,
            "removed": [],  # can be added by patch/hotfix process
            "shards": shards,
        }

        if self.base_url:
            # per https://github.com/conda-incubator/ceps/blob/main/cep-15.md
            shards_index["info"]["base_url"] = f"{self.base_url.rstrip('/')}/{subdir}/"
            shards_index["repodata_version"] = 2

        # Higher compression levels are a waste of time for tiny gains on this
        # collection of small objects.
        compressor = zstandard.ZstdCompressor()

        for name, shard in cache.index_shards():
            shard_data = bytes(packb_typed(shard))
            reference_hash = hashlib.sha256(shard_data).hexdigest()
            output_path = self.channel_root / subdir / f"{reference_hash}.msgpack.zst"
            if not output_path.exists():
                output_path.write_bytes(compressor.compress(shard_data))

            # XXX associate hashes of compressed and uncompressed shards
            shards[name] = bytes.fromhex(reference_hash)

        return shards_index

    def _patch_repodata_shards(
        self, subdir, repodata_shards, patch_generator: str | None = None
    ):
        # XXX see how broken patch instructions are when applied per-shard

        instructions = {}

        if patch_generator and patch_generator.endswith(CONDA_PACKAGE_EXTENSIONS):
            instructions = self._load_patch_instructions_tarball(
                subdir, patch_generator
            )
        else:

            def per_shard_instructions():
                for pkg, reference in repodata_shards["shards"].items():
                    shard_path = (
                        self.channel_root / subdir / f"{reference.hex()}.msgpack.zst"
                    )
                    shard = msgpack.loads(zstandard.decompress(shard_path.read_bytes()))
                    yield (
                        pkg,
                        self._create_patch_instructions(subdir, shard, patch_generator),
                    )

            instructions = dict(per_shard_instructions())

        if instructions:
            self._write_patch_instructions(subdir, instructions)
        else:
            instructions = self._load_instructions(subdir)

        if instructions.get("patch_instructions_version", 0) > 1:
            raise RuntimeError("Incompatible patch instructions version")

        def per_shard_apply_instructions():
            for pkg, reference in repodata_shards["shards"].items():
                shard_path = (
                    self.channel_root / subdir / f"{reference.hex()}.msgpack.zst"
                )
                shard = msgpack.loads(zstandard.decompress(shard_path.read_bytes()))
                yield (pkg, _apply_instructions(subdir, shard, instructions))

        return dict(per_shard_apply_instructions()), instructions


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    rss = False
    channeldata = False
    current_index_versions_file = None
    patch_generator = None
    dir = Path(__file__).parents[2] / "tests" / "index_data" / "packages"
    output = dir.parent / "shards"
    assert dir.exists(), dir
    channel_index = ChannelIndexShards(
        dir.expanduser(),
        channel_name=dir.name,
        output_root=output,
        subdirs=None,
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=True,
        compact_json=True,
        base_url=None,
    )

    current_index_versions = None
    if current_index_versions_file:
        with open(current_index_versions_file) as f:
            current_index_versions = yaml.safe_load(f)

    channel_index.index(
        patch_generator=patch_generator,  # or will use outdated .py patch functions
        current_index_versions=current_index_versions,
        progress=False,  # clone is a batch job
    )

    if channeldata:  # about 2 1/2 minutes for conda-forge
        channel_index.update_channeldata(rss=rss)
