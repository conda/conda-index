"""
Sharded repodata.
"""

import hashlib
import itertools
import json
import logging
from pathlib import Path
from typing import Any

import msgpack
import zstandard

from conda_index.index.sqlitecache import CondaIndexCache

from . import (
    CONDA_PACKAGE_EXTENSIONS,
    REPODATA_VERSION,
    RUN_EXPORTS_JSON_FN,
    ChannelIndex,
    _apply_instructions,
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
        Yield (package name, all packages with that name) from database ordered
        by name, path i.o.w. filename.

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

            if not desired or name in desired:
                yield (name, shard)


class ChannelIndexShards(ChannelIndex):
    """
    Sharded repodata per CEP proposal.
    """

    def __init__(
        self, *args, save_fs_state=False, cache_class=ShardedIndexCache, **kwargs
    ):
        """
        :param cache_only: Generate repodata based on what's in the cache,
            without using os.listdir() to check that those packages still exist
            on disk.
        """
        super().__init__(
            *args, cache_class=cache_class, save_fs_state=save_fs_state, **kwargs
        )

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

        print(len(shards_from_packages["shards"]))

        log.info("%s Writing pre-patch shards", subdir)
        unpatched_path = self.channel_root / subdir / "repodata_shards.msgpack.zst"
        self._maybe_write(
            unpatched_path, zstandard.compress(packb_typed(shards_from_packages))
        )  # type: ignore

        # Apply patch instructions.
        log.info("%s Applying patch instructions", subdir)
        patched_repodata, _ = self._patch_repodata_shards(
            subdir, shards_from_packages, patch_generator
        )

        # Save patched and augmented repodata. If the contents
        # of repodata have changed, write a new repodata.json.
        # Create associated index.html.

        log.info("%s Writing patched repodata", subdir)
        # XXX use final names, write patched repodata shards index
        for pkg, record in patched_repodata.items():
            Path(self.output_root, subdir, f"{pkg}.msgpack").write_bytes(
                packb_typed(record)
            )

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

        (self.output_root / subdir).mkdir(parents=True, exist_ok=True)

        # yield shards and combine tiny ones?

        SMALL_SHARD = 1024  # if a shard is this small, it is a candidate for merge
        MERGE_SHARD = 4096  # if the merged shards are bigger than this then spit them out
        def merged_shards():
            """
            If a shard would be tiny, combine it with a few neighboring shards.
            """
            collected = {}
            for name, shard in cache.index_shards():
                shard_size = len(packb_typed(shard))
                if shard_size > SMALL_SHARD:
                    if collected:
                        yield collected
                    yield {name: shard}

                collected[name] = shard


        for name, shard in cache.index_shards():
            shard_data = packb_typed(shard)
            reference_hash = hashlib.sha256(shard_data).hexdigest()
            output_path = self.output_root / subdir / f"{reference_hash}.msgpack.zst"
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
                # more difficult if some shards are duplicated...
                for pkg, reference in repodata_shards["shards"].items():
                    # XXX keep it all in RAM? only patch changed shards or, if patches change, all shards?
                    shard_path = (
                        self.output_root / subdir / f"{reference.hex()}.msgpack.zst"
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
            # XXX refactor
            # otherwise _apply_instructions would repeat this work
            new_pkg_fixes = {
                k.replace(".tar.bz2", ".conda"): v
                for k, v in instructions.get("packages", {}).items()
            }

            import time

            begin = time.time()

            for i, (pkg, reference) in enumerate(repodata_shards["shards"].items()):
                shard_path = (
                    self.output_root / subdir / f"{reference.hex()}.msgpack.zst"
                )
                shard = msgpack.loads(zstandard.decompress(shard_path.read_bytes()))
                if (now := time.time()) - begin > 1:
                    print(pkg)
                    begin = now

                yield (
                    pkg,
                    _apply_instructions(
                        subdir, shard, instructions, new_pkg_fixes=new_pkg_fixes
                    ),
                )

        return dict(per_shard_apply_instructions()), instructions
