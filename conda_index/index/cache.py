"""
Abstract base class for CondaIndexCache, which stores and loads repository index
information.
"""

from __future__ import annotations

import abc
import fnmatch
import json
import logging
from numbers import Number
from pathlib import Path
from typing import Any, Iterator, TypedDict
from zipfile import BadZipFile

from conda_package_streaming import package_streaming

from .. import yaml
from ..utils import CONDA_PACKAGE_EXTENSIONS, _checksum
from .fs import FileInfo, MinimalFS

log = logging.getLogger(__name__)

INDEX_JSON_PATH = "info/index.json"
ICON_PATH = "info/icon.png"
PATHS_PATH = "info/paths.json"

TABLE_TO_PATH = {
    "index_json": INDEX_JSON_PATH,
    "about": "info/about.json",
    "paths": PATHS_PATH,
    # will use the first one encountered
    "recipe": (
        "info/recipe/meta.yaml",
        "info/recipe/meta.yaml.rendered",
        "info/meta.yaml",
    ),
    # run_exports is rare but used. see e.g. gstreamer.
    # prevents 90% of early tar.bz2 exits.
    # also found in meta.yaml['build']['run_exports']
    "run_exports": "info/run_exports.json",
    "post_install": "info/post_install.json",  # computed
    "icon": ICON_PATH,  # very rare, 16 conda-forge packages
    # recipe_log: always {} in old version of cache
}

PATH_TO_TABLE = {}

for k, v in TABLE_TO_PATH.items():
    if isinstance(v, str):
        PATH_TO_TABLE[v] = k
    else:
        for path in v:
            PATH_TO_TABLE[path] = k

# read, but not saved for later
TABLE_NO_CACHE = {
    "paths",
}

# saved to cache, not found in package
COMPUTED = {"info/post_install.json"}


# lock-free replacement for @cached_property
class cacher:
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __get__(self, inst, objtype=None) -> Any:
        if inst:
            value = self.wrapped(inst)
            setattr(inst, self.wrapped.__name__, value)
            return value
        return self


class ChangedPackage(TypedDict):
    path: str
    mtime: Number
    size: Number


class BaseCondaIndexCache(metaclass=abc.ABCMeta):
    def __init__(
        self,
        channel_root: Path | str,
        subdir: str,
        *,
        fs: MinimalFS | None = None,
        channel_url: str | None = None,
        upstream_stage: str = "fs",
    ):
        """
        channel_root: directory containing platform subdir's, e.g. /clones/conda-forge
        subdir: platform subdir, e.g. 'linux-64'
        fs: MinimalFS (designed to wrap fsspec.spec.AbstractFileSystem); optional.
        channel_url: base url if fs is used; optional.
        upstream_stage: stage from 'stat' table used to track available packages. Default is 'fs'.
        """

        self.subdir = subdir
        self.channel_root = Path(channel_root)
        self.subdir_path = Path(channel_root, subdir)
        self.cache_dir = Path(channel_root, subdir, ".cache")
        self.upstream_stage = upstream_stage

        self.fs = fs or MinimalFS()
        self.channel_url = channel_url or str(channel_root)

        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True)

        # used to determine whether to call self.convert()
        self.cache_is_brand_new = False

    @abc.abstractmethod
    def convert(self):
        """
        Convert filesystem cache to database.
        """

    def close(self):
        """
        Remove and close any database connections.
        """

    @property
    def database_prefix(self):
        """
        All paths must be prefixed with this string.
        """
        return ""

    def database_path(self, fn):
        """
        Return filename with database prefix added.
        """
        return f"{self.database_prefix}{fn}"

    def plain_path(self, path):
        """
        Return filename with any database-specfic prefix stripped off.
        """
        return path.rsplit("/", 1)[-1]

    def open(self, fn: str):
        """
        Given a base package name "somepackage.conda", return an open, seekable
        file object from our channel_url/subdir/fn suitable for reading that
        package.
        """
        abs_fn = self.fs.join(self.channel_url, self.subdir, fn)
        return self.fs.open(abs_fn)

    def extract_to_cache_info_object(self, channel_root, subdir, fn_info: FileInfo):
        """
        fn_info: avoid having to call stat()  a second time on package file.
        """
        return self._extract_to_cache(
            channel_root, subdir, fn_info.fn, stat_result=fn_info
        )

    def _extract_to_cache(self, channel_root, subdir, fn, stat_result=None):
        if stat_result is None:
            # this code path is deprecated
            abs_fn = self.fs.join(self.subdir_path, fn)
            stat_dict = self.fs.stat(abs_fn)
            size = stat_dict["size"]
            mtime = stat_dict["mtime"]
        else:
            abs_fn = self.fs.join(self.channel_url, self.subdir, fn)
            size = stat_result.st_size
            mtime = stat_result.st_mtime
        retval = fn, mtime, size, None

        # we no longer re-use the .conda cache for .tar.bz2; faster conda
        # extraction should preserve enough performance
        try:
            log.debug("cache %s/%s", subdir, fn)

            index_json = self.extract_to_cache_unconditional(fn, abs_fn, size, mtime)

            retval = fn, mtime, size, index_json
        except (
            KeyError,
            EOFError,
            json.JSONDecodeError,
            BadZipFile,  # stdlib zipfile
            OSError,  # stdlib tarfile: OSError: Invalid data stream
        ):
            log.exception("Error extracting %s", fn)
        return retval

    def extract_to_cache_unconditional(self, fn, abs_fn, size, mtime):
        """
        Add or replace fn into cache, disregarding whether it is already cached.

        Return index.json as dict, with added size, checksums.
        """

        wanted = set(PATH_TO_TABLE) - COMPUTED

        # when we see one of these, remove the rest from wanted
        recipe_want_one = {
            "info/recipe/meta.yaml.rendered",
            "info/recipe/meta.yaml",  # by far the most common
            "info/meta.yaml",
        }

        members = {}
        # second stream_conda_info "fileobj" parameter accepts Path or str
        # inherited from ZipFile, bz2.open behavior, but we need to open the
        # file ourselves.
        with self.open(fn) as fileobj:
            package_stream = iter(package_streaming.stream_conda_info(fn, fileobj))
            for tar, member in package_stream:
                if member.name in wanted:
                    wanted.remove(member.name)
                    reader = tar.extractfile(member)
                    if reader is None:
                        log.warning(f"{abs_fn}/{member.name} was not a regular file")
                        continue
                    members[member.name] = reader.read()

                    # immediately parse index.json, decide whether we need icon
                    if member.name == INDEX_JSON_PATH:  # early exit when no icon
                        index_json = json.loads(members[member.name])
                        if index_json.get("icon") is None:
                            wanted = wanted - {ICON_PATH}

                    if member.name in recipe_want_one:
                        # convert yaml; don't look for any more recipe files
                        members[member.name] = _cache_recipe(members[member.name])
                        wanted = wanted - recipe_want_one

                if not wanted:  # we got what we wanted
                    package_stream.close()
                    log.debug("%s early close", fn)

            # XXX if we are reindexing a channel, provide a way to assert that
            # checksums match the upstream stage.
            def checksums():
                """
                Use utility function that accepts open file instead of filename.
                """
                for algorithm in "md5", "sha256":
                    fileobj.seek(0)
                    yield _checksum(fileobj, algorithm)

            md5, sha256 = checksums()

        if wanted and wanted != {"info/run_exports.json"}:
            # very common for some metadata to be missing
            log.debug(f"{fn} missing {wanted} has {set(members.keys())}")

        index_json = json.loads(members["info/index.json"])

        # populate run_exports.json (all False's if there was no
        # paths.json). paths.json should not be needed after this; don't
        # cache large paths.json unless we want a "search for paths"
        # feature unrelated to repodata.json
        try:
            paths_str = members.pop(PATHS_PATH)
        except KeyError:
            paths_str = ""
        members["info/post_install.json"] = _cache_post_install_details(paths_str)

        # decide what fields to filter out, like has_prefix
        filter_fields = {
            "arch",
            "has_prefix",
            "mtime",
            "platform",
            "ucs",
            "requires_features",
            "binstar",
            "target-triplet",
            "machine",
            "operatingsystem",
        }

        index_json = {k: v for k, v in index_json.items() if k not in filter_fields}

        new_info = {"md5": md5, "sha256": sha256, "size": size}

        index_json.update(new_info)

        self.store(fn, size, mtime, members, index_json)

        return index_json

    @abc.abstractmethod
    def store(
        self,
        fn: str,
        size: int,
        mtime,
        members: dict[str, str | bytes],
        index_json: dict,
    ):
        """
        Write a single package's index data to database.
        """

    @abc.abstractmethod
    def load_all_from_cache(self, fn):
        """
        Load package data merged into a single dict for channeldata.
        """

    def save_fs_state(self, subdir_path: str | Path | None = None):
        """
        stat all files in subdir_path to compare against cached repodata.

        subdir_path: implied from self.subdir; not used.
        """
        subdir_url = self.fs.join(self.channel_url, self.subdir)

        log.debug("%s listdir", self.subdir)

        # Put filesystem 'ground truth' into stat table. Will we eventually stat
        # everything on fs, or can we shortcut for new files?

        def listdir_stat():
            # Gather conda package filenames in subdir
            for entry in self.fs.listdir(subdir_url):
                if not entry["name"].endswith(CONDA_PACKAGE_EXTENSIONS):
                    continue
                if "mtime" not in entry or "size" not in entry:
                    entry.update(self.fs.stat(entry["name"]))
                yield {
                    "path": self.database_path(self.fs.basename(entry["name"])),
                    "mtime": entry.get("mtime"),
                    "size": entry["size"],
                }

        log.debug("%s save fs state", self.subdir)
        self.store_fs_state(listdir_stat())

    @abc.abstractmethod
    def store_fs_state(self, listdir_stat: Iterator[dict[str, Any]]):
        """
        Save set of packages to be indexed.
        """

    @abc.abstractmethod
    def changed_packages(self) -> list[ChangedPackage]:
        """
        Compare upstream to 'indexed' state.

        Return packages in upstream that are changed or missing compared to 'indexed'.
        """

    @abc.abstractmethod
    def indexed_packages(self) -> tuple[dict, dict]:
        """
        Return "packages" and "packages.conda" values from the cache for
        "monolithic repodata.json" query.
        """

    @abc.abstractmethod
    def indexed_shards(self, desired: set | None = None):
        """
        Yield (package name, all packages with that name) from database ordered
        by name, path i.o.w. filename.

        :desired: If not None, set of desired package names.
        """

    @abc.abstractmethod
    def run_exports(self) -> Iterator[tuple[str, dict]]:
        """
        Return run_exports data, to be formatted by
        ChannelIndex.build_run_exports_data()
        """


def _cache_post_install_details(paths_json_str):
    post_install_details_json = {
        "binary_prefix": False,
        "text_prefix": False,
        "activate.d": False,
        "deactivate.d": False,
        "pre_link": False,
        "post_link": False,
        "pre_unlink": False,
    }
    if paths_json_str:  # if paths exists at all
        paths = json.loads(paths_json_str).get("paths", [])

        # get embedded prefix data from paths.json
        for f in paths:
            if f.get("prefix_placeholder"):
                if f.get("file_mode") == "binary":
                    post_install_details_json["binary_prefix"] = True
                elif f.get("file_mode") == "text":
                    post_install_details_json["text_prefix"] = True
            # check for any activate.d/deactivate.d scripts
            for k in ("activate.d", "deactivate.d"):
                if not post_install_details_json.get(k) and f["_path"].startswith(
                    f"etc/conda/{k}"
                ):
                    post_install_details_json[k] = True
            # check for any link scripts
            for pat in ("pre-link", "post-link", "pre-unlink"):
                if not post_install_details_json.get(pat) and fnmatch.fnmatch(
                    f["_path"], f"*/.*-{pat}.*"
                ):
                    post_install_details_json[pat.replace("-", "_")] = True

    return json.dumps(post_install_details_json)


def _cache_recipe(recipe_reader):
    recipe_json = yaml.determined_load(recipe_reader)

    try:
        recipe_json_str = json.dumps(recipe_json)
    except TypeError:
        recipe_json.get("requirements", {}).pop("build")  # weird
        recipe_json_str = json.dumps(recipe_json)

    return recipe_json_str


def clear_newline_chars(record, field_name):
    if field_name in record:
        try:
            record[field_name] = record[field_name].strip().replace("\n", " ")
        except AttributeError:
            try:
                # sometimes description gets added as a list instead of just a string
                record[field_name] = (
                    "".join(record[field_name]).strip().replace("\n", " ")
                )

            except TypeError:
                log.warning("Could not _clear_newline_chars from field %s", field_name)
