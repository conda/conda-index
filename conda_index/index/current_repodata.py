"""
Build current_repodata.json, including newest packages and their dependencies,
and any other requested package versions.
"""

import copy
import json

# conda internals
from conda.base.context import context
from conda.core.subdir_data import SubdirData
from conda.models.channel import Channel
from conda.models.match_spec import MatchSpec
from conda.models.version import VersionOrder

from ..utils import CONDA_PACKAGE_EXTENSION_V1, CONDA_PACKAGE_EXTENSION_V2


def _package_sort_key(prec):
    # Single-channel simplification of conda's Resolve.version_key: channel
    # priority is constant for the synthetic dummy channel.
    key = [
        VersionOrder(prec.get("version", "")),
        prec.get("build_number", 0),
        -int(prec.subdir == "noarch"),
    ]
    if context.solver_ignore_timestamps:
        key.append(prec.get("build"))
    else:
        key.extend((prec.get("timestamp", 0), prec.get("build")))
    return key


class _PackageIndex:
    """Grouped, newest-first view of package records from a single channel."""

    def __init__(self, index):
        self.index = index
        self.groups = {}
        self.trackers = {}
        self._cached_find_matches = {}

        for prec in index.values():
            self.groups.setdefault(prec.name, []).append(prec)

        for name, group in self.groups.items():
            unmanageable = [prec for prec in group if prec.is_unmanageable]
            if unmanageable:
                group = self.groups[name] = unmanageable
            for prec in group:
                for feature in prec.track_features or ():
                    self.trackers.setdefault(feature, []).append(prec)
            group.sort(key=_package_sort_key, reverse=True)

    def find_matches(self, spec):
        res = self._cached_find_matches.get(spec)
        if res is not None:
            return res

        name = spec.get_exact_value("name")
        feature_names = spec.get_exact_value("track_features")
        if name:
            candidates = self.groups.get(name, ())
        elif feature_names:
            candidates = (
                prec
                for feature in feature_names
                for prec in self.trackers.get(feature, ())
            )
        else:
            candidates = self.index.values()

        res = tuple(prec for prec in candidates if spec.match(prec))
        self._cached_find_matches[spec] = res
        return res


def _get_package_index(subdir, precs=None, repodata=None):
    packages = {}
    conda_packages = {}
    if not repodata:
        repodata = {
            "info": {
                "subdir": subdir,
                "arch": context.arch_name,
                "platform": context.platform,
            },
            "packages": packages,
            "packages.conda": conda_packages,
        }

    channel = Channel(f"https://conda.anaconda.org/dummy-channel/{subdir}")
    sd = SubdirData(channel)

    # repodata = copy.deepcopy(repodata) # slower than json.dumps/load loop
    repodata_copy = json.loads(json.dumps(repodata))

    # adds url, Channel objects to each repodata package
    sd._process_raw_repodata(repodata_copy)

    sd._loaded = True
    SubdirData._cache_[channel.url(with_credentials=True)] = sd

    index = {prec: prec for prec in precs or sd._package_records}
    return _PackageIndex(index)


def _add_missing_deps(new_index, original_index):
    """For each package in new_index, if any deps are not satisfiable, backfill them from original_index."""

    expanded_groups = copy.deepcopy(new_index.groups)
    seen_specs = set()
    for g_name, g_recs in new_index.groups.items():
        for g_rec in g_recs:
            for dep_spec in g_rec.depends:
                if dep_spec in seen_specs:
                    continue
                ms = MatchSpec(dep_spec)
                if not new_index.find_matches(ms):
                    matches = original_index.find_matches(ms)
                    if matches:
                        version = matches[0].version
                        expanded_groups[ms.name] = set(
                            expanded_groups.get(ms.name, [])
                        ) | set(
                            original_index.find_matches(
                                MatchSpec(f"{ms.name}={version}")
                            )
                        )
                seen_specs.add(dep_spec)
    return [pkg for group in expanded_groups.values() for pkg in group]


def _add_prev_ver_for_features(new_index, orig_index):
    expanded_groups = copy.deepcopy(new_index.groups)
    for g_name in new_index.groups:
        if not any(m.track_features or m.features for m in new_index.groups[g_name]):
            # no features so skip
            continue

        # versions are sorted here so this is the latest
        latest_version = VersionOrder(str(new_index.groups[g_name][0].version))
        if g_name in orig_index.groups:
            # now we iterate through the list to find the next to latest
            # without a feature
            keep_m = None
            for i in range(len(orig_index.groups[g_name])):
                _m = orig_index.groups[g_name][i]
                if VersionOrder(str(_m.version)) <= latest_version and not (
                    _m.track_features or _m.features
                ):
                    keep_m = _m
                    break
            if keep_m is not None:
                expanded_groups[g_name] = {keep_m} | set(
                    expanded_groups.get(g_name, [])
                )

    return [pkg for group in expanded_groups.values() for pkg in group]


def _shard_newest_packages(subdir, index, pins=None):
    """Captures only the newest versions of software in the package index.

    For things where more than one version is supported simultaneously (like Python),
    pass pins as a dictionary, with the key being the package name, and the value being
    a list of supported versions.  For example:

    {'python': ["2.7", "3.6"]}
    """
    groups = {}
    pins = pins or {}
    for g_name, g_recs in index.groups.items():
        # always do the latest implicitly
        version = index.groups[g_name][0].version
        matches = set(index.find_matches(MatchSpec(f"{g_name}={version}")))
        if g_name in pins:
            for pin_value in pins[g_name]:
                version = index.find_matches(MatchSpec(f"{g_name}={pin_value}"))[
                    0
                ].version
                matches.update(index.find_matches(MatchSpec(f"{g_name}={version}")))
        groups[g_name] = matches

    # add the deps of the stuff in the index
    new_index = _get_package_index(
        subdir, precs=[pkg for group in groups.values() for pkg in group]
    )
    new_index = _get_package_index(subdir, precs=_add_missing_deps(new_index, index))

    # now for any pkg with features, add at least one previous version
    # also return
    return set(_add_prev_ver_for_features(new_index, index))


def build_current_repodata(subdir, repodata, pins):
    index = _get_package_index(subdir, repodata=repodata)
    keep_pkgs = _shard_newest_packages(subdir, index, pins)
    new_repodata = {
        k: repodata[k] for k in set(repodata.keys()) - {"packages", "packages.conda"}
    }
    packages = {}
    conda_packages = {}
    for keep_pkg in keep_pkgs:
        if keep_pkg.fn.endswith(CONDA_PACKAGE_EXTENSION_V2):
            conda_packages[keep_pkg.fn] = repodata["packages.conda"][keep_pkg.fn]
            # in order to prevent package churn we consider the md5 for the .tar.bz2 that matches the .conda file
            #    This holds when .conda files contain the same files as .tar.bz2, which is an assumption we'll make
            #    until it becomes more prevalent that people provide only .conda files and just skip .tar.bz2
            counterpart = keep_pkg.fn.replace(
                CONDA_PACKAGE_EXTENSION_V2, CONDA_PACKAGE_EXTENSION_V1
            )
            conda_packages[keep_pkg.fn]["legacy_bz2_md5"] = (
                repodata["packages"].get(counterpart, {}).get("md5")
            )
        elif keep_pkg.fn.endswith(CONDA_PACKAGE_EXTENSION_V1):
            packages[keep_pkg.fn] = repodata["packages"][keep_pkg.fn]
    new_repodata["packages"] = packages
    new_repodata["packages.conda"] = conda_packages
    return new_repodata
