import itertools
import json
import pathlib

from conda.base.constants import CONDA_PACKAGE_EXTENSION_V1

try:
    from tlz import groupby as toolz_groupby
except ImportError:
    from conda._vendor.toolz import groupby as toolz_groupby

from .test_index import TEST_SUBDIR, here

repodata = None


def get_repodata():
    """
    For benchmarks download conda-forge's e.g.
    """
    global repodata
    if repodata:
        return repodata
    repodata = json.loads(
        pathlib.Path(
            here, "index_hotfix_pkgs", TEST_SUBDIR, "repodata.json"
        ).read_text()
    )
    return repodata


def groupby_to_dict(keyfunc, sequence):
    """
    toolz-style groupby, returns a dictionary of { key: [group] } instead of
    iterators.
    """
    result = collections.defaultdict(lambda: [])
    for key, group in itertools.groupby(sequence, keyfunc):
        result[key].extend(group)
    return dict(result)


def strat(groupby_to_dict=groupby_to_dict):
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = set(legacy_packages.keys()) - {
        k[:-6] + CONDA_PACKAGE_EXTENSION_V1 for k in conda_packages.keys()
    }
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})

    candidates = []
    # Could we sort by (name, version, timestamp) all at once and pick the first of each name
    package_groups = groupby_to_dict(
        lambda x: x[1]["name"], all_repodata_packages.items()
    )
    for groupname, group in package_groups.items():
        # Pay special attention to groups that have run_exports - we
        # need to process each version group by version; take newest per
        # version group.  We handle groups that are not in the index at
        # all yet similarly, because we can't check if they have any
        # run_exports.

        # This is more deterministic than, but slower than the old "newest
        # timestamp across all versions if no run_exports", unsatisfying
        # when old versions get new builds. When channeldata.json is not
        # being built from scratch the speed difference is not noticable.
        for vgroup in groupby_to_dict(lambda x: x[1]["version"], group).values():
            candidate = next(
                iter(
                    sorted(
                        vgroup,
                        key=lambda x: x[1].get("timestamp", 0),
                        reverse=True,
                    )
                )
            )
            candidates.append(candidate)

    return candidates


def keyfunc(pair):
    k, v = pair
    return v["name"], v["version"], -v.get("timestamp", 0)


def namever(pair):
    k, v = pair
    return v["name"], v["version"]


def all_packages():
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = set(legacy_packages.keys()) - {
        k[:-6] + CONDA_PACKAGE_EXTENSION_V1 for k in conda_packages.keys()
    }
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})

    return all_repodata_packages


# welp, it's slower
def strat2():
    legacy_packages = repodata["packages"]
    conda_packages = repodata["packages.conda"]

    use_these_legacy_keys = set(legacy_packages.keys()) - {
        k[:-6] + CONDA_PACKAGE_EXTENSION_V1 for k in conda_packages.keys()
    }
    all_repodata_packages = conda_packages.copy()
    all_repodata_packages.update({k: legacy_packages[k] for k in use_these_legacy_keys})

    candidates = []

    for name_version, group in itertools.groupby(
        sorted(all_repodata_packages.items(), key=keyfunc), key=namever
    ):
        candidates.append(next(group))

    return candidates

    package_groups = itertools.groupby(
        sorted(all_repodata_packages.items(), key=lambda x: x[1]["name"]),
        lambda x: x[1]["name"],
    )
    for groupname, group in package_groups:
        # Pay special attention to groups that have run_exports - we
        # need to process each version group by version; take newest per
        # version group.  We handle groups that are not in the index at
        # all yet similarly, because we can't check if they have any
        # run_exports.

        # This is more deterministic than, but slower than the old "newest
        # timestamp across all versions if no run_exports", unsatisfying
        # when old versions get new builds. When channeldata.json is not
        # being built from scratch the speed difference is not noticable.
        for vgroup in groupby_to_dict(lambda x: x[1]["version"], group).values():
            candidate = next(
                iter(
                    sorted(
                        vgroup,
                        key=lambda x: x[1].get("timestamp", 0),
                        reverse=True,
                    )
                )
            )
            candidates.append(candidate)

    return candidates


def strat3():
    all_repodata_packages = all_packages()

    namever = {}

    for fn, package in all_repodata_packages.items():
        key = (package["name"], package["version"])
        timestamp = package.get("timestamp", 0)
        existing = namever.get(key)
        if not existing or existing[1].get("timestamp", 0) < timestamp:
            namever[key] = (fn, package)

    return list(namever.values())


def test_groupby_strategy():
    """
    Assert historical groupby is the same as rewritten version.
    """
    get_repodata()
    assert sorted(strat3()) == sorted(strat(groupby_to_dict=toolz_groupby))


if __name__ == "__main__":
    import timeit

    print("new way", timeit.timeit("strat2()", number=1, globals=globals()))
    print("old way", timeit.timeit("strat()", number=1, globals=globals()))
    print(
        "oldest way",
        timeit.timeit(
            "strat(groupby_to_dict=toolz_groupby)", number=10, globals=globals()
        ),
    )
    print("another way", timeit.timeit("strat3()", number=10, globals=globals()))
