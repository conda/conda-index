import bz2
import json
import os
import shutil
import tarfile
import urllib.parse
from logging import getLogger
from os.path import dirname, isdir, isfile, join
from pathlib import Path
from shutil import rmtree

import conda_package_handling.api
import pytest
import zstandard
from conda.base.context import context

import conda_index.api
import conda_index.index
from conda_index.utils_build import copy_into

from .utils import archive_dir

log = getLogger(__name__)

here = os.path.dirname(__file__)

# NOTE: The recipes for test packages used in this module are at https://github.com/kalefranz/conda-test-packages

# match ./index_hotfix_pkgs/<subdir>
TEST_SUBDIR = "osx-64"


def download(url, local_path):
    # NOTE: The tests in this module used to download packages from the
    # conda-test channel. These packages are small and are now included.
    if not isdir(dirname(local_path)):
        os.makedirs(dirname(local_path))

    archive_path = join(here, "archives", url.rsplit("/", 1)[-1])

    shutil.copy(archive_path, local_path)
    return local_path


def test_index_on_single_subdir_1(testing_workdir):
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/osx-64/conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    download(test_package_url, test_package_path)

    conda_index.index.update_index(
        testing_workdir, channel_name="test-channel", write_bz2=True, write_zst=True
    )

    # #######################################
    # tests for osx-64 subdir
    # #######################################
    assert isfile(join(testing_workdir, "osx-64", "index.html"))
    assert isfile(join(testing_workdir, "osx-64", "repodata.json.bz2"))
    assert isfile(join(testing_workdir, "osx-64", "repodata_from_packages.json.bz2"))

    assert isfile(join(testing_workdir, "osx-64", "repodata.json.zst"))
    assert isfile(join(testing_workdir, "osx-64", "repodata_from_packages.json.zst"))

    # compressed version must be byte-identical
    def compare_zst(filename):
        original_path = Path(testing_workdir, "osx-64", filename)
        compressed_path = Path(testing_workdir, "osx-64", filename + ".zst")
        assert original_path.read_bytes() == zstandard.decompress(
            compressed_path.read_bytes()
        )

    compare_zst("repodata.json")
    compare_zst("current_repodata.json")
    compare_zst("repodata_from_packages.json")

    # we should stop doing bz2 (conda dropped support in 2016) but it should
    # work properly.
    def compare_bz2(filename):
        original_path = Path(testing_workdir, "osx-64", filename)
        compressed_path = Path(testing_workdir, "osx-64", filename + ".bz2")
        assert original_path.read_bytes() == bz2.decompress(
            compressed_path.read_bytes()
        )

    compare_bz2("repodata.json")
    compare_bz2("current_repodata.json")
    compare_bz2("repodata_from_packages.json")

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())
    with open(join(testing_workdir, "osx-64", "repodata_from_packages.json")) as fh:
        actual_pkg_repodata_json = json.loads(fh.read())
    expected_repodata_json = {
        "info": {
            "subdir": "osx-64",
        },
        "packages": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "37861df8111170f5eed4bff27868df59",
                "name": "conda-index-pkg-a",
                "sha256": "459f3e9b2178fa33bdc4e6267326405329d1c1ab982273d9a1c0a5084a1ddc30",
                "size": 8733,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "packages.conda": {},
        "removed": [],
        "repodata_version": 1,
    }
    assert actual_repodata_json == expected_repodata_json
    assert actual_pkg_repodata_json == expected_repodata_json

    # #######################################
    # tests for full channel
    # #######################################

    with open(join(testing_workdir, "channeldata.json")) as fh:
        actual_channeldata_json = json.loads(fh.read())
    expected_channeldata_json = {
        "channeldata_version": 1,
        "packages": {
            "conda-index-pkg-a": {
                "description": "Description field for conda-index-pkg-a. Actually, this is just the python description. "
                "Python is a widely used high-level, general-purpose, interpreted, dynamic "
                "programming language. Its design philosophy emphasizes code "
                "readability, and its syntax allows programmers to express concepts in "
                "fewer lines of code than would be possible in languages such as C++ or "
                "Java. The language provides constructs intended to enable clear programs "
                "on both a small and large scale.",
                "dev_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/meta.yaml",
                "doc_source_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/README.md",
                "doc_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a",
                "home": "https://anaconda.org/conda-test/conda-index-pkg-a",
                "license": "BSD",
                "source_git_url": "https://github.com/kalefranz/conda-test-packages.git",
                "subdirs": [
                    "osx-64",
                ],
                "summary": "Summary field for conda-index-pkg-a",
                "version": "1.0",
                "activate.d": False,
                "deactivate.d": False,
                "post_link": True,
                "pre_link": False,
                "pre_unlink": False,
                "binary_prefix": False,
                "text_prefix": True,
                "run_exports": {},
                # "icon_hash": None,
                # "icon_url": None,
                # "identifiers": None,
                # "keywords": None,
                # "recipe_origin": None,
                # "source_url": None,
                # "tags": None,
                "timestamp": 1508520039,
            }
        },
        "subdirs": ["noarch", "osx-64"],
    }
    assert actual_channeldata_json == expected_channeldata_json


def test_file_index_on_single_subdir_1(testing_workdir):
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/osx-64/conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    download(test_package_url, test_package_path)

    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    # #######################################
    # tests for osx-64 subdir
    # #######################################
    assert isfile(join(testing_workdir, "osx-64", "index.html"))
    assert isfile(join(testing_workdir, "osx-64", "repodata.json.bz2"))
    assert isfile(join(testing_workdir, "osx-64", "repodata_from_packages.json.bz2"))

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())
        assert actual_repodata_json
    with open(join(testing_workdir, "osx-64", "repodata_from_packages.json")) as fh:
        actual_pkg_repodata_json = json.loads(fh.read())
    expected_repodata_json = {
        "info": {
            "subdir": "osx-64",
        },
        "packages": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "37861df8111170f5eed4bff27868df59",
                "name": "conda-index-pkg-a",
                "sha256": "459f3e9b2178fa33bdc4e6267326405329d1c1ab982273d9a1c0a5084a1ddc30",
                "size": 8733,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "packages.conda": {},
        "removed": [],
        "repodata_version": 1,
    }
    assert actual_repodata_json == expected_repodata_json
    assert actual_pkg_repodata_json == expected_repodata_json

    # download two packages here, put them both in the same subdir
    test_package_path = join(testing_workdir, "osx-64", "fly-2.5.2-0.tar.bz2")
    test_package_url = (
        "https://conda.anaconda.org/conda-test/osx-64/fly-2.5.2-0.tar.bz2"
    )
    download(test_package_url, test_package_path)

    test_package_path = join(testing_workdir, "osx-64", "nano-2.4.1-0-tar.bz2")
    test_package_url = (
        "https://conda.anaconda.org/conda-test/osx-64/nano-2.4.1-0.tar.bz2"
    )
    download(test_package_url, test_package_path)

    updated_packages = expected_repodata_json.get("packages")

    expected_repodata_json["packages"] = updated_packages

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())
        assert actual_repodata_json
    with open(join(testing_workdir, "osx-64", "repodata_from_packages.json")) as fh:
        actual_pkg_repodata_json = json.loads(fh.read())
        assert actual_pkg_repodata_json

    assert actual_repodata_json == expected_repodata_json
    assert actual_pkg_repodata_json == expected_repodata_json

    # #######################################
    # tests for full channel
    # #######################################

    with open(join(testing_workdir, "channeldata.json")) as fh:
        actual_channeldata_json = json.loads(fh.read())
    expected_channeldata_json = {
        "channeldata_version": 1,
        "packages": {
            "conda-index-pkg-a": {
                "description": "Description field for conda-index-pkg-a. Actually, this is just the python description. "
                "Python is a widely used high-level, general-purpose, interpreted, dynamic "
                "programming language. Its design philosophy emphasizes code "
                "readability, and its syntax allows programmers to express concepts in "
                "fewer lines of code than would be possible in languages such as C++ or "
                "Java. The language provides constructs intended to enable clear programs "
                "on both a small and large scale.",
                "dev_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/meta.yaml",
                "doc_source_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/README.md",
                "doc_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a",
                "home": "https://anaconda.org/conda-test/conda-index-pkg-a",
                "license": "BSD",
                "source_git_url": "https://github.com/kalefranz/conda-test-packages.git",
                "subdirs": [
                    "osx-64",
                ],
                "summary": "Summary field for conda-index-pkg-a",
                "version": "1.0",
                "activate.d": False,
                "deactivate.d": False,
                "post_link": True,
                "pre_link": False,
                "pre_unlink": False,
                "binary_prefix": False,
                "text_prefix": True,
                "run_exports": {},
                # Possible keys, removed because they are None:
                # "icon_hash": None,
                # "icon_url": None,
                # "identifiers": None,
                # "keywords": None,
                # "recipe_origin": None,
                # "source_url": None,
                # "tags": None,
                "timestamp": 1508520039,
            },
        },
        "subdirs": ["noarch", "osx-64"],
    }

    assert actual_channeldata_json == expected_channeldata_json


def test_index_noarch_osx64_1(testing_workdir):
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/osx-64/conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    download(test_package_url, test_package_path)

    test_package_path = join(
        testing_workdir, "noarch", "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/noarch/conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    download(test_package_url, test_package_path)

    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    # #######################################
    # tests for osx-64 subdir
    # #######################################
    assert isfile(join(testing_workdir, "osx-64", "index.html"))
    assert isfile(
        join(testing_workdir, "osx-64", "repodata.json")
    )  # repodata is tested in test_index_on_single_subdir_1
    assert isfile(join(testing_workdir, "osx-64", "repodata.json.bz2"))
    assert isfile(join(testing_workdir, "osx-64", "repodata_from_packages.json"))
    assert isfile(join(testing_workdir, "osx-64", "repodata_from_packages.json.bz2"))

    # #######################################
    # tests for noarch subdir
    # #######################################
    assert isfile(join(testing_workdir, "noarch", "index.html"))
    assert isfile(join(testing_workdir, "noarch", "repodata.json.bz2"))
    assert isfile(join(testing_workdir, "noarch", "repodata_from_packages.json.bz2"))

    with open(join(testing_workdir, "noarch", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())
    with open(join(testing_workdir, "noarch", "repodata_from_packages.json")) as fh:
        actual_pkg_repodata_json = json.loads(fh.read())
    expected_repodata_json = {
        "info": {
            "subdir": "noarch",
        },
        "packages": {
            "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2": {
                "build": "pyhed9eced_1",
                "build_number": 1,
                "depends": ["python"],
                "license": "BSD",
                "md5": "56b5f6b7fb5583bccfc4489e7c657484",
                "name": "conda-index-pkg-a",
                "noarch": "python",
                "sha256": "7430743bffd4ac63aa063ae8518e668eac269c783374b589d8078bee5ed4cbc6",
                "size": 7882,
                "subdir": "noarch",
                "timestamp": 1508520204768,
                "version": "1.0",
            },
        },
        "packages.conda": {},
        "removed": [],
        "repodata_version": 1,
    }
    assert actual_repodata_json == expected_repodata_json
    assert actual_pkg_repodata_json == expected_repodata_json

    # #######################################
    # tests for full channel
    # #######################################

    with open(join(testing_workdir, "channeldata.json")) as fh:
        actual_channeldata_json = json.loads(fh.read())
    expected_channeldata_json = {
        "channeldata_version": 1,
        "packages": {
            "conda-index-pkg-a": {
                "description": "Description field for conda-index-pkg-a. Actually, this is just the python description. "
                "Python is a widely used high-level, general-purpose, interpreted, dynamic "
                "programming language. Its design philosophy emphasizes code "
                "readability, and its syntax allows programmers to express concepts in "
                "fewer lines of code than would be possible in languages such as C++ or "
                "Java. The language provides constructs intended to enable clear programs "
                "on both a small and large scale.",
                "dev_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/meta.yaml",
                "doc_source_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a/README.md",
                "doc_url": "https://github.com/kalefranz/conda-test-packages/blob/master/conda-index-pkg-a",
                "home": "https://anaconda.org/conda-test/conda-index-pkg-a",
                "license": "BSD",
                "source_git_url": "https://github.com/kalefranz/conda-test-packages.git",
                # "source_url": None,
                "subdirs": [
                    "noarch",
                    "osx-64",
                ],
                "summary": "Summary field for conda-index-pkg-a. This is the python noarch version.",  # <- tests that the higher noarch build number is the data collected
                "version": "1.0",
                "activate.d": False,
                "deactivate.d": False,
                "post_link": True,
                "pre_link": False,
                "pre_unlink": False,
                "binary_prefix": False,
                "text_prefix": True,
                "run_exports": {},
                # "icon_hash": None,
                # "icon_url": None,
                # "identifiers": None,
                # "tags": None,
                "timestamp": 1508520039,
                # "keywords": None,
                # "recipe_origin": None,
            }
        },
        "subdirs": [
            "noarch",
            "osx-64",
        ],
    }
    assert actual_channeldata_json == expected_channeldata_json


def _build_test_index(workdir):
    """
    Ensure workdir contains a valid index.
    """

    # workdir may be the same during a single test run?
    # Python 3.7 workaround "no dirs_exist_ok flag"
    index_hotfix_pkgs = join(here, "index_hotfix_pkgs")
    for path in os.scandir(index_hotfix_pkgs):
        if path.is_dir():
            shutil.copytree(
                join(here, "index_hotfix_pkgs", path.name), join(workdir, path.name)
            )
        elif path.is_file():
            shutil.copyfile(
                join(here, "index_hotfix_pkgs", path.name), join(workdir, path.name)
            )

    with open(os.path.join(workdir, TEST_SUBDIR, "repodata.json")) as f:
        original_metadata = json.load(f)

    pkg_list = original_metadata["packages"]
    assert "track_features_test-1.0-0.tar.bz2" in pkg_list
    assert pkg_list["track_features_test-1.0-0.tar.bz2"]["track_features"] == "dummy"

    assert "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkg_list
    assert pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["features"] == "dummy"
    assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]

    assert "revoke_test-1.0-0.tar.bz2" in pkg_list
    assert "zlib" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    assert (
        "package_has_been_revoked"
        not in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    )

    assert "remove_test-1.0-0.tar.bz2" in pkg_list


def test_gen_patch_py(testing_workdir):
    """
    This is a channel-wide file that applies to many subdirs.  It must have a function with this signature:

    def _patch_repodata(repodata, subdir):

    That function must return a dictionary of patch instructions, of the form:

    {
        "patch_instructions_version": 1,
        "packages": defaultdict(dict),
        "revoke": [],
        "remove": [],
    }

    revoke and remove are lists of filenames. remove makes the file not show up
    in the index (it may still be downloadable with a direct URL to the file).
    revoke makes packages uninstallable by adding an unsatisfiable dependency.
    This can be made installable by including a channel that has that package
    (to be created by @jjhelmus).

    packages is a dictionary, where keys are package filenames. Values are
    dictionaries similar to the contents of each package in repodata.json. Any
    values in provided in packages here overwrite the values in repodata.json.
    Any value set to None is removed.
    """
    _build_test_index(testing_workdir)

    func = """
def _patch_repodata(repodata, subdir):
    pkgs = repodata["packages"]
    import fnmatch
    replacement_dict = {}
    if "track_features_test-1.0-0.tar.bz2" in pkgs:
        replacement_dict["track_features_test-1.0-0.tar.bz2"] = {"track_features": None}
    if "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkgs:
        replacement_dict["hotfix_depends_test-1.0-dummy_0.tar.bz2"] = {
                             "depends": pkgs["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"] + ["dummy"],
                             "features": None}
    revoke_list = [pkg for pkg in pkgs if fnmatch.fnmatch(pkg, "revoke_test*")]
    remove_list = [pkg for pkg in pkgs if fnmatch.fnmatch(pkg, "remove_test*")]
    return {
        "patch_instructions_version": 1,
        "packages": replacement_dict,
        "revoke": revoke_list,
        "remove": remove_list,
    }
"""
    patch_file = os.path.join(testing_workdir, "repodata_patch.py")
    with open(patch_file, "w") as f:
        f.write(func)

    # indexing a second time with the same patchset should keep the removals
    for i in (1, 2):
        conda_index.index.update_index(
            testing_workdir,
            patch_generator=patch_file,
            verbose=True,
        )
        with open(os.path.join(testing_workdir, TEST_SUBDIR, "repodata.json")) as f:
            patched_metadata = json.load(f)

        pkg_list = patched_metadata["packages"]
        assert "track_features_test-1.0-0.tar.bz2" in pkg_list
        assert "track_features" not in pkg_list["track_features_test-1.0-0.tar.bz2"]
        print("pass %s track features ok" % i)

        assert "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkg_list
        assert "features" not in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]
        assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]
        assert "dummy" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]
        print("pass %s hotfix ok" % i)

        assert "revoke_test-1.0-0.tar.bz2" in pkg_list
        assert "zlib" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
        assert (
            "package_has_been_revoked"
            in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
        )
        print("pass %s revoke ok" % i)

        assert "remove_test-1.0-0.tar.bz2" not in pkg_list
        assert "remove_test-1.0-0.tar.bz2" in patched_metadata["removed"], (
            "removed list not populated in run %d" % i
        )
        print("pass %s remove ok" % i)

        with open(
            os.path.join(testing_workdir, TEST_SUBDIR, "repodata_from_packages.json")
        ) as f:
            pkg_metadata = json.load(f)

        pkg_list = pkg_metadata["packages"]
        assert "track_features_test-1.0-0.tar.bz2" in pkg_list
        assert (
            pkg_list["track_features_test-1.0-0.tar.bz2"]["track_features"] == "dummy"
        )

        assert "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkg_list
        assert (
            pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["features"] == "dummy"
        )
        assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]

        assert "revoke_test-1.0-0.tar.bz2" in pkg_list
        assert "zlib" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
        assert (
            "package_has_been_revoked"
            not in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
        )


def test_channel_patch_instructions_json(testing_workdir):
    _build_test_index(testing_workdir)

    replacement_dict = {}
    replacement_dict["track_features_test-1.0-0.tar.bz2"] = {"track_features": None}
    replacement_dict["hotfix_depends_test-1.0-dummy_0.tar.bz2"] = {
        "depends": ["zlib", "dummy"],
        "features": None,
    }

    patch = {
        "patch_instructions_version": 1,
        "packages": replacement_dict,
        "revoke": ["revoke_test-1.0-0.tar.bz2"],
        "remove": ["remove_test-1.0-0.tar.bz2"],
    }

    with open(
        os.path.join(testing_workdir, TEST_SUBDIR, "patch_instructions.json"), "w"
    ) as f:
        json.dump(patch, f)

    conda_index.index.update_index(testing_workdir)

    with open(os.path.join(testing_workdir, TEST_SUBDIR, "repodata.json")) as f:
        patched_metadata = json.load(f)

    formats = (("packages", ".tar.bz2"), ("packages.conda", ".conda"))

    for key, ext in formats:
        pkg_list = patched_metadata[key]
        assert "track_features_test-1.0-0" + ext in pkg_list
        assert "track_features" not in pkg_list["track_features_test-1.0-0" + ext]

        assert "hotfix_depends_test-1.0-dummy_0" + ext in pkg_list
        assert "features" not in pkg_list["hotfix_depends_test-1.0-dummy_0" + ext]
        assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0" + ext]["depends"]
        assert "dummy" in pkg_list["hotfix_depends_test-1.0-dummy_0" + ext]["depends"]

        assert "revoke_test-1.0-0" + ext in pkg_list
        assert "zlib" in pkg_list["revoke_test-1.0-0" + ext]["depends"]
        assert (
            "package_has_been_revoked" in pkg_list["revoke_test-1.0-0" + ext]["depends"]
        )

        assert "remove_test-1.0-0" + ext not in pkg_list

        with open(
            os.path.join(testing_workdir, TEST_SUBDIR, "repodata_from_packages.json")
        ) as f:
            pkg_repodata = json.load(f)

        pkg_list = pkg_repodata[key]
        assert "track_features_test-1.0-0" + ext in pkg_list
        assert pkg_list["track_features_test-1.0-0" + ext]["track_features"] == "dummy"

        assert "hotfix_depends_test-1.0-dummy_0" + ext in pkg_list
        assert pkg_list["hotfix_depends_test-1.0-dummy_0" + ext]["features"] == "dummy"
        assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0" + ext]["depends"]

        assert "revoke_test-1.0-0" + ext in pkg_list
        assert "zlib" in pkg_list["revoke_test-1.0-0" + ext]["depends"]
        assert (
            "package_has_been_revoked"
            not in pkg_list["revoke_test-1.0-0" + ext]["depends"]
        )

        assert "remove_test-1.0-0" + ext in pkg_list


def test_patch_from_tarball(testing_workdir):
    """This is how we expect external communities to provide patches to us.
    We can't let them just give us Python files for us to run, because of the
    security risk of arbitrary code execution."""
    _build_test_index(testing_workdir)

    # our hotfix metadata can be generated any way you want.  Hard-code this
    # here, but in general, people will use some python file to generate this.

    replacement_dict = {}
    replacement_dict["track_features_test-1.0-0.tar.bz2"] = {"track_features": None}
    replacement_dict["hotfix_depends_test-1.0-dummy_0.tar.bz2"] = {
        "depends": ["zlib", "dummy"],
        "features": None,
    }

    patch = {
        "patch_instructions_version": 1,
        "packages": replacement_dict,
        "revoke": ["revoke_test-1.0-0.tar.bz2"],
        "remove": ["remove_test-1.0-0.tar.bz2"],
    }
    with open("patch_instructions.json", "w") as f:
        json.dump(patch, f)

    with tarfile.open("patch_archive.tar.bz2", "w:bz2") as archive:
        archive.add(
            "patch_instructions.json", "%s/patch_instructions.json" % TEST_SUBDIR
        )

    conda_index.index.update_index(
        testing_workdir, patch_generator="patch_archive.tar.bz2"
    )

    with open(os.path.join(testing_workdir, TEST_SUBDIR, "repodata.json")) as f:
        patched_metadata = json.load(f)

    pkg_list = patched_metadata["packages"]
    assert "track_features_test-1.0-0.tar.bz2" in pkg_list
    assert "track_features" not in pkg_list["track_features_test-1.0-0.tar.bz2"]

    assert "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkg_list
    assert "features" not in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]
    assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]
    assert "dummy" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]

    assert "revoke_test-1.0-0.tar.bz2" in pkg_list
    assert "zlib" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    assert (
        "package_has_been_revoked" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    )

    assert "remove_test-1.0-0.tar.bz2" not in pkg_list

    with open(
        os.path.join(testing_workdir, TEST_SUBDIR, "repodata_from_packages.json")
    ) as f:
        pkg_repodata = json.load(f)

    pkg_list = pkg_repodata["packages"]
    assert "track_features_test-1.0-0.tar.bz2" in pkg_list
    assert pkg_list["track_features_test-1.0-0.tar.bz2"]["track_features"] == "dummy"

    assert "hotfix_depends_test-1.0-dummy_0.tar.bz2" in pkg_list
    assert pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["features"] == "dummy"
    assert "zlib" in pkg_list["hotfix_depends_test-1.0-dummy_0.tar.bz2"]["depends"]

    assert "revoke_test-1.0-0.tar.bz2" in pkg_list
    assert "zlib" in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    assert (
        "package_has_been_revoked"
        not in pkg_list["revoke_test-1.0-0.tar.bz2"]["depends"]
    )

    assert "remove_test-1.0-0.tar.bz2" in pkg_list


def test_index_of_removed_pkg(testing_metadata):
    archive_name = "test_index_of_removed_pkg-1.0-1.tar.bz2"
    archive_destination = os.path.join(
        testing_metadata.config.croot, TEST_SUBDIR, archive_name
    )

    # copy the package
    os.makedirs(os.path.join(testing_metadata.config.croot, TEST_SUBDIR))
    shutil.copy(os.path.join(here, "archives", archive_name), archive_destination)

    conda_index.api.update_index(testing_metadata.config.croot)

    # repodata.json should exist here
    with open(
        os.path.join(testing_metadata.config.croot, TEST_SUBDIR, "repodata.json")
    ) as f:
        repodata = json.load(f)
    assert repodata["packages"]

    for f in [archive_destination]:
        os.remove(f)

    # repodata.json should be empty here
    conda_index.api.update_index(testing_metadata.config.croot)
    with open(
        os.path.join(testing_metadata.config.croot, TEST_SUBDIR, "repodata.json")
    ) as f:
        repodata = json.load(f)
    assert not repodata["packages"]
    with open(
        os.path.join(
            testing_metadata.config.croot, TEST_SUBDIR, "repodata_from_packages.json"
        )
    ) as f:
        repodata = json.load(f)
    assert not repodata["packages"]


def test_index_of_updated_package(testing_workdir):
    """
    Test that package is re-indexed when its mtime changes.
    """
    _build_test_index(testing_workdir)

    conda_index.index.update_index(
        testing_workdir, subdirs=["osx-64", "noarch"], verbose=True, threads=1
    )

    index_cache = conda_index.index.sqlitecache.CondaIndexCache(
        channel_root=testing_workdir, subdir="osx-64"
    )
    assert list(index_cache.changed_packages()) == []

    # minimal values or else current_repodata will fail.
    dummy_index_json = (
        '{"name":"x", "version":"1", "build":"0", "build_number":0, "size":0}'
    )

    with index_cache.db as db:
        db.execute(f"UPDATE index_json SET index_json='{dummy_index_json}'")
        assert all(
            row["index_json"] == dummy_index_json
            for row in db.execute("SELECT index_json FROM index_json")
        )
        db.commit()
    index_cache.close()

    conda_index.index.update_index(
        testing_workdir, subdirs=["osx-64"], verbose=True, threads=1
    )

    # indexed and fs mtime still match; index will not be changed.
    with index_cache.db as db:
        assert all(
            row["index_json"] == dummy_index_json
            for row in db.execute("SELECT index_json FROM index_json")
        )
        db.execute("UPDATE stat SET mtime = mtime-1")
        db.commit()
    index_cache.close()

    conda_index.index.update_index(
        testing_workdir, subdirs=["osx-64"], verbose=True, threads=1
    )

    # indexed and fs mtime did not match; index wil be re-populated.
    with index_cache.db as db:
        assert not any(
            row["index_json"] == dummy_index_json
            for row in db.execute("SELECT index_json FROM index_json")
        )


def test_patch_instructions_with_missing_subdir(testing_workdir):
    os.makedirs("linux-64")
    os.makedirs("zos-z")
    conda_index.api.update_index(".")  # what is the current working directory?
    # we use conda-forge's patch instructions because they don't have zos-z
    # data, and that triggers an error
    pkg = "conda-forge-repodata-patches"
    url = "https://anaconda.org/conda-forge/{0}/20180828/download/noarch/{0}-20180828-0.tar.bz2".format(
        pkg
    )
    patch_instructions = download(url, os.path.join(os.getcwd(), "patches.tar.bz2"))
    conda_index.api.update_index(".", patch_generator=patch_instructions)


def test_stat_cache_used(testing_workdir, mocker):
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/osx-64/conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    download(test_package_url, test_package_path)
    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    cph_extract = mocker.spy(conda_package_handling.api, "extract")
    conda_index.index.update_index(testing_workdir, channel_name="test-channel")
    cph_extract.assert_not_called()


@pytest.mark.skip(reason="No longer re-use cache between .tar.bz2 and .conda")
def test_new_pkg_format_preferred(testing_workdir, mocker):
    """Test that in one pass, the .conda file is extracted before the .tar.bz2, and the .tar.bz2 uses the cache"""
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0"
    )
    exts = (".tar.bz2", ".conda")
    for ext in exts:
        copy_into(
            os.path.join(archive_dir, "conda-index-pkg-a-1.0-py27h5e241af_0" + ext),
            test_package_path + ext,
        )
    # mock the extract function, so that we can assert that it is not called
    # with the .tar.bz2, because the .conda should be preferred
    import conda_index.index
    import conda_index.index.sqlitecache

    cph_extract = mocker.spy(
        conda_index.index.sqlitecache.package_streaming, "stream_conda_info"
    )
    conda_index.index.update_index(
        testing_workdir, channel_name="test-channel", debug=True
    )

    # conda-index standalone REMOVES the re-use info between .conda/.tar.bz2 feature
    # other speedups should even us out, but this feature could be brought back.
    cph_extract.assert_any_call(test_package_path + ".conda")
    cph_extract.assert_any_call(test_package_path + ".tar.bz2")

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())

    expected_repodata_json = {
        "info": {
            "subdir": "osx-64",
        },
        "packages": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "37861df8111170f5eed4bff27868df59",
                "name": "conda-index-pkg-a",
                "sha256": "459f3e9b2178fa33bdc4e6267326405329d1c1ab982273d9a1c0a5084a1ddc30",
                "size": 8733,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "packages.conda": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.conda": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "4ed4b435f400dac1aabdc1fff06f78ff",
                "name": "conda-index-pkg-a",
                "sha256": "67b07b644105439515cc5c8c22c86939514cacf30c8c574cd70f5f1267a40f19",
                "size": 9296,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "removed": [],
        "repodata_version": 1,
    }
    assert actual_repodata_json == expected_repodata_json

    # if we clear the stat cache, we force a re-examination.  This
    # re-examination will load files from the cache.  This has been a source of
    # bugs in the past, where the wrong cached file being loaded resulted in
    # incorrect hashes/sizes for either the .tar.bz2 or .conda, depending on
    # which of those 2 existed in the cache.
    rmtree(os.path.join(testing_workdir, "osx-64", "stat.json"))
    conda_index.index.update_index(
        testing_workdir, channel_name="test-channel", verbose=True, debug=True
    )

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())

    assert actual_repodata_json == expected_repodata_json

    # make sure .conda and .tar.bz2 exist in index.html
    index_html = Path(testing_workdir, "osx-64", "index.html").read_text()
    assert len(expected_repodata_json["packages"])
    assert len(expected_repodata_json["packages.conda"])
    expected_packages = {
        *expected_repodata_json["packages"],
        *expected_repodata_json["packages.conda"],
    }
    for package in expected_packages:
        assert f'href="{package}"' in index_html


def test_new_pkg_format_stat_cache_used(testing_workdir, mocker):
    # if we have old .tar.bz2 index cache stuff, assert that we pick up correct
    # md5, sha26 and size for .conda
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0"
    )
    copy_into(
        os.path.join(archive_dir, "conda-index-pkg-a-1.0-py27h5e241af_0" + ".tar.bz2"),
        test_package_path + ".tar.bz2",
    )
    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    # mock the extract function, so that we can assert that it is not called,
    # because the stat cache should exist if this doesn't work, something about
    # the stat cache is confused.  It's a little convoluted, because the index
    # has keys for .tar.bz2's, but the stat cache comes from .conda files when
    # they are available because extracting them is much, much faster.
    copy_into(
        os.path.join(archive_dir, "conda-index-pkg-a-1.0-py27h5e241af_0" + ".conda"),
        test_package_path + ".conda",
    )
    cph_extract = mocker.spy(conda_package_handling.api, "extract")
    conda_index.index.update_index(
        testing_workdir, channel_name="test-channel", debug=True
    )
    cph_extract.assert_not_called()

    with open(join(testing_workdir, "osx-64", "repodata.json")) as fh:
        actual_repodata_json = json.loads(fh.read())

    expected_repodata_json = {
        "info": {
            "subdir": "osx-64",
        },
        "packages": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "37861df8111170f5eed4bff27868df59",
                "name": "conda-index-pkg-a",
                "sha256": "459f3e9b2178fa33bdc4e6267326405329d1c1ab982273d9a1c0a5084a1ddc30",
                "size": 8733,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "packages.conda": {
            "conda-index-pkg-a-1.0-py27h5e241af_0.conda": {
                "build": "py27h5e241af_0",
                "build_number": 0,
                "depends": ["python >=2.7,<2.8.0a0"],
                "license": "BSD",
                "md5": "4ed4b435f400dac1aabdc1fff06f78ff",
                "name": "conda-index-pkg-a",
                "sha256": "67b07b644105439515cc5c8c22c86939514cacf30c8c574cd70f5f1267a40f19",
                "size": 9296,
                "subdir": "osx-64",
                "timestamp": 1508520039632,
                "version": "1.0",
            },
        },
        "removed": [],
        "repodata_version": 1,
    }
    assert actual_repodata_json == expected_repodata_json


@pytest.mark.skipif(
    not hasattr(context, "use_only_tar_bz2") or getattr(context, "use_only_tar_bz2"),
    reason="conda is set to auto-disable .conda for old conda-build.",
)
def test_current_index_reduces_space(index_data):
    repodata = Path(index_data, "time_cut", "repodata.json")
    with open(repodata) as f:
        repodata = json.load(f)
    assert len(repodata["packages"]) == 7
    assert len(repodata["packages.conda"]) == 3
    trimmed_repodata = conda_index.index._build_current_repodata(
        "linux-64", repodata, None
    )

    tar_bz2_keys = {
        "two-because-satisfiability-1.2.11-h7b6447c_3.tar.bz2",
        "two-because-satisfiability-1.2.10-h7b6447c_3.tar.bz2",
        "depends-on-older-1.2.10-h7b6447c_3.tar.bz2",
        "ancient-package-1.2.10-h7b6447c_3.tar.bz2",
        "one-gets-filtered-1.3.10-h7b6447c_3.tar.bz2",
    }
    # conda 4.7+ removes .tar.bz2 files in favor of .conda files
    tar_bz2_keys.remove("one-gets-filtered-1.3.10-h7b6447c_3.tar.bz2")

    # .conda files will replace .tar.bz2 files.  Older packages that are necessary for satisfiability will remain
    assert set(trimmed_repodata["packages"].keys()) == tar_bz2_keys

    assert set(trimmed_repodata["packages.conda"].keys()) == {
        "one-gets-filtered-1.3.10-h7b6447c_3.conda"
    }

    # we can keep more than one version series using a collection of keys
    trimmed_repodata = conda_index.index._build_current_repodata(
        "linux-64", repodata, {"one-gets-filtered": ["1.2", "1.3"]}
    )

    assert set(trimmed_repodata["packages.conda"].keys()) == {
        "one-gets-filtered-1.2.11-h7b6447c_3.conda",
        "one-gets-filtered-1.3.10-h7b6447c_3.conda",
    }


def test_current_index_version_keys_keep_older_packages(index_data):
    pkg_dir = Path(index_data, "packages")

    # pass no version file
    conda_index.api.update_index(pkg_dir)
    with open(os.path.join(pkg_dir, "osx-64", "current_repodata.json")) as f:
        repodata = json.load(f)
    # only the newest version is kept
    assert len(repodata["packages"]) == 1
    assert list(repodata["packages"].values())[0]["version"] == "2.0"

    # pass version file
    conda_index.api.update_index(
        pkg_dir, current_index_versions=os.path.join(pkg_dir, "versions.yml")
    )
    with open(os.path.join(pkg_dir, "osx-64", "current_repodata.json")) as f:
        repodata = json.load(f)
    assert len(repodata["packages"]) == 2

    # pass dict that is equivalent to version file
    conda_index.api.update_index(
        pkg_dir, current_index_versions={"dummy-package": ["1.0"]}
    )
    with open(os.path.join(pkg_dir, "osx-64", "current_repodata.json")) as f:
        repodata = json.load(f)
    assert list(repodata["packages"].values())[0]["version"] == "1.0"


def test_channeldata_picks_up_all_versions_of_run_exports(index_data):
    pkg_dir = os.path.join(index_data, "packages")
    conda_index.api.update_index(pkg_dir)
    with open(os.path.join(pkg_dir, "channeldata.json")) as f:
        repodata = json.load(f)
    run_exports = repodata["packages"]["run_exports_versions"]["run_exports"]
    assert len(run_exports) == 2
    assert "1.0" in run_exports
    assert "2.0" in run_exports


def test_index_invalid_packages(index_data):
    pkg_dir = os.path.join(index_data, "corrupt")
    conda_index.api.update_index(pkg_dir)
    with open(os.path.join(pkg_dir, "channeldata.json")) as f:
        repodata = json.load(f)
    assert len(repodata["packages"]) == 0


def test_index_clears_changed_packages(testing_workdir):
    test_package_path = join(
        testing_workdir, "osx-64", "conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/osx-64/conda-index-pkg-a-1.0-py27h5e241af_0.tar.bz2"
    download(test_package_url, test_package_path)

    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    index_cache = conda_index.index.sqlitecache.CondaIndexCache(
        channel_root=testing_workdir, subdir="osx-64"
    )
    assert list(index_cache.changed_packages()) == []

    # should update mtime
    import time

    time.sleep(1)  # ensure mtime is at least 1 second greater
    download(test_package_url, test_package_path)

    with index_cache.db:  # force transaction
        # this function should also commit a transaction, even without `with
        # index_cache.db`
        index_cache.save_fs_state(join(testing_workdir, "osx-64"))

    assert len(list(index_cache.changed_packages())) == 1

    index_cache.close()

    conda_index.index.update_index(testing_workdir, channel_name="test-channel")

    # force new database connection
    index_cache = conda_index.index.sqlitecache.CondaIndexCache(
        channel_root=testing_workdir, subdir="osx-64"
    )
    assert list(index_cache.changed_packages()) == []


def test_no_run_exports(index_data):
    pkg_dir = os.path.join(index_data, "packages")
    conda_index.api.update_index(pkg_dir, write_run_exports=False)
    for subdir in ("osx-64", "noarch"):
        assert not os.path.isfile(os.path.join(pkg_dir, subdir, "run_exports.json"))


def test_run_exports(index_data):
    pkg_dir = os.path.join(index_data, "packages")
    conda_index.api.update_index(pkg_dir, write_run_exports=True)

    noarch_run_exports_path = os.path.join(pkg_dir, "noarch", "run_exports.json")
    assert os.path.isfile(noarch_run_exports_path)
    with open(noarch_run_exports_path) as f:
        noarch_data = json.load(f)

    # Test data defines two packages with run_exports in noarch
    assert noarch_data["info"]["subdir"] == "noarch"
    assert noarch_data["info"]["version"] == 1
    assert "packages" in noarch_data
    assert "packages.conda" in noarch_data
    seen = 0
    for pkg in noarch_data["packages"]:
        if pkg.startswith("run_exports_versions-1.0-"):
            assert noarch_data["packages"][pkg]["run_exports"] == {
                "weak": ["run_exports_version 1.0"]
            }
            seen += 1
        elif pkg.startswith("run_exports_versions-2.0-"):
            assert noarch_data["packages"][pkg]["run_exports"] == {
                "weak": ["run_exports_version 2.0"]
            }
            seen += 1
    assert seen == 2

    # In osx-64, there're two packages with no run_exports, but they should also be listed
    # with an empty run_exports dict
    osx64_run_exports_path = os.path.join(pkg_dir, "osx-64", "run_exports.json")
    assert os.path.isfile(osx64_run_exports_path)
    with open(osx64_run_exports_path) as f:
        osx64_data = json.load(f)

    assert osx64_data["info"]["subdir"] == "osx-64"
    assert osx64_data["info"]["version"] == 1
    assert "packages" in osx64_data
    assert "packages.conda" in osx64_data
    seen = 0
    for pkg in osx64_data["packages"]:
        if pkg.startswith("dummy-package-"):
            assert osx64_data["packages"][pkg]["run_exports"] == {}
            seen += 1
    assert seen == 2


def test_compact_json(index_data):
    """
    conda-index should be able to write pretty-printed or compact json.
    """
    pkg_dir = Path(index_data, "packages")

    # compact json
    channel_index = conda_index.index.ChannelIndex(
        str(pkg_dir),
        None,
        write_bz2=False,
        write_zst=False,
        compact_json=True,
        threads=1,
    )

    channel_index.index(None)

    assert "\n" not in (pkg_dir / "noarch" / "repodata.json").read_text()

    # bloated json
    channel_index = conda_index.index.ChannelIndex(
        str(pkg_dir), None, write_bz2=False, write_zst=False, compact_json=False
    )

    (pkg_dir / "noarch" / "repodata.json").unlink()

    channel_index.index(None)
    assert "\n" in (pkg_dir / "noarch" / "repodata.json").read_text()


def test_track_features(index_data):
    """
    Coverage testing for _add_prev_ver_for_features.

    The features/track_features system is not often used but we want to cover it
    in tests.
    """
    pkg_dir = Path(index_data, "packages")

    # compact json
    channel_index = conda_index.index.ChannelIndex(
        str(pkg_dir),
        None,
        write_bz2=False,
        write_zst=False,
        compact_json=True,
        threads=1,
    )

    # Add metadata for a package with features, without having to include it on
    # the filesystem.
    index_cache = channel_index.cache_for_subdir("noarch")
    conn = index_cache.db

    features_pkg_name = "features"
    features_pkg = f"{features_pkg_name}-1.0.conda"
    features_pkg_2 = f"{features_pkg_name}-0.9.conda"

    # The function under test is looking for a package with features, and an
    # older version of the same package without features.
    with conn:  # transaction
        conn.execute(
            f"""INSERT INTO index_json VALUES('{features_pkg}','{{"build":"h39de5ba_0","build_number":0,"depends":[],"name":"{features_pkg_name}","noarch":"generic","subdir":"noarch","timestamp":1561127261940,"version":"1.0","md5":"ba68433ef44982170d4e2f2f9bf89338","sha256":"33877cbe447e8c7a026fbcb7e299b37208ad4bc70cf8328fb4cf552af01ada76","size":2683,"track_features":["jim"],"features":["jim"]}}');"""
        )
        conn.execute(
            f"""INSERT INTO stat VALUES('indexed','{features_pkg}',1652905054,2683,'33877cbe447e8c7a026fbcb7e299b37208ad4bc70cf8328fb4cf552af01ada76','ba68433ef44982170d4e2f2f9bf89338',NULL,NULL);"""
        )
        conn.execute(
            f"""INSERT INTO stat VALUES('fs','{features_pkg}',1652905054,2683,NULL,NULL,NULL,NULL);"""
        )

        conn.execute(
            f"""INSERT INTO index_json VALUES('{features_pkg_2}','{{"build":"h39de5ba_0","build_number":0,"depends":[],"name":"{features_pkg_name}","noarch":"generic","subdir":"noarch","timestamp":1561127261940,"version":"0.9","md5":"ba68433ef44982170d4e2f2f9bf89338","sha256":"33877cbe447e8c7a026fbcb7e299b37208ad4bc70cf8328fb4cf552af01ada76","size":2683}}');"""
        )
        conn.execute(
            f"""INSERT INTO stat VALUES('indexed','{features_pkg_2}',1652905054,2683,'33877cbe447e8c7a026fbcb7e299b37208ad4bc70cf8328fb4cf552af01ada76','ba68433ef44982170d4e2f2f9bf89338',NULL,NULL);"""
        )
        conn.execute(
            f"""INSERT INTO stat VALUES('fs','{features_pkg_2}',1652905054,2683,NULL,NULL,NULL,NULL);"""
        )

        # cover "run exports on .conda package" branch
        conn.execute(f"""INSERT INTO run_exports VALUES('{features_pkg_2}','{{}}')""")

        # cover "check for unknown file extension" branch
        conn.execute(
            """INSERT INTO stat VALUES('fs','unexpected-filename',1652905054,2683,NULL,NULL,NULL,NULL)"""
        )
        conn.execute("""INSERT INTO index_json VALUES('unexpected-filename','{}');""")

    # Call internal "write repodata.json" function normally called by
    # channel_index.index(). index_prepared_subdir doesn't check which packages
    # exist.
    channel_index.index_prepared_subdir("noarch", False, False, None, None)

    # complain about 'unexpected-filename'
    channel_index.build_run_exports_data("noarch")


def test_bad_patch_version(index_data):
    """
    Test unsupported patches.
    """
    pkg_dir = Path(index_data, "packages")

    # compact json
    channel_index = conda_index.index.ChannelIndex(
        str(pkg_dir),
        None,
        write_bz2=False,
        write_zst=False,
        compact_json=True,
        threads=1,
    )

    instructions = Path(__file__).parents[1] / "tests" / "gen_patch_2.py"

    with pytest.raises(RuntimeError, match="Incompatible"):
        channel_index._create_patch_instructions(
            "noarch", {"packages": {}}, patch_generator=str(instructions)
        )


def test_base_url(index_data):
    """
    conda-index should be able to add base_url to repodata.json.
    """
    pkg_dir = Path(index_data, "packages")

    # compact json
    channel_index = conda_index.index.ChannelIndex(
        pkg_dir,
        None,
        write_bz2=False,
        write_zst=False,
        compact_json=True,
        threads=1,
        base_url="https://example.org/somechannel/",
    )

    channel_index.index(None)

    osx = json.loads((pkg_dir / "osx-64" / "repodata.json").read_text())
    noarch = json.loads((pkg_dir / "noarch" / "repodata.json").read_text())

    assert osx["repodata_version"] == 2

    assert osx["info"]["base_url"] == "https://example.org/somechannel/osx-64/"
    assert noarch["info"]["base_url"] == "https://example.org/somechannel/noarch/"

    package_url = urllib.parse.urljoin(osx["info"]["base_url"], "package-1.0.conda")
    assert package_url == "https://example.org/somechannel/osx-64/package-1.0.conda"


def test_write_current_repodata(index_data):
    """
    Test that we can skip current_repodata, and that it deletes the old one.
    """
    pkg_dir = Path(index_data, "packages")
    pattern = "*/current_repodata.json*"

    # compact json
    channel_index = conda_index.index.ChannelIndex(
        str(pkg_dir),
        None,
        write_bz2=True,
        write_zst=True,
        compact_json=True,
        threads=1,
    )

    channel_index.index(None)

    assert list(pkg_dir.glob(pattern))

    channel_index.write_current_repodata = False
    channel_index.index(None)

    assert not list(pkg_dir.glob(pattern))
