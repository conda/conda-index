import os
import shutil
import sys
from collections import defaultdict
from pathlib import Path

import pytest
from conda_build.config import (
    Config,
    _src_cache_root_default,
    conda_pkg_format_default,
    enable_static_default,
    error_overdepending_default,
    error_overlinking_default,
    exit_on_verify_error_default,
    filename_hashing_default,
    ignore_verify_codes_default,
    no_rewrite_stdout_env_default,
)
from conda_build.metadata import MetaData
from conda_build.utils import check_call_env, copy_into, prepend_bin_path
from conda_build.variants import get_default_variant

from .http_test_server import run_test_server


@pytest.fixture(scope="function")
def testing_workdir(tmp_path: Path, request):
    """Create a workdir in a safe temporary folder; cd into dir above before test, cd out after

    :param tmpdir: py.test fixture, will be injected
    :param request: py.test fixture-related, will be injected (see pytest docs)
    """

    saved_path = os.getcwd()

    os.chdir(tmp_path)
    # temporary folder for profiling output, if any
    (tmp_path / "prof").mkdir()

    def return_to_saved_path():
        if os.path.isdir(os.path.join(saved_path, "prof")):
            profdir = tmp_path / "prof"
            files = profdir.glob("*.prof") if profdir.is_dir() else []

            for f in files:
                copy_into(str(f), os.path.join(saved_path, "prof", f.name))
        os.chdir(saved_path)

    request.addfinalizer(return_to_saved_path)

    return str(tmp_path)


@pytest.fixture(scope="function")
def testing_homedir(tmpdir, request):
    """Create a homedir in the users home directory; cd into dir above before test, cd out after

    :param tmpdir: py.test fixture, will be injected
    :param request: py.test fixture-related, will be injected (see pytest docs)
    """

    saved_path = os.getcwd()
    d1 = os.path.basename(tmpdir)
    d2 = os.path.basename(os.path.dirname(tmpdir))
    d3 = os.path.basename(os.path.dirname(os.path.dirname(tmpdir)))
    new_dir = os.path.join(os.path.expanduser("~"), d1, d2, d3, "pytest.conda-build")
    # While pytest will make sure a folder in unique
    if os.path.exists(new_dir):
        import shutil

        try:
            shutil.rmtree(new_dir)
        except:
            pass
    try:
        os.makedirs(new_dir)
    except:
        print(f"Failed to create {new_dir}")
        return None
    os.chdir(new_dir)

    def return_to_saved_path():
        os.chdir(saved_path)

    request.addfinalizer(return_to_saved_path)

    return str(new_dir)


@pytest.fixture(scope="function")
def testing_config(testing_workdir):
    def boolify(v):
        return True if "v" == "true" else False

    result = Config(
        croot=testing_workdir,
        anaconda_upload=False,
        verbose=True,
        activate=False,
        debug=False,
        variant=None,
        test_run_post=False,
        # These bits ensure that default values are used instead of any
        # present in ~/.condarc
        filename_hashing=filename_hashing_default,
        _src_cache_root=_src_cache_root_default,
        error_overlinking=boolify(error_overlinking_default),
        error_overdepending=boolify(error_overdepending_default),
        enable_static=boolify(enable_static_default),
        no_rewrite_stdout_env=boolify(no_rewrite_stdout_env_default),
        ignore_verify_codes=ignore_verify_codes_default,
        exit_on_verify_error=exit_on_verify_error_default,
        conda_pkg_format=conda_pkg_format_default,
    )
    assert result._src_cache_root is None
    assert result.src_cache_root == testing_workdir
    return result


@pytest.fixture(scope="function")
def testing_metadata(request, testing_config):
    d = defaultdict(dict)
    d["package"]["name"] = request.function.__name__
    d["package"]["version"] = "1.0"
    d["build"]["number"] = "1"
    d["build"]["entry_points"] = []
    d["requirements"]["build"] = []
    d["requirements"]["run"] = []
    d["test"]["commands"] = ['echo "A-OK"', "exit 0"]
    d["about"]["home"] = "sweet home"
    d["about"]["license"] = "contract in blood"
    d["about"]["summary"] = "a test package"
    d["about"]["tags"] = ["a", "b"]
    d["about"]["identifiers"] = "a"
    testing_config.variant = get_default_variant(testing_config)
    testing_config.variants = [testing_config.variant]
    return MetaData.fromdict(d, config=testing_config)


@pytest.fixture(scope="function")
def testing_env(testing_workdir, request, monkeypatch):
    env_path = os.path.join(testing_workdir, "env")

    check_call_env(
        [
            "conda",
            "create",
            "-yq",
            "-p",
            env_path,
            "python={}".format(".".join(sys.version.split(".")[:2])),
        ]
    )
    monkeypatch.setenv(
        "PATH",
        prepend_bin_path(os.environ.copy(), env_path, prepend_prefix=True)["PATH"],
    )
    # cleanup is done by just cleaning up the testing_workdir
    return env_path


# these are functions so that they get regenerated each time we use them.
#    They could be fixtures, I guess.
@pytest.fixture(scope="function")
def numpy_version_ignored():
    return {
        "python": ["2.7.*", "3.5.*"],
        "numpy": ["1.10.*", "1.11.*"],
        "ignore_version": ["numpy"],
    }


@pytest.fixture(scope="function")
def single_version():
    return {"python": "2.7.*", "numpy": "1.11.*"}


@pytest.fixture(scope="function")
def no_numpy_version():
    return {"python": ["2.7.*", "3.5.*"]}


@pytest.fixture()
def index_data(tmp_path: Path):
    """
    Copy tests/index_data to avoid writing cache to repository.

    Could be made session-scoped if we don't mind re-using the index cache
    during tests.
    """
    index_data = Path(__file__).parents[0] / "index_data"
    shutil.copytree(index_data, tmp_path / "index_data")
    return tmp_path / "index_data"


@pytest.fixture(scope="session")
def http_package_server():
    """Open a local web server to test remote support files."""
    base = Path(__file__).parents[0] / "index_data" / "packages"
    http = run_test_server(str(base))
    yield http
    # shutdown is checked at a polling interval, or the daemon thread will shut
    # down when the test suite exits.
    http.shutdown()


@pytest.fixture(scope="session")
def postgresql_database(tmp_path_factory):
    # ensure we can run the rest of the test suite without sqlalchemy
    from . import postgresql_fixture

    tmp_path = tmp_path_factory.mktemp("db")
    yield from postgresql_fixture.postgresql_fixture(tmp_path)
