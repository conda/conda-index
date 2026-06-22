import conda.base.constants

import conda_index._vendor.constants
import conda_index.utils


def test_subdirs():
    """
    Check that our list of subdirs is up to date.
    """
    assert (
        conda.base.constants.KNOWN_SUBDIRS
        == conda_index._vendor.constants.KNOWN_SUBDIRS
        == conda_index.utils.DEFAULT_SUBDIRS
    )
