"""
`python -m conda_index.index` is a more debugging-focused entry point compared
to `conda-index` or `python -m conda_index`
"""

from . import logutil

# sets up timestamped log lines nicely.
logutil.configure()

import logging

import conda_index.cli.main_index

logging.getLogger("conda_index.index").setLevel(logging.DEBUG)


conda_index.cli.main_index.main()
