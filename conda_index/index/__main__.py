"""
`python -m conda_index.index` is a more debugging-focused entry point compared
to `conda-index` or `python -m conda_index`
"""
# CONDA_DEBUG=1 python -m conda_build.index --verbose --no-progress <directory>

from . import logutil

# sets up timestamped log lines nicely but can conflict with conda logging by
# printing messages twice
# conda resets logging on each subdir
logutil.configure()

import logging

logging.getLogger("conda_index.index.sqlitecache").setLevel(logging.DEBUG)

import conda_index.cli.main_index

conda_index.cli.main_index.main()
