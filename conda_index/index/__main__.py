"""
`python -m conda_index.index` is a more debugging-focused entry point compared
to `conda-index` or `python -m conda_index`
"""
# python -m conda_build.index --verbose --no-progress <directory>

from . import logutil

# sets up timestamped log lines nicely but can conflict with conda logging by
# printing messages twice
# conda resets logging on each subdir
# logging may also be reset in subprocesses
logutil.configure()

import logging

import conda_index.cli.main_index

logging.getLogger("conda_index.index").setLevel(logging.DEBUG)


conda_index.cli.main_index.main()
