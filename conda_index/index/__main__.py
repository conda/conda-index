# normally run with "conda index" or bin/conda-index
# CONDA_DEBUG=1 python -m conda_build index --verbose ...
import logging

from . import logutil

# sets up timestamped log lines nicely but can conflict with conda logging by
# printing messages twice
# conda resets logging on each subdir
logutil.configure()

import conda_index.cli.main_index

conda_index.cli.main_index.main()
