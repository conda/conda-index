# (c) Continuum Analytics, Inc. / http://continuum.io
# All Rights Reserved
#
# conda is distributed under the terms of the BSD 3-clause license.
# Consult LICENSE or http://opensource.org/licenses/BSD-3-Clause


def update_index(
    dir_paths,
    config=None,
    force=False,
    check_md5=False,
    remove=False,
    channel_name=None,
    subdir=None,
    threads=None,
    patch_generator=None,
    verbose=False,
    progress=False,
    hotfix_source_repo=None,
    current_index_versions=None,
    index_file=None,
    **kwargs
):
    import yaml
    import os

    from conda_index.index import update_index
    from conda_build.utils import ensure_list

    dir_paths = [os.path.abspath(path) for path in dir_paths]

    if isinstance(current_index_versions, str):
        with open(current_index_versions) as f:
            current_index_versions = yaml.safe_load(f)

    for path in dir_paths:
        update_index(
            path,
            check_md5=check_md5,
            channel_name=channel_name,
            patch_generator=patch_generator,
            threads=threads,
            verbose=verbose,
            progress=progress,
            hotfix_source_repo=hotfix_source_repo,
            subdirs=ensure_list(subdir),
            current_index_versions=current_index_versions,
        )
