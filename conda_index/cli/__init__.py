"""
Updated command line interface for conda-index.
"""

import logging
import os.path
from pathlib import Path

import click

from conda_index.index import MAX_THREADS_DEFAULT, ChannelIndex, logutil

from .. import yaml


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("dir")
@click.option("--output", help="Output repodata to given directory.")
@click.option(
    "--subdir",
    multiple=True,
    default=None,
    help="Subdir to index. Accepts multiple.",
)
@click.option(
    "-n",
    "--channel-name",
    help="Customize the channel name listed in each channel's index.html.",
)
@click.option(
    "--patch-generator",
    required=False,
    help="Path to Python file that outputs metadata patch instructions from its "
    "_patch_repodata function or a .tar.bz2/.conda file which contains a "
    "patch_instructions.json file for each subdir",
)
@click.option(
    "--channeldata/--no-channeldata",
    help="Generate channeldata.json. Conflicts with --no-write-monolithic.",
    default=False,
    show_default=True,
)
@click.option(
    "--rss/--no-rss",
    help="Write rss.xml (Only if --channeldata is enabled).",
    default=True,
    show_default=True,
)
@click.option(
    "--bz2/--no-bz2",
    help="Write repodata.json.bz2.",
    default=False,
    show_default=True,
)
@click.option(
    "--zst/--no-zst",
    help="Write repodata.json.zst.",
    default=False,
    show_default=True,
)
@click.option(
    "--run-exports/--no-run-exports",
    help="Write run_exports.json. Conflicts with --no-write-monolithic.",
    default=False,
    show_default=True,
)
@click.option(
    "--compact/--no-compact",
    help="Output JSON as one line, or pretty-printed.",
    default=True,
    show_default=True,
)
@click.option(
    "--current-index-versions-file",
    "-m",
    help="""
        YAML file containing name of package as key, and list of versions as values.  The current_index.json
        will contain the newest from this series of versions.  For example:

        python:
          - 3.8
          - 3.9

        will keep python 3.8.X and 3.9.Y in the current_index.json, instead of only the very latest python version.
        """,
)
@click.option(
    "--base-url",
    help="""
        If packages should be served separately from repodata.json, URL of the
        directory tree holding packages. Generates repodata.json with
        repodata_version=2 which is supported in conda 24.5.0 or later.
        """,
)
@click.option(
    "--update-cache/--no-update-cache",
    help="""
        Control whether listdir() is called to refresh the set of available
        packages. Used to generate complete repodata.json from cache only when
        packages are not on disk. (Experimental)
        """,
    default=True,
    show_default=True,
)
@click.option(
    "--upstream-stage",
    help="""
    Set to 'clone' to generate example repodata from conda-forge test database.
    (Experimental)
    """,
    default="fs",
)
@click.option(
    "--current-repodata/--no-current-repodata",
    help="""
        Skip generating current_repodata.json, a file containing only the newest
        versions of all packages and their dependencies, only used by the
        classic solver. Conflicts with --no-write-monolithic.
        """,
    default=True,
    show_default=True,
)
@click.option("--threads", default=MAX_THREADS_DEFAULT, show_default=True)
@click.option(
    "--verbose",
    help="""
        Enable debug logging.
        """,
    default=False,
    is_flag=True,
)
@click.option(
    "--write-monolithic/--no-write-monolithic",
    help="""
    Write repodata.json with all package metadata in a single file.
    """,
    default=True,
    is_flag=True,
)
@click.option(
    "--write-shards/--no-write-shards",
    help="""
        Write a repodata.msgpack.zst index and many smaller files per CEP-16.
        (Experimental)
        """,
    default=False,
    is_flag=True,
)
@click.option(
    "--db",
    help="""
        Choose database backend. "sqlite3" (default) or "postgresql"
        (Experimental)
        """,
    default="sqlite3",
    type=click.Choice(["sqlite3", "postgresql"]),
)
@click.option(
    "--db-url",
    help="""
        SQLAlchemy database URL when using --db=postgresql. Alternatively, use
        the CONDA_INDEX_DBURL environment variable. (Experimental)
        """,
    default="postgresql:///conda_index",
    show_default=True,
    envvar="CONDA_INDEX_DBURL",
)
@click.option(
    "--html-dependencies/--no-html-dependencies",
    help="""
        Include dependency popups in generated HTML index files.
        May significantly increase file size for large repositories like
        main or conda-forge.
        """,
    default=False,
    show_default=True,
)
def cli(
    dir,
    patch_generator=None,
    subdir=None,
    output=None,
    channeldata=False,
    verbose=False,
    threads=None,
    current_index_versions_file=None,
    channel_name=None,
    bz2=False,
    zst=False,
    rss=False,
    run_exports=False,
    compact=True,
    base_url=None,
    update_cache=False,
    upstream_stage="fs",
    current_repodata=True,
    write_monolithic=True,
    write_shards=False,
    db="sqlite3",
    db_url="",
    html_dependencies=False,
):
    logutil.configure()
    if verbose:
        logging.getLogger("conda_index.index").setLevel(logging.DEBUG)

    if output:
        output = os.path.expanduser(output)

    if not write_monolithic:
        # --current-repodata, --run-exports, and --channeldata are only supported with --write-monolithic
        incompatible_args = []
        if current_repodata:
            incompatible_args.append("--current-repodata")
        if run_exports:
            incompatible_args.append("--run-exports")
        if channeldata:
            incompatible_args.append("--channeldata")

        if incompatible_args:
            args_str = ", ".join(incompatible_args)
            raise click.ClickException(
                f"Conflicting arguments: {args_str} require(s) --write-monolithic (the default setting)."
            )

    cache_kwargs = {}

    if db == "postgresql":
        try:
            import conda_index.postgres.cache

            cache_class = conda_index.postgres.cache.PsqlCache
            cache_kwargs["db_url"] = db_url
        except ImportError as e:
            raise click.ClickException(f"Missing dependencies for postgresql: {e}")
    else:
        from conda_index.index.sqlitecache import CondaIndexCache

        cache_class = CondaIndexCache

    channel_index = ChannelIndex(
        os.path.expanduser(dir),
        channel_name=channel_name,
        output_root=output,
        subdirs=subdir,
        write_bz2=bz2,
        write_zst=zst,
        threads=threads,
        write_run_exports=run_exports,
        compact_json=compact,
        base_url=base_url,
        save_fs_state=update_cache,
        write_current_repodata=current_repodata,
        upstream_stage=upstream_stage,
        write_monolithic=write_monolithic,
        write_shards=write_shards,
        cache_class=cache_class,
        cache_kwargs=cache_kwargs,
        html_dependencies=html_dependencies,
    )

    if update_cache is False:
        # We call listdir() in save_fs_state, or its remote fs equivalent; then
        # we call changed_packages(); but the changed_packages query against a
        # remote filesystem is different than the one we need for a local
        # filesystem. How about skipping the extract packages stage entirely by
        # returning no changed packages? Might fail if we use
        # threads/multiprocessing.
        def no_changed_packages(self, *args):
            return []

        channel_index.cache_class.changed_packages = no_changed_packages

    current_index_versions = None
    if current_index_versions_file:
        with open(current_index_versions_file) as f:
            current_index_versions = yaml.safe_load(f)

    if patch_generator:
        patch_generator = str(Path(patch_generator).expanduser())

    channel_index.index(
        patch_generator=patch_generator,  # or will use outdated .py patch functions
        current_index_versions=current_index_versions,
        progress=False,  # clone is a batch job
    )

    if channeldata:  # about 2 1/2 minutes for conda-forge
        channel_index.update_channeldata(rss=rss)
