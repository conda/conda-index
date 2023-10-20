"""
Conda command plugin, provide "conda reindex" command, renamed to
avoid clash with old "conda index" CLI.
"""

import conda.plugins

def command(arguments: list[str]):
     import conda_index.cli
     conda_index.cli.cli()

@conda.plugins.hookimpl
def conda_subcommands():
    yield conda.plugins.CondaSubcommand(
        name="reindex",
        action=command,
        summary="Update package index metadata files.  Replaces `conda index`."
    )

