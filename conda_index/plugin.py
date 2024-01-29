"""
Conda command plugin, provide "conda index" command, renamed to
avoid clash with old "conda index" CLI.
"""
import conda.plugins

def command(args):
     import conda_index.cli
     return conda_index.cli.cli(prog_name="conda index", args=args)


@conda.plugins.hookimpl
def conda_subcommands():
    yield conda.plugins.CondaSubcommand(
        name="index",
        action=command,
        summary="Update package index metadata files."
    )
