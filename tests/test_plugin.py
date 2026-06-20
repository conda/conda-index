from __future__ import annotations

import sys
from argparse import Namespace
from types import ModuleType

import pytest

import conda_index


def import_plugin():
    pytest.importorskip("conda.plugins.types")

    import conda_index.plugin

    return conda_index.plugin


def test_command_delegates_to_cli(monkeypatch):
    plugin = import_plugin()
    cli = ModuleType("conda_index.cli")
    calls = []
    cli.run = calls.append
    monkeypatch.setitem(sys.modules, "conda_index.cli", cli)
    monkeypatch.setattr(conda_index, "cli", cli, raising=False)

    args = Namespace(dir=".")
    plugin.command(args)

    assert calls == [args]


def test_conda_subcommands_registers_index(monkeypatch):
    plugin = import_plugin()
    conda_build = ModuleType("conda_build")
    conda_build.__version__ = "24.1.0"
    monkeypatch.setitem(sys.modules, "conda_build", conda_build)

    subcommands = list(plugin.conda_subcommands())

    assert len(subcommands) == 1
    assert subcommands[0].name == "index"
    assert subcommands[0].action is plugin.command
    assert subcommands[0].configure_parser is plugin.configure_parser
