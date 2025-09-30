from __future__ import annotations

import pytest
from click.testing import CliRunner

from conda_index.cli import cli


@pytest.mark.skip(reason="causes many other tests to fail")
def test_cli(tmp_path):
    """
    Coverage testing for the click cli.
    """

    (tmp_path / "noarch").mkdir()  # makes valid channel
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    current_index_versions = tmp_path / "current_index_versions.yml"

    current_index_versions.write_text(
        """
python:
    - 3.8
    - 3.9
"""
    )

    runner = CliRunner()

    # fancy separate output directory run
    result = runner.invoke(
        cli,
        [
            f"--output={output_dir}",
            f"--current-index-versions-file={current_index_versions}",
            "--channeldata",
            str(tmp_path),
            "--rss",
            "--bz2",
            "--zst",
            "--verbose",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_dir / "channeldata.json").exists()
    # CliRunner isolation prevents checking logging.getLogger("conda.index").level == logging.DEBUG

    # plain run
    assert not (tmp_path / "noarch" / "repodata.json").exists()
    runner.invoke(cli, [str(tmp_path)])
    assert not (tmp_path / "channeldata.json").exists()
    assert (tmp_path / "noarch" / "repodata.json").exists()


def test_current_repodata_validation_with_write_shards_does_not_work(tmp_path):
    """Test --current-repodata only works with --write-monolithic"""
    runner = CliRunner()

    # Test that --current-repodata still requires --write-monolithic when --write-shards is False
    result = runner.invoke(cli, [
        '--no-write-monolithic',
        '--write-shards',
        '--current-repodata',
        str(tmp_path)
    ])

    assert result.exit_code != 0
    assert "--current-repodata requires --write-monolithic" in result.output


@pytest.mark.parametrize("cli_option", ["--current-repodata", "--run-exports", "--channeldata"])
def test_mutual_exclusion_current_repodata(cli_option: str, tmp_path):
    """Test that 'cli_option' is blocked when repodata.json is not written."""
    runner = CliRunner()

    result = runner.invoke(cli, [
        '--no-write-monolithic',
        '--no-write-shards',
        cli_option,
        str(tmp_path)
    ])

    assert result.exit_code != 0
    assert "Arguments mutually exclusive" in result.output
    assert cli_option in result.output
    assert "cannot be used when both --no-write-monolithic and --no-write-shards are specified" in result.output
