from __future__ import annotations

import sys
from io import StringIO
from unittest.mock import patch

import pytest

from conda_index.cli import cli


@pytest.mark.skip(reason="causes many other tests to fail")
def test_cli(tmp_path):
    """
    Coverage testing for the argparse cli.
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

    # fancy separate output directory run
    args = [
        f"--output={output_dir}",
        f"--current-index-versions-file={current_index_versions}",
        "--channeldata",
        str(tmp_path),
        "--rss",
        "--bz2",
        "--zst",
        "--verbose",
    ]

    try:
        cli(args)
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"
    assert (output_dir / "channeldata.json").exists()

    # plain run
    assert not (tmp_path / "noarch" / "repodata.json").exists()
    cli([str(tmp_path)])
    assert not (tmp_path / "channeldata.json").exists()
    assert (tmp_path / "noarch" / "repodata.json").exists()


@pytest.mark.parametrize(
    "cli_option", ["--current-repodata", "--run-exports", "--channeldata"]
)
def test_mutual_exclusion_mononlithic_repodata(cli_option: str, tmp_path):
    """Test that 'cli_option' is blocked when repodata.json is not written."""

    # Capture stderr to check for error message
    with patch("sys.stderr", new_callable=StringIO) as mock_stderr:
        with pytest.raises(SystemExit) as exc_info:
            cli(["--no-write-monolithic", cli_option, str(tmp_path)])

        assert exc_info.value.code == 1
        error_output = mock_stderr.getvalue()
        assert "Conflicting arguments" in error_output
        assert cli_option in error_output
