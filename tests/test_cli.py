from click.testing import CliRunner

from conda_index.cli import cli


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
