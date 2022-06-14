import pathlib
import tempfile

from conda_index.utils import file_contents_match


def test_file_contents_match():
    """
    Assert file_contents_match works correctly with different and same length
    files, and files with different and same filenames. To allow for compare
    size optimization.
    """
    with tempfile.TemporaryDirectory() as directory:
        a = pathlib.Path(directory, "a.txt")
        a.write_text("matching length A")
        b = pathlib.Path(directory, "b.txt")
        b.write_text("matching length B")
        c = pathlib.Path(directory, "c.txt")
        c.write_text("different length")
        d = pathlib.Path(directory, "d.txt")
        d.write_text("different length")

        assert not file_contents_match(a, b)
        assert not file_contents_match(a, c)

        assert file_contents_match(c, c)
        assert file_contents_match(c, d)
