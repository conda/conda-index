import pathlib
import tempfile

from conda_index.index.convert_cache import ichunked
from conda_index.utils import file_contents_match, human_bytes


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


def test_ichunked():
    """
    Test laziness of our version of ichunked.
    """
    CHUNK_SIZE = 5  # not divisible into total
    TOTAL = 32
    REMAINDER = TOTAL - (TOTAL // CHUNK_SIZE) * CHUNK_SIZE

    consumed = -1
    generated = 0

    def counter():
        nonlocal consumed
        consumed += 1
        return consumed

    def counters():
        nonlocal generated
        for i in range(TOTAL):
            generated = i
            yield i, counter

    print("More lazy version")
    for chunk in ichunked(counters(), CHUNK_SIZE):
        print("Batch")
        chunk_size = 0
        for i, c in chunk:
            chunk_size += 1
            count = c()
            print(i, generated, count)
            assert i == generated == count
        assert chunk_size == CHUNK_SIZE or chunk_size == REMAINDER

    try:
        from itertools import batched
    except ImportError:
        return

    # demonstrate that generated is sometimes greater than i, c() in
    # gathers-into-tuples implementation
    print("Less lazy version")
    consumed = -1
    for chunk in batched(counters(), CHUNK_SIZE):
        print("Batch")
        for i, c in chunk:
            print(i, generated, c())


def test_human_bytes():
    assert human_bytes(42) == "42 B"
    assert human_bytes(1042) == "1 KB"
    assert human_bytes(10004242) == "9.5 MB"
    assert human_bytes(100000004242) == "93.13 GB"
