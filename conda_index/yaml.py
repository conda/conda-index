"""
Indirection to preferred yaml library.
"""


from conda.common.serialize import yaml_safe_load as safe_load
from ruamel.yaml.constructor import ConstructorError
from ruamel.yaml.parser import ParserError
from ruamel.yaml.reader import ReaderError
from ruamel.yaml.scanner import ScannerError


def determined_load(string):
    """
    Load YAML, returning {} on error.
    """

    try:
        return safe_load(string)
    except (
        ConstructorError,
        ParserError,
        ScannerError,
        ReaderError,
    ):
        return {}
