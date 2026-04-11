"""
Exception needed by models/version.py
"""


class InvalidVersionSpec(ValueError):
    def __init__(self, invalid_spec: str, details: str):
        self.invalid_spec = invalid_spec
        self.details = details
        message = "Invalid version '%(invalid_spec)s': %(details)s"
        super().__init__(message)
