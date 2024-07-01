# override synced-from-infra rever.xsh
$ACTIVITIES = ["version_bump", "authors", "changelog"]

$VERSION_BUMP_PATTERNS = [  # These note where/how to find the version numbers
    ('conda_index/__init__.py', r'__version__\s*=.*', '__version__ = "$VERSION"'),
]
