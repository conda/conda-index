"""Sphinx configuration for conda-index documentation."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.abspath(".."))

project = html_title = "conda-index"
copyright = "2026, conda contributors"
author = "conda contributors"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_sitemap",
    "sphinxarg.ext",
]

myst_heading_anchors = 3
myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "tasklist",
]

exclude_patterns = ["_build"]

html_theme = "conda_sphinx_theme"

html_theme_options = {
    "navigation_depth": -1,
    "use_edit_page_button": True,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/conda/conda-index",
            "icon": "fa-brands fa-square-github",
            "type": "fontawesome",
        },
        {
            "name": "Zulip",
            "url": "https://conda.zulipchat.com",
            "icon": "fa-brands fa-zulip",
            "type": "fontawesome",
        },
    ],
}

html_context = {
    "github_user": "conda",
    "github_repo": "conda-index",
    "github_version": "main",
    "doc_path": "docs",
}

html_extra_path = ["robots.txt"]
html_baseurl = "https://conda.github.io/conda-index/"

sitemap_locales = [None]
sitemap_url_scheme = "{link}"
