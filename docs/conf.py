# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Yet Another Pika Wrapper"
copyright = "2021, Open Contracting Partnership"
author = "Open Contracting Partnership"

version = "0.2.1"
release = version

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_design",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = []

# -- Extension configuration -------------------------------------------------

autodoc_default_options = {
    "members": None,
    "member-order": "bysource",
}
autodoc_typehints = "description"
autodoc_type_aliases = {
    "ConsumerCallback": "yapw.types.ConsumerCallback",
    "Decode": "yapw.types.Decode",
    "Decorator": "yapw.types.Decorator",
    "Encode": "yapw.types.Encode",
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pika": ("https://pika.readthedocs.io/en/stable/", None),
}
