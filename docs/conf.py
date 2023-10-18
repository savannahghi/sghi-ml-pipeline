# ruff: noqa

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath("src"))


# -----------------------------------------------------------------------------
# Project information
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
# -----------------------------------------------------------------------------

author = "Savannah Global Health Institute"
copyright = "2023, Savannah Global Health Institute"
project = "sghi-ml-pipeline"


# -----------------------------------------------------------------------------
# General configuration
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration
# -----------------------------------------------------------------------------

extensions = ["sphinx.ext.autodoc", "sphinx.ext.autosummary"]

# Preserve authored syntax for defaults
autodoc_preserve_defaults = True

autodoc_default_flags = {
    "inherited-members": True,
    "show-inheritance": True,
    "special-members": (
        "__enter__",
        "__exit__",
        "__call__",
        "__getattr__",
        "__setattr_",
    ),
}

autodoc_member_order = "groupwise"

autoapi_python_use_implicit_namespaces = True

autosummary_generate = True  # Turn on sphinx.ext.autosummary

exclude_patterns = []

# Be strict about any broken references
nitpicky = True

nitpick_ignore = [
    ("py:class", "TracebackType"),  # Used as type annotation. Only available when type checking
    ("py:class", "sghi.disposable.Disposable"),  # docs aren't published yet
    ("py:class", "sghi.ml_pipeline.domain._PDT"),  # private type annotations
    ("py:class", "sghi.ml_pipeline.domain._RDT"),  # private type annotations
    ("py:exc", "sghi.disposable.ResourceDisposedError"),  # docs aren't published yet
    ("py:func", "sghi.disposable.not_disposed"),  # docs aren't published yet
    ("py:obj", "sghi.ml_pipeline.domain._PDT"),  # private type annotations
    ("py:obj", "sghi.ml_pipeline.domain._RDT"),  # private type annotations
]

templates_path = ["templates"]

root_doc = "index"


# -----------------------------------------------------------------------------
# Options for HTML output
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
# -----------------------------------------------------------------------------

html_logo = "images/sghi_globe.png"

html_static_path = ["static"]

html_theme = "furo"

html_theme_options = {
    "sidebar_hide_name": True,
    "light_css_variables": {
        "color-brand-primary": "#0474AC",  # "blue"
        "color-brand-content": "#0474AC",
    },
    "dark_css_variables": {
        "color-brand-primary": "#C1368C",  # "purple"
        "color-brand-content": "#C1368C",
    },
}


# -----------------------------------------------------------------------------
# Include Python intersphinx mapping to prevent failures
# jaraco/skeleton#51
# -----------------------------------------------------------------------------

extensions += ["sphinx.ext.intersphinx"]
intersphinx_mapping = {
    "peps": ("https://peps.python.org/", None),
    "python": ("https://docs.python.org/3", None),
    "pypackage": ("https://packaging.python.org/en/latest/", None),
    "importlib-resources": (
        "https://importlib-resources.readthedocs.io/en/latest",
        None,
    ),
    "django": (
        "http://docs.djangoproject.com/en/dev/",
        "http://docs.djangoproject.com/en/dev/_objects/",
    ),
}


# -----------------------------------------------------------------------------
# Support tooltips on references
# -----------------------------------------------------------------------------

extensions += ["hoverxref.extension"]
hoverxref_auto_ref = True
hoverxref_intersphinx = [
    "python",
    "pip",
    "pypackage",
    "importlib-resources",
    "django",
]


# -----------------------------------------------------------------------------
# Add support for nice Not Found 404 pages
# -----------------------------------------------------------------------------

extensions += ["notfound.extension"]


# -----------------------------------------------------------------------------
# Add icons (aka "favicons") to documentation
# -----------------------------------------------------------------------------

extensions += ["sphinx_favicon"]
html_static_path += ["images"]  # should contain the folder with icons

# List of dicts with <link> HTML attributes
# static-file points to files in the html_static_path (href is computed)

favicons = [
    {
        "rel": "icon",
        "type": "image/png",
        "static-file": "sghi_globe.png",
        "sizes": "any",
    },
]
