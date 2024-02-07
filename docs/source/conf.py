# -*- coding: utf-8 -*- --------------------------------------------------===#
#
#  Copyright 2022-2023 Trovares Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===----------------------------------------------------------------------===#

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../../src'))
print(f"Sys.path: {sys.path}")


# -- Project information -----------------------------------------------------

project = 'trovares_connector'
copyright = '2022-2024, Trovares, Inc.'
author = 'trovares.com'

# The full version, including alpha/beta/rc tags
release = '2.4.0'

# -- General configuration ---------------------------------------------------

# Sphinx extensions. These are Python module names that will be imported.
extensions = [
  'sphinx.ext.autodoc',
  'sphinx.ext.autosummary',
  'sphinx.ext.napoleon',
  'sphinx.ext.mathjax',
  'sphinx.ext.viewcode',
  'myst_nb',
  'sphinx_copybutton',
]
mathjax_path = 'https://cdnjs.cloudflare.com/ajax/libs/mathjax/2.7.1/MathJax.js?config=TeX-AMS-MML_HTMLorMML'

# Combined with the ".. autosummary" directive in rST files and the
# sphinx-apidoc program, this option enables building separate pages for
# each entity requested under the autosummary directive.
autosummary_generate = True

# We generally don't want to document the whole module (since we only have one),
# but we *do* want to generate entries for class methods and attributes
# (members). Note: even though this is an autodoc setting and not an autosummary
# one, it still applies, since sphinx-apidoc simply creates autodoc directives
# in its separate files.
autodoc_default_options = {
  'no-modules': True,
  'members': True,
  'inherited-members': True,
}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = 'sphinx_book_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
#html_static_path = ['_static']

# Combined with the ".. autosummary" directive in rST files and the
# sphinx-apidoc program, this option enables building separate pages for
# each entity requested under the autosummary directive.
autosummary_generate = True

# By default, Sphinx likes to show the .rST source file that created the page.
# Since we auto-generated these with autosummary directives, they're useless.
html_show_sourcelink = False

# These options are specific to the 'sphinx_book_theme' currently used. If/when
# we switch to a custom theme, this will change or may go away entirely.
html_theme_options = {
  'home_page_in_toc' : True,
  'extra_navbar' : '',
  'repository_url' : 'https://github.com/trovares/trovares_connector',
  'path_to_docs' : 'docs/source',
  'repository_branch' : 'main',
  #"use_edit_page_button": True,
  'use_issues_button': True,
  'use_repository_button': True,
  'use_download_button': True,
  'show_toc_level': 2,
}

# Left sidebar title
html_title = "Trovares Connector"

# Don't execute notebooks when executing.
nb_execution_mode = "off"

# Contents of the sidebar. Since we don't have that many entities that we're
# documenting, we just add the list of everything and a search box.
#
# Technically, this dict maps fileglobs to corresponding sidebar formats.
# Since we want one sidebar for everything, we use the ** to match everything.
#html_sidebars = {
#   '**': ['localtoc.html', 'globaltoc.html', 'relations.html', 'searchbox.html' ]
#}

# Configures the page during generation.
def setup(app):
  # This adds a stylesheet 'link' tag to each page using RELATIVE paths rather
  # than a single fixed path if it was added directly to the 'layout.html' file.
  app.add_css_file('trovares.css')

# Napoleon settings.
# This module enable Sphinx to process NumPy and Google docstrings.
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = False
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
