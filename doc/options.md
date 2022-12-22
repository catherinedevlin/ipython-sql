---
jupytext:
  cell_metadata_filter: -all
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.14.0
kernelspec:
  display_name: Python 3 (ipykernel)
  language: python
  name: python3
---

# Options


``-l`` / ``--connections``
    List all active connections

``-x`` / ``--close <session-name>`` 
    Close named connection 

``-c`` / ``--creator <creator-function>``
    Specify creator function for new connection

``-s`` / ``--section <section-name>``
    Section of dsn_file to be used for generating a connection string

``-p`` / ``--persist``
    Create a table name in the database from the named DataFrame

``-n`` / ``--no-index``
    Do not persist data frame's index

``--append``
    Like ``--persist``, but appends to the table if it already exists 

``-a`` / ``--connection_arguments <"{connection arguments}">``
    Specify dictionary of connection arguments to pass to SQL driver

``-f`` / ``--file <path>``
    Run SQL from file at this path
