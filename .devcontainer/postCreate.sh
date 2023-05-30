#!/bin/bash -x

conda init bash

# Perform install instructions from
# https://ploomber-contributing.readthedocs.io/en/latest/contributing/setup.html
conda create --name ploomber-base python=3.10 --yes
conda activate ploomber-base
pip install pkgmt
pkgmt setup --doc

# After the devcontainer comes up, you can just enable the jupysql conda env:
# conda activate jupysql