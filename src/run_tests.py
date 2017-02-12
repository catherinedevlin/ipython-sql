# coding: utf-8

"""
Small wrapper around IPythons testing runner
It calls iptest with 'tests' as testgroup. You can pass
arguments to nose by appending them after '--'
"""
from __future__ import print_function

from IPython.testing.iptestcontroller import main
import sys

if '--coverage' in sys.argv:
    print("Coverage will not see any data (because tests work via ipythons '.run_cell()'?)")

# add our testgroup at the beginning of the arguments to iptest
sys.argv[1:1] = ["sql"]

main()
