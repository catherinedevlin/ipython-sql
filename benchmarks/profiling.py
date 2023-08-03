"""
Sample script to profile the sql magic.

>>> pip install line_profiler
>>> kernprof -lv profiling.py
"""
from sql.magic import SqlMagic
from IPython import InteractiveShell
import duckdb
from pandas import DataFrame
import numpy as np

num_rows = 1_000_000
num_cols = 50

df = DataFrame(np.random.randn(num_rows, num_cols))

magic = SqlMagic(InteractiveShell())

conn = duckdb.connect()
magic.execute(line="conn --alias duckdb", local_ns={"conn": conn})
magic.autopandas = True
magic.displaycon = False


# NOTE: you can put the @profile decorator on any internal function to profile it
# the @profile decorator is injected by the line_profiler package at runtime, to learn
# more, see: https://github.com/pyutils/line_profiler
# e.g., to check the magic performance, you can add @profile to the _execute function
def run_magic():
    magic.execute("SELECT * FROM df")


if __name__ == "__main__":
    run_magic()
