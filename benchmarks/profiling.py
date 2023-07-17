"""
Sample script to profile the sql magic.
"""
from sql.magic import SqlMagic
from IPython import InteractiveShell
import duckdb
from pandas import DataFrame
import numpy as np

num_rows = 1000_000

df = DataFrame(np.random.randn(num_rows, 20))

magic = SqlMagic(InteractiveShell())

conn = duckdb.connect()
magic.execute(line="conn --alias duckdb", local_ns={"conn": conn})
magic.autopandas = True
magic.displaycon = False


# NOTE: you can put the @profile decorator on any internal function to profile it
# the @profile decorator is injected by the line_profiler package at runtime, to learn
# more, see: https://github.com/pyutils/line_profiler
@profile  # noqa
def run_magic():
    magic.execute("SELECT * FROM df")


if __name__ == "__main__":
    run_magic()
