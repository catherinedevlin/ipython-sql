# JupySQL vs ipython-sql

JupySQL is an actively maintained fork of [ipython-sql](https://github.com/catherinedevlin/ipython-sql); it is a drop-in replacement for 99% cases with a lot of new features.

## Incompatibilities

If you're migrating from `ipython-sql` to JupySQL, these are the differences (in most cases, no code changes are needed):

- Since `0.6` JupySQL no longer supports old versions of IPython
- Variable expansion is replaced from `{variable}`, `${variable}` to `{{variable}}`

## New features

- [Plotting](../plot) module that allows you to efficiently plot massive datasets without running out of memory.
- JupySQL allows you to break queries into multiple cells with the help of CTEs. [Click here](../compose) to learn more.
- Using `%sqlcmd tables` and `%sqlcmd columns --table/-t` user can quickly explore tables in the database and the columns each table has. [Click here](../user-guide/tables-columns) to learn more.
- [Polars Integration](../integrations/polars) to convert query results to `polars.DataFrame`. `%config SqlMagic.autopolars` can be used to automatically return Polars DataFrames instead of regular result sets.
- Integration tests with PostgreSQL, MariaDB, MySQL, SQLite and DuckDB.