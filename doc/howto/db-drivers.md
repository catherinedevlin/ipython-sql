# Install database drivers

## DuckDB

To connect to a DuckDB database, install `duckdb-engine`:

```sh
%pip install duckdb-engine --quiet
```

## PostgreSQL

We recommend using `psycopg2` to connect to a PostgreSQL database. The most reliable
way to install it is via `conda`:

```sh
# run this in your notebook
%conda install psycopg2 -c conda-forge --yes --quiet
```

If you don't have conda, you can install it with `pip`:

```sh
# run this in your notebook
%pip install psycopg2-binary --quiet
```

Once installed, restart the kernel.