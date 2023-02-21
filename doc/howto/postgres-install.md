# Install PostgreSQL client

To connect to a PostgreSQL database from Python, you need a client library. We recommend using `psycopg2`, but there are others like `pg8000`, and `asyncpg`. JupySQL supports the [following connectors.](https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#dialect-postgresql)

+++

## Installing `psycopg2`

The simplest way to install `psycopg2` is with the following command:

```sh
pip install psycopg2-binary
```

If you have `conda` installed, it is more reliable to use it:

```sh
conda install psycopg2 -c conda-forge
```


If you have trouble getting it to work, [message us on Slack.](https://ploomber.io/community)
