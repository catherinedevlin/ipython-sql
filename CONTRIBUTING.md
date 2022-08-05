# Contributing

## Postgres database

We use a postgres database for the `plot.md` example.

[macOS installation](https://stackoverflow.com/a/49689589/709975)

```sh
brew install libpq                                             1 ↵

# get sample data
curl -O https://raw.githubusercontent.com/lerocha/chinook-database/master/ChinookDatabase/DataSources/Chinook_PostgreSql.sql

# load data
psql -h $HOST -p $PORT -U $USER -W $DB -f Chinook_PostgreSql.sql

# load large table
cd scripts
python large-table-gen.py
psql -h $HOST -p $PORT -U $USER -W $DB -f large-table.sql

# start console
psql -h $HOST -p $PORT -U $USER -W $DB
```
