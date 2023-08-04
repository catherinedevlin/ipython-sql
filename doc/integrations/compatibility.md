# Compatibility

```{note}
These table reflects the compatibility status of JupySQL `>=0.7`
```

## DuckDB

**Full compatibility**

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ✅
- Plotting with `%%sqlplot bar` ✅
- Plotting with `%%sqlplot pie` ✅
- Plotting with `%%sqlplot histogram` ✅
- Plotting with `ggplot` API ✅
- Profiling tables with `%sqlcmd profile` ✅
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅

## Snowflake

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ❓
- Plotting with `%%sqlplot bar` ❓
- Plotting with `%%sqlplot pie` ❓
- Plotting with `%%sqlplot histogram` ❓
- Plotting with `ggplot` API ❓
- Profiling tables with `%sqlcmd profile` ❓
- Listing tables with `%sqlcmd tables` ❓
- Listing columns with `%sqlcmd columns` ❓
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅

## Redshift

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ✅
- Plotting with `%%sqlplot bar` ✅
- Plotting with `%%sqlplot pie` ✅
- Plotting with `%%sqlplot histogram` ✅
- Plotting with `ggplot` API ✅
- Profiling tables with `%sqlcmd profile` ✅
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅

## PostgreSQL

**Almost full compatibility**

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ✅
- Plotting with `%%sqlplot bar` ✅
- Plotting with `%%sqlplot pie` ✅
- Plotting with `%%sqlplot histogram` ✅
- Plotting with `ggplot` API ❓
- Profiling tables with `%sqlcmd profile` ✅
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅


## MariaDB / MySQL

**Almost full compatibility**

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ❌
- Plotting with `%%sqlplot bar` ❓
- Plotting with `%%sqlplot pie` ❓
- Plotting with `%%sqlplot histogram` ✅
- Plotting with `ggplot` API ✅ (partial support)
- Profiling tables with `%sqlcmd profile` ✅
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅

## SQL Server

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ✅
- Plotting with `%%sqlplot bar` ✅
- Plotting with `%%sqlplot pie` ✅
- Plotting with `%%sqlplot histogram` ❌
- Plotting with `ggplot` API ✅
- Profiling tables with `%sqlcmd profile` ✅
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅

## Oracle Database

- Running queries with `%%sql` ✅
- CTEs with `%%sql --save NAME` ✅
- Plotting with `%%sqlplot boxplot` ❌
- Plotting with `%%sqlplot bar` ❓
- Plotting with `%%sqlplot pie` ❓
- Plotting with `%%sqlplot histogram` ❌
- Plotting with `ggplot` API ❌
- Profiling tables with `%sqlcmd profile` ❌
- Listing tables with `%sqlcmd tables` ✅
- Listing columns with `%sqlcmd columns` ✅
- Parametrized SQL queries via `{{parameter}}` ✅
- Interactive SQL queries via `--interact` ✅