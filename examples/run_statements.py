from sql.run import run
from sqlalchemy import create_engine
from sql.connection import SQLAlchemyConnection
from sql.magic import SqlMagic
from IPython.core.interactiveshell import InteractiveShell

ip = InteractiveShell()

sqlmagic = SqlMagic(shell=ip)
ip.register_magics(sqlmagic)

# Modify config options if needed
sqlmagic.feedback = 1
sqlmagic.autopandas = True

conn = SQLAlchemyConnection(create_engine("duckdb://"))

run.run_statements(conn, "CREATE TABLE numbers (num INTEGER)", config=sqlmagic)
run.run_statements(conn, "INSERT INTO numbers values (1)", config=sqlmagic)
run.run_statements(conn, "INSERT INTO numbers values (2)", config=sqlmagic)
run.run_statements(conn, "INSERT INTO numbers values (1)", config=sqlmagic)

query_result = run.run_statements(conn, "SELECT * FROM numbers", config=sqlmagic)
print(query_result)
