import sqlalchemy
import connection

def run(conn, sql):
    statement = sqlalchemy.sql.text(sql)
    return conn.execute(statement)