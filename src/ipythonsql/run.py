import sqlalchemy
import connection

def run(conn, sql):
    if sql.strip():
        statement = sqlalchemy.sql.text(sql)
        return conn.session.execute(statement)
    else:
        return 'Connected to %s' % conn.name