from sql.connection import censor_passwords

def test_censor_passwords_PWD():
    s1 = 'vertica+pyodbc:///?odbc_connect=DRIVER=/opt/vertica/lib64/libverticaodbc.so;SERVER=<server>;DATABASE=<db>;PORT=5433;UID=<uid>;PWD=TESTPWD'
    assert 'TESTPWD' in s1
    censored1 = censor_passwords(s1)
    assert 'TESTPWD' not in censored1

def test_censor_passwords_PWD():
    s2 = 'vertica+pyodbc:///?odbc_connect=DRIVER=/opt/vertica/lib64/libverticaodbc.so;SERVER=<server>;DATABASE=<db>;PORT=5433;UID=<uid>;password=TESTPWD'
    assert 'TESTPWD' in s2
    censored2 = censor_passwords(s2)
    assert 'TESTPWD' not in censored2