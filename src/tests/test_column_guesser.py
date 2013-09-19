from sql.magic import execute
import sys
import nose
import re

def test_memory_db():
    execute('', 'sqlite:// CREATE TABLE manycoltbl (name TEXT, y1 REAL, y2 REAL, name2 TEXT, y3 INT)')
    execute('', "sqlite:// INSERT INTO manycoltbl VALUES ('r1-txt1', 1.01, 1.02, 'r1-txt2', 1.04);")
    execute('', "sqlite:// INSERT INTO manycoltbl VALUES ('r2-txt1', 2.01, 2.02, 'r2-txt2', 2.04);")
    execute('', "sqlite:// INSERT INTO manycoltbl VALUES ('r3-txt1', 3.01, 3.02, 'r3-txt2', 3.04);")

    results = execute('', "sqlite:// SELECT name, y1 FROM manycoltbl;")
    results.guess_pie_columns(xlabel_sep="//")
    assert results.ys[0].is_quantity
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.xlabel == ['r1-txt1', 'r2-txt1', 'r3-txt1']

    results.guess_plot_columns()
    assert results.ys == [[1.01, 2.01, 3.01]]
    assert results.x == []
    
    results = execute('', 
        "sqlite:// SELECT name, y1, y2, name2 FROM manycoltbl;")
    results.guess_pie_columns()
    assert results.ys == [[1.02, 2.02, 3.02]]
    assert results.xlabel == ['r1-txt1 1.01 r1-txt2', 
                              'r2-txt1 2.01 r2-txt2', 
                              'r3-txt1 3.01 r3-txt2']

    results.guess_plot_columns()
    assert results.ys == [[1.02, 2.02, 3.02]]
    assert results.x == [1.01, 2.01, 3.01]

    results = execute('', 
        "sqlite:// SELECT name, y1, y2, name2, y3 FROM manycoltbl;")
    results.guess_pie_columns()
    assert results.ys == [[1.04, 2.04, 3.04]]
    assert results.xlabel == ['r1-txt1 1.01 1.02 r1-txt2', 
                              'r2-txt1 2.01 2.02 r2-txt2', 
                              'r3-txt1 3.01 3.02 r3-txt2']

