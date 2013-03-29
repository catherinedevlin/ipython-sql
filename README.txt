===========
ipython-sql
===========

:Author: Catherine Devlin, http://catherinedevlin.blogspot.com

Introduces a %sql / %%sql magic.  

Connect to a database, using SQLAlchemy connect strings, then issue SQL
commands within IPython or IPython Notebook.

.. image:: examples/writers.png
   :width: 600px
   :alt: ipython-sql notepad usage screenshot

Examples::

    In [1]: %load_ext sql

    In [2]: %%sql postgres://will:longliveliz@localhost/shakes
       ...: select * from character
       ...: where abbrev = 'ALICE'
       ...: 
    Out[2]: [(u'Alice', u'Alice', u'ALICE', u'a lady attending on Princess Katherine', 22)]
  
    In [3]: result = _
   
    In [4]: print(result)
    charid   charname   abbrev                description                 speechcount 
    =================================================================================
    Alice    Alice      ALICE    a lady attending on Princess Katherine   22         
                                                 
    In [4]: result.keys
    Out[5]: [u'charid', u'charname', u'abbrev', u'description', u'speechcount']
    
    In [6]: result[0][0]
    Out[6]: u'Alice'
    
    In [7]: result[0].description
    Out[7]: u'a lady attending on Princess Katherine'
                                                 
After the first connection, connect info can be omitted::

    In [8]: %sql select count(*) from work
    Out[8]: [(43L,)]
   
Connections to multiple databases can be maintained.  You can refer to 
an existing connection by username@database::

    In [9]: %%sql will@shakes
       ...: select charname, speechcount from character 
       ...: where  speechcount = (select max(speechcount) 
       ...:                       from character);
       ...: 
    Out[9]: [(u'Poet', 733)]
    
    In [10]: print(_)
    charname   speechcount 
    ======================
    Poet       733  
   
You may use multiple SQL statements inside a single cell, but you will
only see any query results from the last of them, so this really only
makes sense for statements with no output::

    In [11]: %%sql sqlite://
       ....: CREATE TABLE writer (first_name, last_name, year_of_death);
       ....: INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
       ....: INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
       ....:     
    Out[11]: []   

Connecting
----------

Connection strings are `SQLAlchemy`_ standard.

Some example connection strings::

    mysql+pymysql://scott:tiger@localhost/foo
    oracle://scott:tiger@127.0.0.1:1521/sidname
    sqlite://
    sqlite:///foo.db
    
.. _SQLAlchemy: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls

Configuration
-------------
    
Query results are loaded as lists, so very large result sets may use up
your system's memory.  There is no autolimit by default.

You can set an autolimit by adding this to your
`ipython_config.py` file::

    c.SqlMagic.autolimit = 1000 
 
You can similarly change the table printing style to any of `prettytable`_'s
defined styles (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM):

    c.SqlMagic.style = 'PLAIN_COLUMNS'
    
You can create and find your `ipython_config.py` file from
the command line::

    ipython profile create
    ipython locate profile
    
See http://ipython.org/ipython-doc/stable/config/overview.html#configuration-objects-and-files  
for more details on IPython configuration. 

.. _prettytable: http://code.google.com/p/prettytable/wiki/Tutorial

Development
-----------

https://github.com/catherinedevlin/ipython-sql

Credits
-------

- Matthias Bussonnier for help with configuration
- `Distribute`_
- `Buildout`_
- `modern-package-template`_

.. _Buildout: http://www.buildout.org/
.. _Distribute: http://pypi.python.org/pypi/distribute
.. _`modern-package-template`: http://pypi.python.org/pypi/modern-package-template
