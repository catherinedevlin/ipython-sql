ipython-sql
===========

Introduces a %sql / %%sql magic.  

Connect to a database, using SQLAlchemy connect strings, then issue SQL
commands within IPython or IPython Notebook.

Examples::

    In [1]: %load_ext sql

    In [2]: %%sql postgres://will:longliveliz@localhost/shakes
       ...: select * from character
       ...: where abbrev = 'ALICE'
       ...: 
    Out[2]: [(u'Alice', u'Alice', u'ALICE', u'a lady attending on Princess Katherine', 22)]
  
    In [3]: result = _
   
    In [4]: print(result)
       charid        charname        abbrev      description    speechcount  
    ========================================================================
    Alice          Alice          ALICE          a lady         22           
                                                 attending on                
                                                 Princess                    
                                                 Katherine          
                                                 
    In [4]: result.keys
    Out[5]: [u'charid', u'charname', u'abbrev', u'description', u'speechcount']
    
    In [6]: result[0][0]
    Out[6]: u'Alice'
    
    In [7]: result[0]['description']
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
   
Connecting
----------

Connection strings are `SQLAlchemy`_ standard.

Some example connection strings:

    mysql+pymysql://scott:tiger@localhost/foo
    oracle://scott:tiger@127.0.0.1:1521/sidname
    sqlite://
    sqlite:///foo.db
    
.. _SQLAlchemy: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls

Configuration
-------------
    
Query results are loaded as lists, so very large result sets may use up
your system's memory.  There is no autolimit by default.

You can stop text wrapping and set an autolimit by adding this to your
:file:`ipython_config.py` file::


    c.SqlMagic.wrap = False
    c.SqlMagic.autolimit = 1000 
    
You can create and find your :file:`ipython_config.py` file from
the command line::

    ipython profile create
    ipython locate profile
    
See http://ipython.org/ipython-doc/stable/config/overview.html#configuration-objects-and-files  
for more details on IPython configuration. 

Development
-----------

https://github.com/catherinedevlin/ipython-sql

Credits
-------

- `ipython-sqlitemagic`_ for ideas
- `texttable`_ for command-line table display
- Matthias Bussonnier for help with configuration
- `Distribute`_
- `Buildout`_
- `modern-package-template`_

.. _ipython-sqlitemagic: https://github.com/tkf/ipython-sqlitemagic
.. _texttable: https://pypi.python.org/pypi/texttable
.. _Buildout: http://www.buildout.org/
.. _Distribute: http://pypi.python.org/pypi/distribute
.. _`modern-package-template`: http://pypi.python.org/pypi/modern-package-template
