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
                                                 
Set ``wrap`` to ``False`` if you prefer.
    
    In [8]: wrap = False

    In [9]: print(result)
    charid   charname   abbrev                description                 speechcount 
    =================================================================================
    Alice    Alice      ALICE    a lady attending on Princess Katherine   22     
   
After the first connection, connect info can be omitted::

    In [10]: %sql select count(*) from work
    Out[10]: [(43L,)]
   
Connections to multiple databases can be maintained.  You can refer to 
an existing connection by username@database::

    In [11]: %%sql will@shakes
       ....: select charname, speechcount from character 
       ....: where  speechcount = (select max(speechcount) 
       ....:                       from character);
       ....: 
    Out[11]: [(u'Poet', 733)]
    
    In [12]: print(_)
    charname   speechcount 
    ======================
    Poet       733  