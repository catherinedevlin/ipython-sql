ipython-sql
===========

Introduces a %%sql magic.  

Connect to a database, using SQLAlchemy connect strings, then issue SQL
commands within IPython or IPython Notebook.

Examples::

    In [1]: %load_ext sql

    In [2]: %%sql postgres://will:longliveliz@localhost/shakes
       ...: select * from character
       ...: where abbrev = 'ALICE'
       ...: 
       charid        charname        abbrev      description    speechcount  
    ========================================================================
    Alice          Alice          ALICE          a lady         22           
                                                 attending on                
                                                 Princess                    
                                                 Katherine                   
    Out[2]: <sql.run.PrettyProxy at 0x26eebd0>
   
After the first connection, connect info can be omitted::

    In [3]: %%sql
       ...: select count(*) from work;
       ...: 
    count 
    =====
    43    
    Out[3]: <sql.run.PrettyProxy at 0x273cd90>

Connections to multiple databases can be maintained.  You can refer to 
an existing connection by username@database::

    In [4]: %%sql will@shakes
       ...: select charname, speechcount from character 
       ...: where  speechcount = (select max(speechcount) 
       ...:                       from character);
       ...: 
    charname   speechcount 
    ======================
    Poet       733         
    Out[4]: <sql.run.PrettyProxy at 0x273d490>
    