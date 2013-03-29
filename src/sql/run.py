import sqlalchemy
import connection
import types
import jinja2
import texttable
import sys
import IPython.core
   
class ResultSet(list):
    ip = IPython.core.ipapi.get()
    html_template = jinja2.Template(r"""
  <table>
    <tr>
      {% for header in headers %}
      <th>{{ header }}</th>
      {% endfor %}
    </tr>
    {% for row in rows %}
    <tr>
      {% for itm in row %}
      <td>{{ itm }}</td>
      {% endfor %}
    </tr>    
    {% endfor %}
  </table>
    """)
    def __init__(self, sqlaproxy, sql, wrap, limit):
        self.keys = sqlaproxy.keys()
        self.sql = sql
        self.wrap = wrap
        if sqlaproxy.returns_rows:
            if limit:
                list.__init__(self, sqlaproxy.fetchmany(size=limit))
            else:
                list.__init__(self, sqlaproxy.fetchall())
        else:
            list.__init__(self, [])
    def _repr_html_(self):
        return self.html_template.render(headers=self.keys, rows=self)    
    def __str__(self, *arg, **kwarg):
        if ('max_width' not in kwarg) and (not self.wrap):
            kwarg['max_width'] = 0
        tt = texttable.Texttable(*arg, **kwarg)
        tt.set_deco(texttable.Texttable.HEADER)
        tt.header(self.keys)
        for row in self:
            tt.add_row(row)
        return tt.draw()   
    def unwrapped(self):
        return self.__repr__(max_width=0)

def run(conn, sql, config):
    if sql.strip():
        statement = sqlalchemy.sql.text(sql)
        return ResultSet(conn.session.execute(statement), sql, 
                         config.get('wrap'), config.get('autolimit'))
    else:
        return 'Connected: %s' % conn.name
     