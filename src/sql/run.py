import sqlalchemy
import connection
import types
import jinja2
import texttable
import sys
from IPython.zmq import displayhook
   
class PrettyProxy(sqlalchemy.engine.result.ResultProxy):
    def _repr_html_(self):
        return html_table_template.render(headers=self.keys(), rows=self)    

def _tabular_str_(self):
    tt = texttable.Texttable()
    tt.set_deco(texttable.Texttable.HEADER)
    tt.header(self.keys())
    for row in self:
        tt.add_row(row)
    return tt.draw()    

html_table_template = jinja2.Template(r"""
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

def printable(resultProxy):
    resultProxy.__class__ = PrettyProxy
    if not isinstance(sys.displayhook, displayhook.ZMQShellDisplayHook):
        print(_tabular_str_(resultProxy))  # attempts to set PrettyProxy.__str__ not working
    return resultProxy
    
def run(conn, sql):
    if sql.strip():
        statement = sqlalchemy.sql.text(sql)
        result = conn.session.execute(statement)
        return printable(result)
    else:
        return 'Connected to %s' % conn.name
     