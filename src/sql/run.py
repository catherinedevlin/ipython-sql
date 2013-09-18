import functools
import types
import sqlalchemy
import sqlparse
import prettytable


def unduplicate_field_names(field_names):
    """ Append a number to duplicate field names to make them unique. """
    res = []
    for k in field_names:
        if k in res:
            i = 1
            while k + '_' + str(i) in res:
                i += 1
            k += '_' + str(i)
        res.append(k)
    return res


def _plot(self, plot_func):
    return plot_func(self[self.columns[-1]], labels=self[self.columns[0]])
  
try:
    import matplotlib.pylab as plt
    def _pie(self):
        """Display a matplotlib pie chart based on the DataFrame.
            
            Values (pie slice sizes) are taken from the rightmost column;
            labels from the leftmost.
        """
        dtypes = [str, ] * len(self.columns) - 1 + [float, ]
        return plt.pie(self[self.columns[-1]], 
                       labels=self[self.columns[0]],
                       dtype=dtypes)
    
    def _bar(self):
        """Display a matplotlib bar chart based on the DataFrame.
            
            Values (bar heights) are taken from the rightmost column;
            labels from the leftmost.
        """ 
        return plt.bar(self[self.columns[-1]], labels=self[self.columns[0]])
        return _plot(self, plt.bar)
    
    def _scatter(self):
        """Display a matplotlib scatter plot based on the DataFrame.
            
            y values are taken from the rightmost column;
            x values from the leftmost.
        """ 
        return _plot(self, plt.scatter)
    
except ImportError:
    def _pie(self):
        return ImportError("Could not import matplotlib; is it installed?")
    _bar = _pie
    _scatter = _pie
    
class ResultSet(list):
    def __init__(self, sqlaproxy, sql, config):
        self.keys = sqlaproxy.keys()
        self.sql = sql
        self.limit = config.get('limit')
        style_name = config.get('style', 'DEFAULT')
        self.style = prettytable.__dict__[style_name.upper()]
        if sqlaproxy.returns_rows:
            if self.limit:
                list.__init__(self, sqlaproxy.fetchmany(size=self.limit))
            else:
                list.__init__(self, sqlaproxy.fetchall())
            self.pretty = prettytable.PrettyTable(unduplicate_field_names(self.keys))
            for row in self:
                self.pretty.add_row(row)
            self.pretty.set_style(self.style)
        else:
            list.__init__(self, [])
            self.pretty = None
    def _repr_html_(self):
        if self.pretty:
            return self.pretty.get_html_string()
        else:
            return None
    def __str__(self, *arg, **kwarg):
        return str(self.pretty or '')
    def DataFrame(self):
        "Returns a Pandas DataFrame instance built from the result set."
        import pandas as pd
        frame = pd.DataFrame(self, columns=self.keys)
        frame.pie = types.MethodType(_pie, frame)
        frame.bar = types.MethodType(_bar, frame)
        frame.scatter = types.MethodType(_scatter, frame)
        return frame

def run(conn, sql, config, user_namespace):
    if sql.strip():
        for statement in sqlparse.split(sql):
            txt = sqlalchemy.sql.text(statement)
            result = conn.session.execute(txt, user_namespace)
        return ResultSet(result, statement, config)
        #returning only last result, intentionally
    else:
        return 'Connected: %s' % conn.name
     