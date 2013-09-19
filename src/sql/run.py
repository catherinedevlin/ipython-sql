import functools
import types
import sqlalchemy
import sqlparse
import prettytable
from .column_guesser import ColumnFinderMixin


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

    
class ResultSet(list, ColumnFinderMixin):
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
    def _find_columns(self, key_word_sep=" ", multi_column_keys=False):
        self._plot_value_label = self.keys[-1]
        self._plot_values = [tuple(r)[-1] for r in self]
        if multi_column_keys: 
            self._plot_keys = [key_word_sep.join(tuple(r)[:-1])
                                                 for r in self]
            self._plot_key_label = key_word_sep.join(self.keys[:-1])
        else:
            if len(self.keys) > 1:
                self._plot_keys = [r[0] for r in self]
                self._plot_key_label = self.keys[0]
            else:
                self._plot_keys = range(len(self))
                self._plot_key_label = 'index'
    def pie(self, key_word_sep=" ", title=None, **kwargs):
        """Generates a pylab pie chart from the result set.
       
        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::
        
            %%matplotlib inline
            
        Values (pie slice sizes) are taken from the 
        rightmost column (numerical values required).
        All other columns are used to label the pie slices.
        
        Parameters
        ----------
        key_word_sep: string used to separate column values
                      from each other in pie labels
        title: Plot title, defaults to Y by X              

        Any additional keyword arguments will be passsed 
        through to ``matplotlib.pylab.pie``.
        """
    def pie(self, key_word_sep=" ", title=None, **kwargs):
        self._find_columns(key_word_sep=key_word_sep, multi_column_keys=True)
        import matplotlib.pylab as plt
        pie = plt.pie(self._plot_values, labels=self._plot_keys, **kwargs)
        plt.title(title or '%s by %s' % (self._plot_value_label, 
                                            self._plot_key_label))
        return pie
    def plot(self, title=None, **kwargs):
        """Generates a pylab plot from the result set.
       
        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::
        
            %%matplotlib inline
           
        The first and last columns are taken as the X and Y
        values.  Any columns between are ignored.
        
        Parameters
        ----------
        title: Plot title, defaults to Y by X              

        Any additional keyword arguments will be passsed 
        through to ``matplotlib.pylab.plot``.
        """
        self._find_columns(multi_column_keys=False)
        import matplotlib.pylab as plt
        plot = plt.plot(self._plot_keys, self._plot_values, **kwargs)
        plt.xlabel(self._plot_key_label)
        plt.ylabel(self._plot_value_label)
        return plot
        
        
        

def run(conn, sql, config, user_namespace):
    if sql.strip():
        for statement in sqlparse.split(sql):
            txt = sqlalchemy.sql.text(statement)
            result = conn.session.execute(txt, user_namespace)
        return ResultSet(result, statement, config)
        #returning only last result, intentionally
    else:
        return 'Connected: %s' % conn.name
     