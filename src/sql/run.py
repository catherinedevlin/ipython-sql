import functools
import operator
import types
import sqlalchemy
import sqlparse
import prettytable
from .column_guesser import ColumnGuesserMixin


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

    
class ResultSet(list, ColumnGuesserMixin):
    def __init__(self, sqlaproxy, sql, config):
        self.keys = sqlaproxy.keys()
        self.sql = sql
        self.limit = config.autolimit
        style_name = config.style
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
        return frame
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
        title: Plot title, defaults to name of value column

        Any additional keyword arguments will be passsed 
        through to ``matplotlib.pylab.pie``.
        """
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        import matplotlib.pylab as plt
        pie = plt.pie(self.ys[0], labels=self.xlabels, **kwargs)
        plt.title(title or self.ys[0].name)
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
        title: Plot title, defaults to names of Y value columns

        Any additional keyword arguments will be passsed 
        through to ``matplotlib.pylab.plot``.
        """
        import matplotlib.pylab as plt
        self.guess_plot_columns()
        self.x = self.x or range(len(self.ys[0]))
        coords = reduce(operator.add, [(self.x, y) for y in self.ys])
        plot = plt.plot(*coords, **kwargs)
        if hasattr(self.x, 'name'):
            plt.xlabel(self.x.name)
        ylabel = ", ".join(y.name for y in self.ys)
        plt.title(title or ylabel)
        plt.ylabel(ylabel)
        return plot
    
    def bar(self, key_word_sep = " ", title=None, **kwargs):
        """Generates a pylab bar plot from the result set.
       
        ``matplotlib`` must be installed, and in an
        IPython Notebook, inlining must be on::
        
            %%matplotlib inline
           
        The last quantitative column is taken as the Y values;
        all other columns are combined to label the X axis. 
        
        Parameters
        ----------
        title: Plot title, defaults to names of Y value columns
        key_word_sep: string used to separate column values
                      from each other in labels
                      
        Any additional keyword arguments will be passsed 
        through to ``matplotlib.pylab.bar``.
        """
        import matplotlib.pylab as plt
        self.guess_pie_columns(xlabel_sep=key_word_sep)
        plot = plt.bar(range(len(self.ys[0])), self.ys[0], **kwargs)
        if self.xlabels:
            plt.xticks(range(len(self.xlabels)), self.xlabels,
                       rotation=45)
        plt.xlabel(self.xlabel)
        plt.ylabel(self.ys[0].name)
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
     