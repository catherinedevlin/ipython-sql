import re
import warnings
from IPython.core.magic_arguments import parse_argstring
from jinja2 import Template

from sqlalchemy.engine import Engine

from sql import parse
from sql.store import store
from sql.connection import Connection


class SQLPlotCommand:
    def __init__(self, magic, line) -> None:
        self.args = parse_argstring(magic.execute, line)


class SQLCommand:
    """
    Encapsulates the parsing logic (arguments, SQL code, connection string, etc.)

    """

    def __init__(self, magic, user_ns, line, cell) -> None:
        # Support for the variable substition in the SQL clause
        line, cell = self._var_expand(magic, user_ns, line, cell)
        self.args = parse.magic_args(magic.execute, line)
        # self.args.line (everything that appears after %sql/%%sql in the first line)
        # is splited in tokens (delimited by spaces), this checks if we have one arg
        one_arg = len(self.args.line) == 1

        if (
            one_arg
            and self.args.line[0] in user_ns
            and isinstance(user_ns[self.args.line[0]], Engine)
        ):
            line_for_command = []
            add_conn = True
        else:
            line_for_command = self.args.line
            add_conn = False

        if one_arg and self.args.line[0] in Connection.connections:
            line_for_command = []
            add_alias = True
        else:
            add_alias = False
        self.command_text = " ".join(line_for_command) + "\n" + cell

        if self.args.file:
            with open(self.args.file, "r") as infile:
                self.command_text = infile.read() + "\n" + self.command_text

        self.parsed = parse.parse(self.command_text, magic)

        self.parsed["sql_original"] = self.parsed["sql"]

        if add_conn:
            self.parsed["connection"] = user_ns[self.args.line[0]]

        if add_alias:
            self.parsed["connection"] = self.args.line[0]

        if self.args.with_:
            final = store.render(self.parsed["sql"], with_=self.args.with_)
            self.parsed["sql"] = str(final)

    @property
    def sql(self):
        """
        Returns the SQL query to execute, without any other options or arguments
        """
        return self.parsed["sql"]

    @property
    def sql_original(self):
        """
        Returns the raw SQL query. Might be different from `sql` if using --with
        """
        return self.parsed["sql_original"]

    @property
    def connection(self):
        """Returns the connection string"""
        return self.parsed["connection"]

    @property
    def result_var(self):
        """Returns the result_var"""
        return self.parsed["result_var"]

    def _var_expand(self, magic, user_ns, line, cell):
        """
        Support for the variable substition in the SQL clause
        For now, we have enabled two ways:
        1. Latest format, {{a}}, we use jinja2 to parse the string with {{a}} format
        2. Legacy format, {a}, $a, and :a format.

        We will deprecate the legacy format feature in next major version
        """
        self.is_legacy_var_expand_parsed = False
        # Latest format parsing
        # TODO: support --param and --use-global logic here
        # Ref: https://github.com/ploomber/jupysql/issues/93
        line = Template(line).render(user_ns)
        cell = Template(cell).render(user_ns)
        # Legacy format parsing
        parsed_cell = magic.shell.var_expand(cell, depth=2)
        parsed_line = magic.shell.var_expand(line, depth=2)
        # Exclusive the string with "://", but has :variable
        has_SQLAlchemy_var_expand = re.search("(?<!://):[^/]+", line) or ":" in cell
        if parsed_line != line or parsed_cell != cell or has_SQLAlchemy_var_expand:
            self.is_legacy_var_expand_parsed = True
            warnings.warn(
                "Variable substitution with $var and {var} has been "
                "deprecated and will be removed in a future version. "
                "Use {{var}} instead.",
                FutureWarning,
            )
        return parsed_line, parsed_cell
