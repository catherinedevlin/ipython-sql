from pathlib import Path
from jinja2 import Template

from sqlalchemy.engine import Engine

from sql import parse, exceptions
from sql.store import store
from sql.connection import ConnectionManager, is_pep249_compliant
from sql.util import validate_nonidentifier_connection


class SQLPlotCommand:
    def __init__(self, magic, line) -> None:
        self.args = parse.magic_args(
            magic.execute, line, "sqlplot", allowed_duplicates=["-w", "--with"]
        )


class SQLCommand:
    """
    Encapsulates the parsing logic (arguments, SQL code, connection string, etc.)

    """

    def __init__(self, magic, user_ns, line, cell) -> None:
        self._line = line
        self._cell = cell

        self.args = parse.magic_args(
            magic.execute,
            line,
            "sql",
            allowed_duplicates=["-w", "--with", "--append", "--interact"],
        )

        # self.args.line (everything that appears after %sql/%%sql in the first line)
        # is split in tokens (delimited by spaces), this checks if we have one arg
        one_arg = len(self.args.line) == 1

        # NOTE: this is only used to determine if what the user passed looks like a
        # connection, we can simplify it
        if len(self.args.line) > 0 and self.args.line[0] in user_ns:
            conn = user_ns[self.args.line[0]]

            is_dbapi_connection_ = is_pep249_compliant(conn)
        else:
            is_dbapi_connection_ = False

        if (
            one_arg
            and self.args.line[0] in user_ns
            and (isinstance(user_ns[self.args.line[0]], Engine) or is_dbapi_connection_)
        ):
            line_for_command = []
            add_conn = True
        else:
            line_for_command = self.args.line
            add_conn = False

        if one_arg and self.args.line[0] in ConnectionManager.connections:
            line_for_command = []
            add_alias = True
        else:
            add_alias = False

        self.command_text = " ".join(line_for_command) + "\n" + cell

        if self.args.file:
            try:
                file_contents = Path(self.args.file).read_text()
            except FileNotFoundError as e:
                raise exceptions.FileNotFoundError(str(e)) from e

            self.command_text = file_contents + "\n" + self.command_text

        self.parsed = parse.parse(self.command_text, magic.dsn_filename)

        self.parsed["sql_original"] = self.parsed["sql"] = self._var_expand(
            self.parsed["sql"], user_ns
        )

        if add_conn:
            self.parsed["connection"] = user_ns[self.args.line[0]]

        if add_alias:
            self.parsed["connection"] = self.args.line[0]

        if self.args.with_:
            final = store.render(self.parsed["sql"], with_=self.args.with_)
            self.parsed["sql"] = str(final)

        if (
            one_arg
            and self.sql
            and not (add_conn or add_alias)
            and not (self.args.persist_replace or self.args.persist or self.args.append)
        ):
            # Apply strip to ensure whitespaces/linebreaks aren't passed
            validate_nonidentifier_connection(self.sql.strip().split(" ")[0].strip())

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

    @property
    def return_result_var(self):
        """Returns the return_result_var"""
        return self.parsed["return_result_var"]

    def _var_expand(self, sql, user_ns):
        return Template(sql).render(user_ns)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(line={self._line!r}, cell={self._cell!r}) -> "
            f"({self.sql!r}, {self.sql_original!r})"
        )

    def set_sql_with(self, with_):
        """
        Sets the final rendered SQL query using the WITH clause

        Parameters
        ----------
        with_ : list
        list of all subqueries needed to render the query
        """
        final = store.render(self.parsed["sql"], with_)
        self.parsed["sql"] = str(final)
