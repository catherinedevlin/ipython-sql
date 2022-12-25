from sql import parse
from sql.store import store


# NOTE: this will encapsulate the logic in magic.py
class SQLCommand:
    """
    Encapsulates the parsing logic (arguments, SQL code, connection string, etc.)

    """

    def __init__(self, magic, user_ns, line, cell) -> None:
        self.args = parse.magic_args(magic.execute, line)

        # if line == "my_engine":
        #     from IPython import embed

        #     embed()

        if len(self.args.line) == 1 and self.args.line[0] in user_ns:
            line_for_command = []
            add_conn = True
        else:
            line_for_command = self.args.line
            add_conn = False

        self.command_text = " ".join(line_for_command) + "\n" + cell

        if self.args.file:
            with open(self.args.file, "r") as infile:
                self.command_text = infile.read() + "\n" + self.command_text

        # TODO: test with something that requires the dsn_filename attribute
        self.parsed = parse.parse(self.command_text, magic)

        self.parsed["sql_original"] = self.parsed["sql"]

        if add_conn:
            self.parsed["connection"] = self.args.line[0]

        if self.args.with_:
            final = store.render(self.parsed["sql"], with_=self.args.with_)
            self.parsed["sql"] = str(final)
