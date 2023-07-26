import prettytable


class CustomPrettyTable(prettytable.PrettyTable):
    def add_rows(self, data):
        for row in data:
            formatted_row = []
            for cell in row:
                if isinstance(cell, str) and cell.startswith("http"):
                    formatted_row.append("<a href={}>{}</a>".format(cell, cell))
                else:
                    formatted_row.append(cell)
            self.add_row(formatted_row)
