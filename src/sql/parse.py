import re

sql_keywords = """insert update delete select create drop alter""".split()

parser_spec = r"""\s*(?P<connection>[^@ \t]+@\w+)?\s+(?P<sql>(%s).*)""" % "|".join(sql_keywords)
parser = re.compile(parser_spec, re.IGNORECASE | re.DOTALL)

def parse(cell):
    parts = cell.split(None, 1)
    if not parts:
        return {'connection': '', 'sql': ''}
    if '@' in parts[0] or 'sqlite://' in parts[0]:
        connection = parts[0]
        if len(parts) > 1:
            sql = parts[1]
        else:
            sql = ''
    else:
        connection = ''
        sql = cell
    return {'connection': connection.strip(),
            'sql': sql.strip()
            }
   