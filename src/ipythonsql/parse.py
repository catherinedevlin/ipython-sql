import re

sql_keywords = """insert update delete select create drop alter""".split()

parser_spec = r"""(?P<connection>.*?)(?P<sql>(%s).*)""" % "|".join(sql_keywords)
parser = re.compile(parser_spec, re.IGNORECASE | re.DOTALL)

def parse(cell):
    result = parser.search(cell).groupdict()
    return {'connection': result['connection'].strip(),
            'sql': result['sql'].strip()
            }
    