import re

def parse(cell):
    parts = cell.split(None, 1)
    if not parts:
        return {'connection': '', 'sql': ''}
    if '@' in parts[0] or '://' in parts[0]:
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
   