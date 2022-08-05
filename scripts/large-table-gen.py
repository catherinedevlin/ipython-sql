"""Renter large-table-template.sql
"""
from pathlib import Path
from jinja2 import Template

t = Template(Path('large-table-template.sql').read_text())
Path('large-table.sql').write_text(t.render())