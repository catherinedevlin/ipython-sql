from .magic import RenderMagic, SqlMagic, load_ipython_extension
from .connection import PLOOMBER_DOCS_LINK_STR

__version__ = "0.8.1dev"


__all__ = [
    "RenderMagic",
    "SqlMagic",
    "load_ipython_extension",
    "PLOOMBER_DOCS_LINK_STR",
]
