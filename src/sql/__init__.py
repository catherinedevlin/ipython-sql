from .magic import RenderMagic, SqlMagic, load_ipython_extension
from .error_message import SYNTAX_ERROR
from .connection import PLOOMBER_DOCS_LINK_STR

__version__ = "0.7.6dev"


__all__ = [
    "RenderMagic",
    "SqlMagic",
    "load_ipython_extension",
    "SYNTAX_ERROR",
    "PLOOMBER_DOCS_LINK_STR",
]
