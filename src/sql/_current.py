"""Get/set the current SqlMagic instance."""

__sql_magic = None


def _get_sql_magic():
    """Returns the current SqlMagic instance."""
    if __sql_magic is None:
        raise RuntimeError("%sql has not been loaded yet. Run %load_ext sql")

    return __sql_magic


def _set_sql_magic(sql_magic):
    """Sets the current SqlMagic instance."""
    global __sql_magic
    __sql_magic = sql_magic


def _config_feedback_all():
    """Returns True if the current feedback level is >=2"""
    return _get_sql_magic().feedback >= 2


def _config_feedback_normal_or_more():
    """Returns True if the current feedback level is >=1"""
    return _get_sql_magic().feedback >= 1
