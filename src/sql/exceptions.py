from IPython.core import error


def exception_factory(name):
    def _error(message):
        exc = error.UsageError(message)
        exc.error_type = name
        # this attribute will allow the @modify_exceptions decorator to add the
        # community link
        exc.modify_exception = True
        return exc

    return _error


# raise it when there's an issue with the user's input in a magic. e.g., missing an
# argument
UsageError = exception_factory("UsageError")

# raise it when a user wants to use a feature that requires an optional dependency
MissingPackageError = exception_factory("MissingPackageError")

# the following exceptions should be called instead of the Python built-in ones,
# for guidelines on when to use them:
# https://docs.python.org/3/library/exceptions.html#bltin-exceptions
TypeError = exception_factory("TypeError")
RuntimeError = exception_factory("RuntimeError")
ValueError = exception_factory("ValueError")


# The following are internal exceptions that should not be raised directly

# raised internally when the user chooses a table that doesn't exist
TableNotFoundError = exception_factory("TableNotFoundError")
