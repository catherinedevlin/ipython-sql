"""
In most scenarios, users don't care about the full Python traceback because it's
irrelevant to them (they run SQL, not Python code). Hence, when raising errors,
we only display the error message. This is possible via IPython.core.error.UsageError:
IPython/Jupyter automatically detect this error and hide the traceback.
Unfortunately, IPython.core.error.UsageError isn't the most appropriate error type for
all scenarios, so we define our own error types here. The main caveat is that due to a
bug in IPython (https://github.com/ipython/ipython/issues/14024), subclassing
IPython.core.error.UsageError doesn't work, so `exception_factory` is a workaround
to create new errors that are IPython.core.error.UsageError but with a different name.

"""
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
KeyError = exception_factory("KeyError")
FileNotFoundError = exception_factory("FileNotFoundError")
NotImplementedError = exception_factory("NotImplementedError")

# The following are internal exceptions that should not be raised directly

# raised internally when the user chooses a table that doesn't exist
TableNotFoundError = exception_factory("TableNotFoundError")

# raise it when there is an error in parsing the configuration file
ConfigurationError = exception_factory("ConfigurationError")


InvalidQueryParameters = exception_factory("InvalidQueryParameters")
