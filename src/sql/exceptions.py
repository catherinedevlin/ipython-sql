from IPython.core import error


def ArgumentError(message):
    exc = error.UsageError(message)
    exc.error_type = "ArgumentError"
    return exc


def TableNotFoundError(message):
    exc = error.UsageError(message)
    exc.error_type = "TableNotFoundError"
    return exc
