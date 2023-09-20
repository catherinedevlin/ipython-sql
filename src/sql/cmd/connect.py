try:
    from jupysql_plugin.widgets import ConnectorWidget
except ModuleNotFoundError:
    ConnectorWidget = None

from ploomber_core.dependencies import requires


@requires(["jupysql-plugin", "ipywidgets"])
def connect(others):
    """
    Implementation of `%sqlcmd connect`
    """

    connectorwidget = ConnectorWidget()
    return connectorwidget
