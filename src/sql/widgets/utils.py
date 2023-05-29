import re
from jinja2 import Template
import psutil


def load_file(file_path) -> str:
    """
    Returns the content of a file
    """
    with open(file_path, mode="r") as file:
        return file.read()


def load_js(*files) -> str:
    """
    Loads js files into HTML <script>

    Parameters
    ----------
    *files : str or a sequence of (file_path : str, parameters : dict)
        The JS files to load.
    """

    js = ""

    for file in files:
        if isinstance(file, str):
            js += load_file(file)
        else:
            path = file[0]
            template_params = file[1]
            js_template = load_file(path)
            js_template = Template(js_template)
            js += js_template.render(template_params)

    return f"""
    <script>{js}</script>
    """


def load_css(*files) -> str:
    """
    Loads css files into HTML <style>
    """
    css = ""

    for file in files:
        css += load_file(file)

    return f"""
    <style>{css}</style>
    """


def set_template_params(**kwargs):
    """
    Returns parameters in a dict format for Jinja2 template.

    We can use it when loading JS files with custom parameters.

    e.g.
    html_scripts = utils.load_js([path_to_file,
                                    set_template_params(
                                        param_one = 1,
                                        param_one = 2)
                                    ]
                                    )
    """
    return kwargs


def extract_function_by_name(source, function_name) -> str:
    """
    Return function str by name from string

    Parameters
    ----------
    source : str
        Text to extract JS function from

    function_name : str
        The name of the function to extract
    """
    pattern = (
        r"function\s+"
        + function_name
        + r"\s*\([^)]*\)\s*\{((?:[^{}]+|\{(?:[^{}]+|\{[^{}]*\})*\})*)\}"
    )
    match = re.search(pattern, source)
    if match:
        return match.group(0)
    else:
        return None


def is_jupyterlab_session() -> bool:
    """Check whether we are in a Jupyter-Lab session.
    Notes
    -----
    This is a heuristic based process inspection based on the current Jupyter lab
    (major 3) version. So it could fail in the future.
    It will also report false positive in case a classic notebook frontend is started
    via Jupyter lab.

    reference:
    https://discourse.jupyter.org/t/find-out-if-my-code-runs-inside-a-notebook-or-jupyter-lab/6935
    """

    # inspect parent process for any signs of being a jupyter lab server

    parent = psutil.Process().parent()
    if parent.name() == "jupyter-lab":
        return True
    keys = (
        "JUPYTERHUB_API_KEY",
        "JPY_API_TOKEN",
        "JUPYTERHUB_API_TOKEN",
    )
    env = parent.environ()
    if any(k in env for k in keys):
        return True

    return False
