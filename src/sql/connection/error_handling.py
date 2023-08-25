import shutil


_CONDA_INSTALLED = shutil.which("conda") is not None
_PREFER_CONDA = {"psycopg2"}


def install_command(package):
    # special case for psycopg2
    if package == "psycopg2" and not _CONDA_INSTALLED:
        package = "psycopg2-binary"

    if _CONDA_INSTALLED and package in _PREFER_CONDA:
        template = "%conda install {package} -c conda-forge --yes --quiet"
    else:
        template = "%pip install {package} --quiet"

    return template.format(package=package)
