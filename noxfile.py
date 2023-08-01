from pathlib import Path
from os import environ

import nox


# list non-setup sessions here
nox.options.sessions = ["test_postgres"]

# GitHub actions does not have conda installed
VENV_BACKEND = "conda" if "CI" not in environ else None
DEV_ENV_NAME = "jupysql-env"


if VENV_BACKEND == "conda":
    CONDA_PREFIX = environ.get("CONDA_PREFIX")

    if CONDA_PREFIX:
        nox.options.envdir = str(Path(CONDA_PREFIX).parent)
    else:
        print("CONDA_PREFIX not found, creating envs in default location...")


INTEGRATION_CONDA_DEPENDENCIES = [
    "pyarrow",
    "psycopg2",
    "pymysql",
    "oracledb",
    "pip",
]

INTEGRATION_PIP_DEPENDENCIES = [
    "dockerctx",
    "pgspecial==2.0.1",
    "pyodbc==4.0.34",
    "sqlalchemy-pytds",
    "python-tds",
]


def _install(session, integration):
    session.install("--editable", ".[dev]")

    if integration:
        session.install(*INTEGRATION_PIP_DEPENDENCIES)
        session.install(*INTEGRATION_CONDA_DEPENDENCIES)


def _check_sqlalchemy(session, version):
    session.run(
        "python",
        "-c",
        (
            "import sqlalchemy; "
            f"assert int(sqlalchemy.__version__.split('.')[0]) == {version}"
        ),
    )


def _run_unit(session, skip_image_tests):
    args = [
        "pytest",
        "src/tests/",
        "--ignore",
        "src/tests/integration",
    ]

    if skip_image_tests:
        args.extend(
            [
                "--ignore",
                "src/tests/test_ggplot.py",
                "--ignore",
                "src/tests/test_magic_plot.py",
            ]
        )

    session.run(*args)


@nox.session(
    venv_backend=VENV_BACKEND,
    name=DEV_ENV_NAME,
    python=environ.get("PYTHON_VERSION", "3.11"),
)
def setup(session):
    print("Installing requirements...")
    _install(session, integration=False)


@nox.session(
    venv_backend=VENV_BACKEND,
    python=environ.get("PYTHON_VERSION", "3.11"),
)
def test_unit(session):
    """Run unit tests (SQLAlchemy 2.x)"""
    SKIP_IMAGE_TEST = "--skip-image-tests" in session.posargs

    _install(session, integration=False)
    session.install("sqlalchemy>=2")
    _check_sqlalchemy(session, version=2)
    _run_unit(session, skip_image_tests=SKIP_IMAGE_TEST)


@nox.session(
    venv_backend=VENV_BACKEND,
    python=environ.get("PYTHON_VERSION", "3.11"),
)
def test_unit_sqlalchemy_one(session):
    """Run unit tests (SQLAlchemy 1.x)"""
    SKIP_IMAGE_TEST = "--skip-image-tests" in session.posargs

    _install(session, integration=False)
    session.install("sqlalchemy<2")
    _check_sqlalchemy(session, version=1)
    _run_unit(session, skip_image_tests=SKIP_IMAGE_TEST)


@nox.session(
    venv_backend=VENV_BACKEND,
    python=environ.get("PYTHON_VERSION", "3.11"),
)
def test_integration_snowflake(session):
    """
    Run snowflake tests (NOTE: the sqlalchemy-snowflake driver only works with
    SQLAlchemy 1.x)
    """

    # TODO: do not require integrationt test dependencies if only running snowflake
    # tests
    _install(session, integration=True)
    session.install("snowflake-sqlalchemy")
    session.run("pytest", "src/tests/integration", "-k", "snowflake", "-v")


@nox.session(
    venv_backend=VENV_BACKEND,
    python=environ.get("PYTHON_VERSION", "3.11"),
)
def test_integration(session):
    """Run integration tests (to check compatibility with databases)"""
    _install(session, integration=True)
    session.run(
        "pytest",
        "src/tests/integration",
        "-k",
        "not snowflake",
        "-v",
    )
