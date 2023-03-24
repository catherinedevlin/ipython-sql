import os
from io import open
import re
import ast

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.md"), encoding="utf-8").read()

_version_re = re.compile(r"__version__\s+=\s+(.*)")

with open("src/sql/__init__.py", "rb") as f:
    VERSION = str(
        ast.literal_eval(_version_re.search(f.read().decode("utf-8")).group(1))
    )

install_requires = [
    "prettytable",
    "ipython>=1.0",
    "sqlalchemy<2",
    "sqlparse",
    "ipython-genutils>=0.1.0",
    "sqlglot",
    "jinja2",
    "sqlglot>=11.3.7",
    "ploomber-core>=0.2.7",
    'importlib-metadata;python_version<"3.8"',
]

DEV = [
    "flake8",
    "pytest",
    "pandas",
    "polars==0.16.14",  # 03/24/23 this breaks our CI
    "invoke",
    "pkgmt",
    "twine",
    # tests
    "duckdb",
    "duckdb-engine",
    # sql.plot module tests
    "matplotlib",
    "black",
    "dockerctx",
    "docker",
    # for %%sql --interact
    "ipywidgets",
]

setup(
    name="jupysql",
    version=VERSION,
    description="Better SQL in Jupyter",
    long_description=README,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Topic :: Database",
        "Topic :: Database :: Front-Ends",
        "Programming Language :: Python :: 3",
    ],
    keywords="database ipython postgresql mysql duckdb",
    author="Ploomber",
    author_email="contact@ploomber.io",
    url="https://github.com/ploomber/jupysql",
    project_urls={
        "Source": "https://github.com/ploomber/jupysql",
    },
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    extras_require={"dev": DEV},
)
