import os
from io import open
import re
import ast

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.md"), encoding="utf-8").read()

_version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('src/sql/__init__.py', 'rb') as f:
    VERSION = str(
        ast.literal_eval(
            _version_re.search(f.read().decode('utf-8')).group(1)))

install_requires = [
    "prettytable<1",
    "ipython>=1.0",
    "sqlalchemy>=0.6.7",
    "sqlparse",
    "six",
    "ipython-genutils>=0.1.0",
    "jinja2",
    "ploomber-core>=0.0.4",
    'importlib-metadata;python_version<"3.8"',
]

DEV = [
    'pytest',
    'pandas',
    'invoke',
    'pkgmt',
    'twine',
    # tests
    'duckdb',
    'duckdb-engine',
    'matplotlib',
]

setup(name="jupysql",
      version=VERSION,
      description="Better SQL in Jupyter",
      long_description=README,
      long_description_content_type="text/markdown",
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Environment :: Console",
          "License :: OSI Approved :: MIT License",
          "Topic :: Database",
          "Topic :: Database :: Front-Ends",
          "Programming Language :: Python :: 3",
      ],
      keywords="database ipython postgresql mysql",
      author="Ploomber",
      author_email="contact@ploomber.io",
      url="https://github.com/ploomber/jupysql",
      project_urls={
          "Source": "https://github.com/ploomber/jupysql",
      },
      license="MIT",
      packages=find_packages("src"),
      package_dir={"": "src"},
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      extras_require={'dev': DEV})
