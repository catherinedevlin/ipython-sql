import os
from io import open
import re
import ast

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.rst"), encoding="utf-8").read()
NEWS = open(os.path.join(here, "NEWS.rst"), encoding="utf-8").read()

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
]

setup(name="ipython-sql",
      version=VERSION,
      description="RDBMS access via IPython",
      long_description=README + "\n\n" + NEWS,
      long_description_content_type="text/x-rst",
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Environment :: Console",
          "License :: OSI Approved :: MIT License",
          "Topic :: Database",
          "Topic :: Database :: Front-Ends",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 2",
      ],
      keywords="database ipython postgresql mysql",
      author="Catherine Devlin",
      author_email="catherine.devlin@gmail.com",
      url="https://github.com/catherinedevlin/ipython-sql",
      project_urls={
          "Source": "https://github.com/catherinedevlin/ipython-sql",
      },
      license="MIT",
      packages=find_packages("src"),
      package_dir={"": "src"},
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      extras_require={'dev': [
          'pytest',
          'pandas',
      ]})
