import os
from io import open

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.rst"), encoding="utf-8").read()
NEWS = open(os.path.join(here, "NEWS.rst"), encoding="utf-8").read()


version = "0.5.0"

install_requires = [
    "prettytable",
    "ipython",
    "sqlalchemy>=2.0",
    "sqlparse",
    "six",
    "ipython-genutils",
]


setup(
    name="ipython-sql",
    version=version,
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
)
