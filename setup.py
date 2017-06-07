from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.3.9'

install_requires = [
    'prettytable',
    'ipython>=1.0',
    'sqlalchemy>=0.6.7',
    'sqlparse',
    'six',
    'pgspecial',
    'ipython-genutils>=0.1.0',
]


setup(name='ipython-sql',
    version=version,
    description="RDBMS access via IPython",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Topic :: Database',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 2',
    ],
    keywords='database ipython postgresql mysql',
    author='Catherine Devlin',
    author_email='catherine.devlin@gmail.com',
    url='https://pypi.python.org/pypi/ipython-sql',
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
