from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()

version = '0.3.9'

install_requires = [
    'prettytable==0.7.2',
    'ipython==5.2.2',
    'sqlalchemy==1.1.4',
    'sqlparse==0.2.2',
    'six==1.10.0',
    'ipython-genutils==0.1.0',
]

extras_require = dict(
    dev=[
        'nose',
        'pandas',
        'matplotlib',
    ]
)

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
      packages=find_packages('src', exclude=['*test*']),
      package_dir={'': 'src'},
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      extras_require=extras_require
      )
