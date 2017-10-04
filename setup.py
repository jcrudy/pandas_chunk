from setuptools import setup, find_packages
import versioneer
import os

setup(name='pandas_feather_chunk',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      author='Jason Rudy',
      author_email='jcrudy@gmail.com',
      url='https://github.com/jcrudy/pandas_feather_chunk',
      packages=find_packages(),
      requires=[],
      install_requires=['pandas', 'feather-format'],
     )