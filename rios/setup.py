#!/usr/bin/env python
"""
The setup script for PyModeller. Creates the module, installs
the scripts. 
Good idea to use 'install --prefix=/opt/xxxxx' so not installed
with Python.
"""
import glob
from distutils.core import setup

setup(name='rios',
      version='1.1',
      description='Raster Input/Output Simplification',
      author='Sam Gillingham',
      author_email='gillingham.sam@gmail.com',
      scripts=glob.glob('bin/*.py'),
      packages=['rios'],
      license='LICENSE.txt', 
      url='https://bitbucket.org/chchrsc/rios'
     )
