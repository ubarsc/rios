#!/usr/bin/env python
"""
The setup script for RIOS. Creates the module, installs
the scripts. 
Good idea to use 'install --prefix=/opt/xxxxx' so not installed
with Python.
"""
from distutils.core import setup
import glob, os

os.chdir('rios')
from rios.rios import RIOS_VERSION

setup(name='rios',
      version=RIOS_VERSION,
      description='Raster Input/Output Simplification',
      author='Sam Gillingham',
      author_email='gillingham.sam@gmail.com',
      scripts=glob.glob("bin/*.py"),
      packages=['rios', 'rios/parallel', 'rios/parallel/multiprocessing', 
                        'rios/parallel/mpi', 'rios/riostests'],
      license='LICENSE.txt', 
      url='https://bitbucket.org/chchrsc/rios'
     )
