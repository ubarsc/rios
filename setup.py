#!/usr/bin/env python
"""
The setup script for PyModeller. Creates the module, installs
the scripts. 
Good idea to use 'install --prefix=/opt/xxxxx' so not installed
with Python.
"""
from distutils.core import setup
import glob, os

import rios

os.chdir('rios')

setup(name='rios',
      version=rios.RIOS_VERSION,
      description='Raster Input/Output Simplification',
      author='Sam Gillingham',
      author_email='gillingham.sam@gmail.com',
      scripts=glob.glob("bin/*.py"),
      packages=['rios', 'rios/parallel', 'rios/parallel/multiprocessing', 
                        'rios/parallel/mpi', 'rios/riostests'],
      license='LICENSE.txt', 
      url='https://bitbucket.org/chchrsc/rios'
     )
