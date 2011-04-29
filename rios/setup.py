#!/usr/bin/env python
"""
The setup script for PyModeller. Creates the module, installs
the scripts. 
Good idea to use 'install --prefix=/opt/xxxxx' so not installed
with Python.
"""
from distutils.core import setup

setup(name='rios',
      version='1.0',
      description='Raster Input/Output Simplification',
      author='Sam Gillingham',
      author_email='gillingham.sam@gmail.com',
      scripts=['bin/pymdltestreader.py','bin/pymdltestwriter.py',
		'bin/pymdltestreaderdict.py','bin/pymdltestapplier.py',
        'bin/testapplier.py'],
      packages=['rios'],
      license='LICENSE.txt'
     )
