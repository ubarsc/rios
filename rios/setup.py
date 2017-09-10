#!/usr/bin/env python
"""
The setup script for RIOS. Creates the module, installs
the scripts. 
Good idea to use 'install --prefix=/opt/xxxxx' so not installed
with Python.
"""
# This file is part of RIOS - Raster I/O Simplification
# Copyright (C) 2012  Sam Gillingham, Neil Flood
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
from distutils.core import setup
import glob

import rios

# Are we installing the command line scripts?
# this is an experimental option for users who are
# using the Python entry point feature of setuptools and Conda instead
NO_INSTALL_CMDLINE = int(os.getenv('RIOS_NOCMDLINE', '0')) > 0

if NO_INSTALL_CMDLINE:
    # still install the scripts for the parallel processing to work properly
    scripts_list = ['bin/rios_subproc.py', 'bin/rios_subproc_mpi.py']
else:
    scripts_list = glob.glob("bin/*.py")

setup(name='rios',
      version=rios.RIOS_VERSION,
      description='Raster Input/Output Simplification',
      author='Sam Gillingham',
      author_email='gillingham.sam@gmail.com',
      scripts=scripts_list,
      packages=['rios', 'rios/parallel', 
                        'rios/riostests', 'rios/cmdline'],
      license='LICENSE.txt', 
      url='https://bitbucket.org/chchrsc/rios'
     )
