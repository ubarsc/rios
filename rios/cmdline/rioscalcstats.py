
"""
Simple function for the rioscalcstats.py command line
program that can be turned into an entry point.
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

import sys
import argparse

from osgeo import gdal
from rios import calcstats


gdal.UseExceptions()


def getCmdargs():
    """
    Get commandline arguments
    """
    p = argparse.ArgumentParser()
    p.add_argument("imgfile", nargs='*', help="Name of input image file")
    p.add_argument("--ignore", "-i", type=float,
        help=("Ignore given value when calculating statistics. "+
            "Default is whatever is already defined in image file"))
    p.add_argument("--approx", action="store_true", default=False,
        help=("Do approximate stats calculation (much faster)"))

    cmdargs = p.parse_args()

    if cmdargs.imgfile is None or len(cmdargs.imgfile) == 0:
        p.print_help()
        sys.exit(1)

    return cmdargs


def main():
    """
    Main routine for calling from command line.
    """
    cmdargs = getCmdargs()
    
    for filename in cmdargs.imgfile:
        ds = gdal.Open(filename, gdal.GA_Update)
        
        # If no ignore value given, check if one is already defined in the file
        if cmdargs.ignore is not None:
            ignore = cmdargs.ignore
        else:
            b1 = ds.GetRasterBand(1)
            ignore = b1.GetNoDataValue()
        
        calcstats.calcStats(ds, ignore=ignore, approx_ok=cmdargs.approx)
        ds.FlushCache()

    # so entry points return success at command line
    return 0
