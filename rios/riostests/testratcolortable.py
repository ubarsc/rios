#!/usr/bin/env python
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

"""
Test the rios.colortable functionality
"""
import os
import numpy
from osgeo import gdal

from rios import colortable
from rios.riostests import riostestutils

TESTNAME = 'TESTRATCOLORTABLE'

NUMENTRIES = 1000
RAMPNAME = 'inferno'


def run():
    """
    Run tests of the rios.colortable functions
    """
    riostestutils.reportStart(TESTNAME)
    allOK = True

    imgfile = 'test.img'
    ds = makeTestFile(imgfile)

    # test generating color tables
    tableNames = colortable.getRampNames()
    allOK = RAMPNAME in tableNames
    if not allOK:
        riostestutils.report(TESTNAME, 
                "Cannot find {} color ramp".format(RAMPNAME))
    
    if allOK:
    
        # write a table out
        table = colortable.genTable(NUMENTRIES, RAMPNAME, 0)
        
        colortable.setTable(ds, table)
        
        # now read it back
        table_fromfile = colortable.getTable(ds)
        if not (table == table_fromfile).all():
            riostestutils.report(TESTNAME, "Value mis-match for color table")
            allOK = False
        
    if os.path.exists(imgfile):
        riostestutils.removeRasterFile(imgfile)

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def makeTestFile(imgfile):
    # Make a test image with a ramp of values
    nRows = 100
    nCols = 100
    
    imgArray = numpy.linspace(0, NUMENTRIES, nRows * nCols)
    imgArray = numpy.reshape(imgArray, (nCols, nRows))
    
    ds = riostestutils.createTestFile(imgfile, numRows=nRows, numCols=nCols,
                    dtype=gdal.GDT_UInt16)
    band = ds.GetRasterBand(1)
    band.WriteArray(imgArray)

    band.SetMetadataItem('LAYER_TYPE', 'thematic')

    return ds

    
if __name__ == "__main__":
    run()
