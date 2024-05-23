"""
Test the controls.setInputNoDataValue() function.

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
import numpy
from osgeo import osr
from rios import applier
from rios import pixelgrid

from . import riostestutils

TESTNAME = "TESTSETINPUTNULL"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    
    nRows = riostestutils.DEFAULT_ROWS
    nCols = riostestutils.DEFAULT_COLS
    arr = numpy.zeros((nRows, nCols), dtype=numpy.uint8)
    # Set the lower half to 100
    i = riostestutils.DEFAULT_ROWS // 2
    dataval = 100
    arr[i:] = dataval

    img1 = 'image1.img'
    img2 = 'image2.img'
    ds1 = riostestutils.createTestFile(img1, numRows=nRows, numCols=nCols)
    ds2 = riostestutils.createTestFile(img2, numRows=nRows, numCols=nCols)
    ds1.GetRasterBand(1).WriteArray(arr)
    ds2.GetRasterBand(1).WriteArray(arr)
    del ds1
    del ds2

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    controls = applier.ApplierControls()

    # Make a reference pixgrid in a different projection.
    dstSrs = osr.SpatialReference()
    dstSrs.ImportFromEPSG(3577)
    dstProj = dstSrs.ExportToWkt()
    refPixgrid = pixelgrid.pixelGridFromFile(img1)
    refPixgrid.projection = dstProj

    infiles.img1 = img1
    infiles.img2 = img2
    otherargs.nullval = 0
    otherargs.dataval = dataval
    otherargs.countSmeared1 = 0
    otherargs.countSmeared2 = 0
    controls.setReferencePixgrid(refPixgrid)
    controls.setResampleMethod('cubic')
    # We over-ride the missing null, but only on img2
    controls.setInputNoDataValue(0, 'img2')

    applier.apply(checkNulls, infiles, outfiles, otherargs, controls=controls)

    smearOK = ((otherargs.countSmeared1 > 0) and (otherargs.countSmeared2 == 0))
    if not smearOK:
        msg = "Smeared pixel counts should be (>0, ==0), actually ({}, {})"
        msg = msg.format(otherargs.countSmeared1, otherargs.countSmeared2)
        riostestutils.report(TESTNAME, msg)
    else:
        riostestutils.report(TESTNAME, "Passed")

    nullForOK = ((otherargs.nullvalFor1 is None) and
                 (otherargs.nullvalFor2 == otherargs.nullval))
    if not nullForOK:
        allOK = False
        msg = "info.getNoDataValueFor() reports ({}, {}), should be ({}, {})"
        msg = msg.format(otherargs.nullvalFor1, otherargs.nullvalFor2, None, 
            otherargs.nullval)
        riostestutils.report(TESTNAME, msg)

    allOK = (smearOK and nullForOK)
    
    # Clean up
    for filename in [img1, img2]:
        riostestutils.removeRasterFile(filename)

    return allOK


def checkNulls(info, inputs, outputs, otherargs):
    """
    Check the behaviour of nulls on each of the two input files.

    The working grid is in a different projecrtion to the input files,
    so the resampling of the inputs will have resampled across the boundary
    between null and non-null areas of the images. The input files do not
    have a null value set, but the img2 file has had an over-ride null
    set via controls.setInputNoDataValue. So, the behaviour should be
    different. The img1 file should have some smeared values present, while
    img2 should not.

    We also test that this is honoured by info.getNoDataValueFor

    """
    img1 = inputs.img1
    img2 = inputs.img2
    nullval = otherargs.nullval
    dataval = otherargs.dataval
    countSmeared1 = numpy.count_nonzero((img1 != nullval) & (img1 != dataval))
    countSmeared2 = numpy.count_nonzero((img2 != nullval) & (img2 != dataval))

    otherargs.countSmeared1 += countSmeared1
    otherargs.countSmeared2 += countSmeared2

    otherargs.nullvalFor1 = info.getNoDataValueFor(inputs.img1)
    otherargs.nullvalFor2 = info.getNoDataValueFor(inputs.img2)
