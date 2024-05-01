"""
Full test of the overlap mechanism.

Apply a focal maximum filter to a ramp input, and check that RIOS's
output gives the same as applying it to the array in memory. If no
overlap is set, then it will find a few hundred incorrect pixles,
but with the right overlap, there should be none.

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
try:
    from scipy.ndimage import maximum_filter
except ImportError:
    maximum_filter = None
from osgeo import gdal

from rios import applier

from . import riostestutils

TESTNAME = "TESTOVERLAP"


def run():
    """
    Run the test
    """
    allOK = True
    
    riostestutils.reportStart(TESTNAME)
    if maximum_filter is None:
        riostestutils.report(TESTNAME, "Skipped, as scipy is unavailable")
        return allOK

    img = 'img.img'
    outimg = 'outimg.img'
    riostestutils.genRampImageFile(img)

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    controls = applier.ApplierControls()

    infiles.img = img
    outfiles.out = outimg
    otherargs.size = 5
    overlap = otherargs.size // 2
    controls.setOverlap(overlap)

    applier.apply(doFilter, infiles, outfiles, otherargs, controls=controls)

    riosFiltered = readArr(outimg)
    inArr = readArr(img)
    directFiltered = maximum_filter(inArr, size=otherargs.size)

    # If overlap is not set, this should find a few hundred incorrect pixels
    mismatchCount = numpy.count_nonzero(directFiltered != riosFiltered)
    if mismatchCount > 0:
        allOK = False
        msg = "Found {} incorrect pixels of {}".format(mismatchCount,
            directFiltered.size)
        riostestutils.report(TESTNAME, msg)

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def doFilter(info, inputs, outputs, otherargs):
    """
    Apply median filter to input
    """
    outputs.out = maximum_filter(inputs.img, size=otherargs.size)


def readArr(filename):
    """
    Read the whole of first band of given file
    """
    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    return arr
