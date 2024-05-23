"""
Test the behaviour of the footprint type. Reads two input files
which have different extents, and checks that each of the footprint types
behaves as it should. Currently tests with images which are offset
from each other, but in the same projection, making calculation of the
correct answer fairly simple.

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
from rios import applier, fileinfo

from . import riostestutils

TESTNAME = "TESTFOOTPRINT"

PIX = riostestutils.DEFAULT_PIXSIZE
OFFSET_PIXELS = 100
OFFSET_2ND_IMAGE = OFFSET_PIXELS * PIX


def run():
    """
    Run the test
    """
    allOK = True
    
    riostestutils.reportStart(TESTNAME)

    ramp1 = 'ramp1.img'
    ramp2 = 'ramp2.img'
    outimg = 'outimg.img'
    riostestutils.genRampImageFile(ramp1)

    # Second file is same as first, but shifted 100 pixels right and down. 
    xLeft = riostestutils.DEFAULT_XLEFT + OFFSET_2ND_IMAGE
    yTop = riostestutils.DEFAULT_YTOP - OFFSET_2ND_IMAGE
    riostestutils.genRampImageFile(ramp2, xLeft=xLeft, yTop=yTop)
    
    # Set up some RIOS calls
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()

    infiles.img1 = ramp1
    infiles.img2 = ramp2

    outfiles.outimg = outimg
    footprintList = [applier.INTERSECTION, applier.UNION,
        applier.BOUNDS_FROM_REFERENCE]
    allOK = True
    for footprintType in footprintList:
        controls.setFootprintType(footprintType)
        if footprintType == applier.BOUNDS_FROM_REFERENCE:
            controls.setReferenceImage(ramp1)

        applier.apply(doSomething, infiles, outfiles, controls=controls)

        ok = checkOutputExtent(ramp1, ramp2, outimg, footprintType)
        allOK = allOK and ok

    for fn in [ramp1, ramp2, outimg]:
        riostestutils.removeRasterFile(fn)
    
    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def doSomething(info, inputs, outputs):
    """
    Do nothing of importance, just to write an output file
    """
    shape = inputs.img1.shape
    dtype = inputs.img1.dtype
    outputs.outimg = numpy.zeros(shape, dtype=dtype)


def checkOutputExtent(ramp1, ramp2, outimg, footprintType):
    """
    Check that the extent of the output image is appropriate for the
    given footprint type and the input extents.

    Return True is extent is correct
    """
    info1 = fileinfo.ImageInfo(ramp1)
    info2 = fileinfo.ImageInfo(ramp2)
    outInfo = fileinfo.ImageInfo(outimg)
    outExtent = makeExtentTuple(outInfo)

    if footprintType == applier.INTERSECTION:
        correctExtent = (max(info1.xMin, info2.xMin),
                         min(info1.xMax, info2.xMax),
                         max(info1.yMin, info2.yMin),
                         min(info1.yMax, info2.yMax))
    elif footprintType == applier.UNION:
        correctExtent = (min(info1.xMin, info2.xMin),
                         max(info1.xMax, info2.xMax),
                         min(info1.yMin, info2.yMin),
                         max(info1.yMax, info2.yMax))
    elif footprintType == applier.BOUNDS_FROM_REFERENCE:
        # We set reference image as ramp1, so extent of that should
        # carry through
        correctExtent = makeExtentTuple(info1)

    ok = (outExtent == correctExtent)
    if not ok:
        msg = ("Extent mis-match for footprint type: {}\n" +
               "{} != {}")
        msg = msg.format(footprintType, outExtent, correctExtent)
        riostestutils.report(TESTNAME, msg)

    return ok

    
def makeExtentTuple(info):
    """
    Make a single tuple of the extent, to allow for quick comparisons
    of whole extents
    """
    extent = (info.xMin, info.xMax, info.yMin, info.yMax)
    return extent
