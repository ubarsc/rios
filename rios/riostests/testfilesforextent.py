"""
Test the use of controls.setFilesForExtent()

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

from rios import applier, pixelgrid

from . import riostestutils

TESTNAME = "TESTFILESFOREXTENT"

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
    ramp3 = 'ramp3.img'
    riostestutils.genRampImageFile(ramp1)

    # Second file is same as first, but shifted 100 pixels right 
    xLeft = riostestutils.DEFAULT_XLEFT + OFFSET_2ND_IMAGE
    riostestutils.genRampImageFile(ramp2, xLeft=xLeft)
    # Third file is same as first, but shifted 100 pixels down
    yTop = riostestutils.DEFAULT_YTOP - OFFSET_2ND_IMAGE
    riostestutils.genRampImageFile(ramp3, yTop=yTop)

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherArgs = applier.OtherInputs()
    controls = applier.ApplierControls()

    infiles.img1 = ramp1
    infiles.img2 = ramp2
    infiles.img3 = ramp3

    extentfiles = [ramp1, ramp2, ramp3]
    applier.apply(getBounds, infiles, outfiles, otherArgs, controls=controls)
    ok = checkExtent(otherArgs, extentfiles)
    allOK = allOK and ok

    extentfiles = [ramp1, ramp2]
    controls.setFilesForExtent(extentfiles)
    applier.apply(getBounds, infiles, outfiles, otherArgs, controls=controls)
    ok = checkExtent(otherArgs, extentfiles)
    allOK = allOK and ok

    extentfiles = [ramp1]
    controls.setFilesForExtent(extentfiles)
    applier.apply(getBounds, infiles, outfiles, otherArgs, controls=controls)
    ok = checkExtent(otherArgs, extentfiles)
    allOK = allOK and ok

    for fn in [ramp1, ramp2, ramp3]:
        riostestutils.removeRasterFile(fn)
    
    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def getBounds(info, inputs, outputs, otherArgs):
    """
    Called from RIOS.

    Save the extent on otherArgs
    """
    gt = info.getTransform()
    otherArgs.topleft = (gt[3], gt[0])
    (ncols, nrows) = info.getTotalSize()
    otherArgs.shape = (nrows, ncols)


def checkExtent(otherArgs, extentfiles):
    """
    Check the extent deduced inside RIOS with the intersection region found directly
    from extentfiles
    """
    gridList = [pixelgrid.pixelGridFromFile(fn) for fn in extentfiles]
    refGrid = gridList[0]
    workinggrid = pixelgrid.findCommonRegion(gridList, refGrid)

    shape = workinggrid.getDimensions()
    gt = workinggrid.makeGeoTransform()
    topleft = (gt[3], gt[0])
    numFiles = len(extentfiles)

    ok = True
    if topleft != otherArgs.topleft:
        msg = (f"Top-left mis-match. Num extent files = {numFiles}. " +
               f"{topleft} != {otherArgs.topleft}")
        riostestutils.report(TESTNAME, msg)
        ok = False

    if shape != otherArgs.shape:
        msg = (f"Shape mis-match. Numfiles = {numFiles}. " +
               f"{shape} != {otherArgs.shape}")
        riostestutils.report(TESTNAME, msg)
        ok = False

    return ok
