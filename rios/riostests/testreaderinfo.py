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
Test the ReaderInfo object for correctness
"""
from __future__ import division

import math
from multiprocessing import cpu_count

from rios import applier, structures

from . import riostestutils

TESTNAME = "TESTREADERINFO"


def run():
    """
    Run the test
    """
    allOK = True
    
    riostestutils.reportStart(TESTNAME)

    filename = "rowcolimg.img"
    (nrows, ncols) = (1000, 900)
    (xRes, yRes) = (20, 20)
    (xLeft, yTop) = (30000, 50000)
    riostestutils.genRowColImage(filename, nrows, ncols, xRes, yRes,
        xLeft, yTop)

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    controls = applier.ApplierControls()
    blocksize = 256
    controls.setWindowSize(blocksize, blocksize)
    infiles.rc = filename

    otherargs.nrows = nrows
    otherargs.ncols = ncols
    otherargs.xtotalblocks = int(math.ceil(ncols / blocksize))
    otherargs.ytotalblocks = int(math.ceil(nrows / blocksize))
    otherargs.blocksize = blocksize
    otherargs.xRes = xRes
    otherargs.yRes = yRes
    otherargs.xLeft = xLeft
    otherargs.yTop = yTop
    otherargs.errors = []

    applier.apply(checkReaderInfo, infiles, outfiles, otherargs,
        controls=controls)

    allOK = (len(otherargs.errors) == 0)
    for msg in otherargs.errors:
        riostestutils.report(TESTNAME, msg)

    # Check the info.getNoDataValueFor & getFilenameFor functions
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    controls = applier.ApplierControls()

    infiles.img1 = "ramp1.img"
    infiles.img2 = ["ramp2.img", "ramp3.img"]
    otherargs.nulls = {
        ('img1', None): 253,
        ('img2', 0): 254,
        ('img2', 1): 255
    }
    otherargs.filenames = {
        ('img1', None): infiles.img1,
        ('img2', 0): infiles.img2[0],
        ('img2', 1): infiles.img2[1]
    }
    otherargs.errors = []

    for key in otherargs.filenames:
        fn = otherargs.filenames[key]
        nullval = otherargs.nulls[key]
        riostestutils.genRampImageFile(fn, nullVal=nullval)

    numComputeWorkers = min(2, cpu_count())
    if (structures.cloudpickle is not None and numComputeWorkers > 1 and
            riostestutils.CAN_BIND_SOCKET):
        # We want this to work across threads and processes
        conc = applier.ConcurrencyStyle(numReadWorkers=2,
            numComputeWorkers=numComputeWorkers,
            computeWorkerKind=applier.CW_SUBPROC)
        controls.setConcurrencyStyle(conc)

        rtn = applier.apply(checkLookupFunctions, infiles, outfiles, otherargs,
            controls=controls)
        errorList = []
        for oa in rtn.otherArgsList:
            errorList.extend(oa.errors)

        ok = (len(errorList) == 0)
        for msg in errorList:
            riostestutils.report(TESTNAME, msg)
        allOK = (ok and allOK)
    else:
        if structures.cloudpickle is None:
            riostestutils.report(TESTNAME,
                "Skipping process test, as cloudpickle unavailable")
        if not riostestutils.CAN_BIND_SOCKET:
            riostestutils.report(TESTNAME,
                "Skipping process test, as cannot bind socket")
        if numComputeWorkers == 1:
            riostestutils.report(TESTNAME,
                "Skipping process test, as only 1 cpu")
    
    # Clean up
    for fn in [filename, infiles.img1] + infiles.img2:
        riostestutils.removeRasterFile(fn)
    
    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def checkReaderInfo(info, inputs, outputs, otherargs):
    """
    For each block, check that the info object corresponds to the
    row/col taken from the input file
    """
    (row, col) = tuple(inputs.rc)

    if info.xsize != otherargs.ncols:
        msg = "Grid X size mis-match: {} != {}".format(
            info.xsize, otherargs.ncols)
        otherargs.errors.append(msg)
    if info.ysize != otherargs.nrows:
        msg = "Grid Y size mis-match: {} != {}".format(
            info.ysize, otherargs.nrows)
        otherargs.errors.append(msg)
    if info.xtotalblocks != otherargs.xtotalblocks:
        msg = "xtotalblocks: {} != {}".format(info.xtotalblocks,
            otherargs.xtotalblocks)
        otherargs.errors.append(msg)

    (nrows, ncols) = row.shape
    if info.blockwidth != ncols:
        msg = "info.blockwidth mis-match: {} != {}".format(info.blockwidth,
            ncols)
        otherargs.errors.append(msg)
    if info.blockheight != nrows:
        msg = "info.blockheight mis-match: {} != {}".format(info.blockheight,
            nrows)
        otherargs.errors.append(msg)

    # The block number, in X and Y directions
    xblock = int(math.ceil(col[0, 0] / otherargs.blocksize))
    yblock = int(math.ceil(row[0, 0] / otherargs.blocksize))
    if info.xblock != xblock:
        msg = "xblock mis-match: {} != {}".format(info.xblock, xblock)
        otherargs.errors.append(msg)
    if info.yblock != yblock:
        msg = "yblock mis-match: {} != {}".format(info.yblock, yblock)
        otherargs.errors.append(msg)

    # Simple coordinate test.
    # Note that there is a separate test for getBlockCoordArrays()
    tlx = otherargs.xLeft + col[0, 0] * otherargs.xRes
    tly = otherargs.yTop - row[0, 0] * otherargs.yRes
    if info.blocktl.x != tlx:
        msg = "tlx mis-match: {} != {}".format(info.blocktl.x, tlx)
        otherargs.errors.append(msg)
    if info.blocktl.y != tly:
        msg = "tly mis-match: {} != {}".format(info.blocktl.y, tly)
        otherargs.errors.append(msg)

    # The info.blockbr point is the coordinates of the bottom-right corner
    # of the bottom-right pixel
    brx = otherargs.xLeft + (col[-1, -1] + 1) * otherargs.xRes
    bry = otherargs.yTop - (row[-1, -1] + 1) * otherargs.yRes
    if info.blockbr.x != brx:
        msg = "brx mis-match: {} != {}".format(info.blockbr.x, brx)
        otherargs.errors.append(msg)
    if info.blockbr.y != bry:
        msg = "bry mis-match: {} != {}".format(info.blockbr.y, bry)
        otherargs.errors.append(msg)


def checkLookupFunctions(info, inputs, outputs, otherargs):
    """
    Check the array-based lookup functions of the info object
    """
    for key in otherargs.filenames:
        (symbName, seqNum) = key
        if seqNum is None:
            arr = getattr(inputs, symbName)
        else:
            arr = getattr(inputs, symbName)[seqNum]

        filename = info.getFilenameFor(arr)
        if otherargs.filenames[key] != filename:
            msg = "Filename mis-match: '{}' != '{}'".format(
                otherargs.filenames[key], filename)
            otherargs.errors.append(msg)

        nullval = info.getNoDataValueFor(arr)
        if otherargs.nulls[key] != nullval:
            msg = "Null value mis-match: {} != {}".format(
                otherargs.nulls[key], nullval)
            otherargs.errors.append(msg)
