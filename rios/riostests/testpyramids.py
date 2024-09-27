"""
Test that pyramid layers (i.e overviews) are written correctly, in both the
single-pass and GDAL cases
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

import numpy
from osgeo import gdal

from rios import applier

from rios.riostests import riostestutils

TESTNAME = 'TESTPYRAMIDS'


def run():
    """
    Run tests of pyramid layers (i.e. overviews)
    """
    riostestutils.reportStart(TESTNAME)

    allOK = True

    # Create a test input file
    rampInfile = 'ramp.img'
    # Set a raster size which will result in exactly one pyramid layer
    nRows = nCols = 1024
    riostestutils.genRampImageFile(rampInfile, numRows=nRows, numCols=nCols)

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()

    infiles.inimg = rampInfile
    outfiles.outimg = "ramp.tif"
    controls.setOutputDriverName("GTiff")

    for singlePass in [True, False]:
        controls.setSinglePassPyramids(singlePass)
        applier.apply(doit, infiles, outfiles, controls=controls)
        ok = checkPyramids(outfiles.outimg, singlePass)
        allOK = allOK and ok

    for filename in [rampInfile, outfiles.outimg]:
        if os.path.exists(filename):
            riostestutils.removeRasterFile(filename)

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def doit(info, inputs, outputs):
    outputs.outimg = inputs.inimg


def checkPyramids(filename, singlePass):
    """
    Check that the pyramids as written correspond to the base raster
    in the file. If singlePass is True, then we should have centred the low-res
    pixels, if it is False, then GDAL calculated them, and it appears not to
    centre the low-res pixels (which I believe to be a bug).
    """
    ok = True

    ds = gdal.Open(filename)
    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    numOverviews = band.GetOverviewCount()
    if numOverviews != 1:
        msg = "Incorrect overview count: {}".format(numOverviews)
        riostestutils.report(TESTNAME, msg)
        ok = False

    band_ov = band.GetOverview(0)
    arr_ov = band_ov.ReadAsArray()

    # Work out which offset (o) to use
    factor = int(round(arr.shape[0] / arr_ov.shape[0]))
    if singlePass:
        o = int(round(factor / 2))
    else:
        # GDAL doesn't use an offset (sigh....)
        o = 0

    # The true sub-sampled overview array
    true_arr_ov = arr[o::factor, o::factor]
    if true_arr_ov.shape != arr_ov.shape:
        msg = "Overview shape mis-match: {} != {}".format(true_arr_ov.shape,
            arr_ov.shape)
        riostestutils.report(TESTNAME, msg)
        ok = False
    else:
        mismatch = (arr_ov != true_arr_ov)
        numMismatch = numpy.count_nonzero(mismatch)
        if numMismatch > 0:
            msg = "Pyramid layer pixel mis-match for {} pixels".format(numMismatch)
            riostestutils.report(TESTNAME, msg)
            ok = False

    return ok


if __name__ == "__main__":
    run()
