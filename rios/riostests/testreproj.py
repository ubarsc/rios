"""
Test reprojection of input file.

Reprojects an input file using GDAL directly, then reads these two files as
RIOS inputs, allowing RIOS to reproject the original on-the-fly. They should
thus be the same when checked inside the userFunction. The check is a per-pixel
match, looking for zero mis-matched pixel values.

This test is a little bit flaky, because the code in GDAL is not completely
stable. With this in mind, don't be in a hurry to change this test much.

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
from osgeo import gdal, osr
from rios import applier

from . import riostestutils
from ..pixelgrid import PixelGridDefn

TESTNAME = "TESTREPROJECTION"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    
    ramp1 = 'ramp1.img'
    ramp2 = 'ramp2.img'
    nullval = 0
    riostestutils.genRampImageFile(ramp1, nullVal=nullval)

    resample = "near"
    reprojFile(ramp1, ramp2, resample, nullval)
    
    ok = checkMatch(ramp1, ramp2, resample)
    
    # Clean up
    for filename in [ramp1, ramp2]:
        riostestutils.removeRasterFile(filename)

    if ok:
        riostestutils.report(TESTNAME, "Passed")

    return ok


def reprojFile(ramp1, ramp2, resampleMethod, nullval):
    """
    Use gdalwarp to reproject the file, independently of RIOS. Does a
    bit of work to ensure that the extent of the reprojected file is aligned
    to nice neat numbers, in a way which matches what RIOS will probably do
    """
    xRes = riostestutils.DEFAULT_PIXSIZE
    yRes = riostestutils.DEFAULT_PIXSIZE
    dstSrs = osr.SpatialReference()
    dstSrs.ImportFromEPSG(3577)
    dstProj = dstSrs.ExportToWkt()
    srcSrs = osr.SpatialReference()
    srcSrs.ImportFromEPSG(riostestutils.DEFAULT_EPSG)
    tr = osr.CoordinateTransformation(srcSrs, dstSrs)
    (tlx_a, tly_a) = (riostestutils.DEFAULT_XLEFT, riostestutils.DEFAULT_YTOP)
    brx_a = (riostestutils.DEFAULT_XLEFT +
        (riostestutils.DEFAULT_COLS + 1) * xRes)
    bry_a = (riostestutils.DEFAULT_YTOP -
        (riostestutils.DEFAULT_ROWS + 1) * yRes)
    (tlx_b, tly_b, z) = tr.TransformPoint(tlx_a, tly_a)
    (blx_b, bly_b, z) = tr.TransformPoint(tlx_a, bry_a)
    (trx_b, try_b, z) = tr.TransformPoint(brx_a, tly_a)
    (brx_b, bry_b, z) = tr.TransformPoint(brx_a, bry_a)
    xMin_b = min(tlx_b, blx_b, trx_b, brx_b)
    xMax_b = max(tlx_b, blx_b, trx_b, brx_b)
    yMin_b = min(tly_b, bly_b, try_b, bry_b)
    yMax_b = max(tly_b, bly_b, try_b, bry_b)
    xMin_b = PixelGridDefn.snapToGrid(xMin_b, 0, xRes)
    xMax_b = PixelGridDefn.snapToGrid(xMax_b, 0, xRes)
    yMin_b = PixelGridDefn.snapToGrid(yMin_b, 0, yRes)
    yMax_b = PixelGridDefn.snapToGrid(yMax_b, 0, yRes)
    outBounds = (xMin_b, yMin_b, xMax_b, yMax_b)

    # Need to do the reprojection to a VRT, otherwise GDAL does not
    # exactly match. This is due to some glitch in the VRT code, not
    # in RIOS code.
    warpOptions = gdal.WarpOptions(format="VRT", 
        xRes=xRes, yRes=yRes, outputBounds=outBounds,
        srcNodata=nullval, dstSRS=dstProj, dstNodata=nullval,
        overviewLevel="NONE", resampleAlg=resampleMethod)
    gdal.Warp(ramp2, ramp1, options=warpOptions)


def checkMatch(ramp1, ramp2, resample):
    """
    Use RIOS to check that the files are the same. Ramp1 will be reprojected
    on-the-fly by RIOS, to match ramp2.
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    otherargs = applier.OtherInputs()
    controls = applier.ApplierControls()

    infiles.img1 = ramp1
    infiles.img2 = ramp2
    otherargs.mismatchCount = 0
    otherargs.pixelCount = 0
    controls.setReferenceImage(ramp2)
    controls.setResampleMethod(resample)

    applier.apply(userFunc, infiles, outfiles, otherargs, controls=controls)

    ok = True
    if otherargs.pixelCount == 0:
        msg = "No pixels counted"
        riostestutils.report(TESTNAME, msg)
        ok = False

    if otherargs.mismatchCount > 0:
        msg = "Mis-match on {} out of {} pixels".format(otherargs.mismatchCount,
            otherargs.pixelCount)
        riostestutils.report(TESTNAME, msg)
        ok = False

    return ok


def userFunc(info, inputs, outputs, otherargs):
    """
    Check pixel-by-pixel match
    """
    match = (inputs.img1 == inputs.img2)
    mismatchCount = numpy.count_nonzero(~match)
    otherargs.mismatchCount += mismatchCount
    otherargs.pixelCount += inputs.img1.size
