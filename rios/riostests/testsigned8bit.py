"""
Test the use of signed 8-bit rasters. The Int8 type was added to GDAL
in version 3.7, and only later supported properly by RIOS.

This does not test the earlier GDAL implementation using 'PIXELTYPE=SIGNEDBYTE',
as this is not supported by RIOS.
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
import traceback

import numpy
from osgeo import gdal
from osgeo.gdal_array import GDALTypeCodeToNumericTypeCode

from rios import applier, VersionObj

from rios.riostests import riostestutils


TESTNAME = 'TESTSIGNED8BIT'


def run():
    """
    Run a test of statistics calculation
    """
    riostestutils.reportStart(TESTNAME)

    allOK = True

    if not hasattr(gdal, 'GDT_Int8'):
        msg = "GDT_Int8 support only available with GDAL >= 3.7"
        riostestutils.report(TESTNAME, msg)
        allOK = False

    (nRows, nCols) = (1, 256)
    if allOK:
        inimg = "int8.tif"
        outimg = "int8_2.tif"
        ok = readAndWrite(inimg, outimg, nRows, nCols)
        allOK = ok

    if allOK:
        ok = checkHistogram(outimg, nCols)
        allOK = ok

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    for fn in [inimg, outimg]:
        if os.path.exists(fn):
            riostestutils.removeRasterFile(fn)

    return allOK


def readAndWrite(inimg, outimg, nRows, nCols):
    """
    Create a small signed 8-bit raster, and use RIOS to read it and write it
    out again, testing both the reading and writing
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    otherargs = applier.OtherInputs()

    infiles.inimg = inimg
    outfiles.outimg = outimg
    controls.setOutputDriverName("GTiff")
    controls.setStatsIgnore(None)

    creationOptions = ['COMPRESS=DEFLATE', 'TILED=YES']
    gdt = gdal.GDT_Int8
    ds = riostestutils.createTestFile(infiles.inimg,
        numRows=nRows, numCols=nCols, driverName='GTiff',
        dtype=gdt, creationOptions=creationOptions)

    arr = numpy.arange(-128, 128, dtype=numpy.int8).reshape((nRows, nCols))
    ds.GetRasterBand(1).WriteArray(arr)
    del ds

    ok = True
    try:
        applier.apply(copy, infiles, outfiles, otherargs,
            controls=controls)
        if otherargs.inDtype != numpy.int8:
            msg = "Input dtype is {}, should be int8".format(otherargs.inDtype)
            riostestutils.report(TESTNAME, msg)
            ok = False
    except Exception as e:
        tbStr = ''.join(traceback.format_exception(e))
        msg = f"Exception raised:\n{tbStr}"
        riostestutils.report(TESTNAME, msg)
        ok = False

    return ok


def checkHistogram(outimg, ncols):
    """
    Check that the histogram on the output file is correct
    """
    # We got through without an exception, now check that the values are correct
    ds = gdal.Open(outimg)
    band = ds.GetRasterBand(1)
    md = band.GetMetadata()
    ok = True
    if int(md['STATISTICS_HISTOMIN']) != -128:
        msg = "HISTOMIN incorrect {} != {}".format(md['STATISTICS_HISTOMIN'], -128)
        riostestutils.report(TESTNAME, msg)
        ok = False
    if int(md['STATISTICS_HISTOMAX']) != 127:
        msg = "HISTOMAX incorrect {} != {}".format(md['STATISTICS_HISTOMAX'], 127)
        riostestutils.report(TESTNAME, msg)
        ok = False
    trueHist = numpy.ones(ncols)
    histStr = md['STATISTICS_HISTOBINVALUES']
    hist = numpy.array([int(i) for i in histStr.split('|')[:-1]])
    if (hist != trueHist).any():
        msg = "HISTOBINVALUES incorrect {} != {}".format(hist, trueHist)
        riostestutils.report(TESTNAME, msg)
        ok = False

    return ok


def copy(info, inputs, outputs, otherargs):
    """
    Called from RIOS. Write input to output
    """
    otherargs.inDtype = inputs.inimg.dtype
    outputs.outimg = inputs.inimg


if __name__ == "__main__":
    run()
