"""
Test reprojection of input file, with particular attention to the edges
of the working grid which can be projected into a curve.

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

from rios.riostests import riostestutils


TESTNAME = "TESTREPROJCURVES"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)

    allOK = True

    circleVal = 100
    circleLLfile = createCircleImg(circleVal)
    circleGdal = reprojGdal(circleLLfile)
    circleRios = reprojRios(circleLLfile, circleGdal)

    ok = compareCircles(circleGdal, circleRios, circleVal)

    allOK = allOK and ok
    
    # Clean up
    for filename in [circleLLfile, circleGdal, circleRios]:
        riostestutils.removeRasterFile(filename)

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


def createCircleImg(circleVal):
    """
    Create an input image in lat/long, with a circle embedded. The extent
    is about the width of Australia, and the circle is near the top
    of Australia, up against the top edge of the extent. This edge will
    curve when reprojected into Australian Albers, and if not handled 
    properly, the working grid edge could clip off the top part of the circle.

    Return the name of the file.

    """
    drvr = gdal.GetDriverByName('HFA')
    (left, right, bottom, top) = (112.0, 154.0, -15.0, -10.0)
    (xRes, yRes) = (0.1, 0.1)
    (width, height) = (right - left, top - bottom)
    (nrows, ncols) = (int(height / yRes), int(width / xRes))

    filename = 'circle.img'
    ds = drvr.Create(filename, ncols, nrows, 1, eType=gdal.GDT_Byte,
                     options=["COMPRESS=YES"])
    srLL = osr.SpatialReference()
    srLL.ImportFromEPSG(4326)
    ds.SetSpatialRef(srLL)
    ds.SetGeoTransform((left, xRes, 0.0, top, 0.0, -yRes))
    band = ds.GetRasterBand(1)

    (row, col) = numpy.mgrid[:nrows, :ncols]
    circleMask = (((row - nrows / 2)**2 + (col - ncols / 2)**2) < (nrows / 2)**2)
    arr = numpy.full((nrows, ncols), 50, dtype=numpy.uint8)
    arr[circleMask] = circleVal
    band.WriteArray(arr)

    return filename


def reprojGdal(circleLLfile):
    """
    Reproject to AustAlbers using gdalwarp
    """
    circleGdal = "circleGdal.img"
    srAlbers = osr.SpatialReference()
    srAlbers.ImportFromEPSG(3577)
    srAlbers.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)

    outRes = 10000  # 10km pixel size
    warpOptions = gdal.WarpOptions(format='HFA', creationOptions=["COMPRESS=YES"],
                                   targetAlignedPixels=True, dstSRS=srAlbers,
                                   xRes=outRes, yRes=outRes)
    gdal.Warp(circleGdal, circleLLfile, options=warpOptions)
    return circleGdal


def reprojRios(circleLLfile, circleGdal):
    """
    Reproject the input file to match the GDAL warped output.
    """
    circleRiosfile = "circle_riosalbers.img"
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()

    infiles.circle = circleLLfile
    outfiles.circle_albers = "circle_riosalbers.img"
    controls.setReferenceImage(circleGdal)
    applier.apply(doNothing, infiles, outfiles, controls=controls)

    return circleRiosfile


def doNothing(info, inputs, outputs):
    """
    Called from RIOS
    """
    outputs.circle_albers = inputs.circle


def compareCircles(circleGdal, circleRios, circleVal):
    """
    Count how many pixels of circleVal appear in each of the two images. They
    should be the same. Return False if not.
    """
    dsGdal = gdal.Open(circleGdal)
    dsRios = gdal.Open(circleRios)
    arrGdal = dsGdal.GetRasterBand(1).ReadAsArray()
    arrRios = dsRios.GetRasterBand(1).ReadAsArray()
    gdalCount = numpy.count_nonzero(arrGdal == circleVal)
    riosCount = numpy.count_nonzero(arrRios == circleVal)
    ok = (gdalCount == riosCount)

    if not ok:
        msg = f"Pixel count mis-match. {gdalCount} != {riosCount}"
        riostestutils.report(TESTNAME, msg)

    return ok

