#!/usr/bin/env python
"""
Generate a set of test images to use in testing the system.

This set of routines uses only simple gdal routines, so as
to be independant of anything inside RIOS. After all, not much use 
using RIOS to generate the input images, if RIOS is the 
thing being tested. 

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
from __future__ import print_function, division

import numpy
from osgeo import gdal
from osgeo import osr
from osgeo import ogr

from rios import rioserrors

gdal.UseExceptions()

DEFAULT_ROWS = 500
DEFAULT_COLS = 500
DEFAULT_PIXSIZE = 10
DEFAULT_DTYPE = gdal.GDT_Byte
DEFAULT_XLEFT = 500000
DEFAULT_YTOP = 7000000
DEFAULT_EPSG = 28355


def createTestFile(filename, numRows=DEFAULT_ROWS, numCols=DEFAULT_COLS, 
        dtype=DEFAULT_DTYPE, numBands=1, epsg=28355, xLeft=DEFAULT_XLEFT, 
        yTop=DEFAULT_YTOP, xPix=DEFAULT_PIXSIZE, yPix=DEFAULT_PIXSIZE, 
        driverName='HFA', creationOptions=['COMPRESS=YES']):
    """
    Create a simple test file, on a standard footprint. Has some fairly arbitrary
    default values for all the relevant characteristics, which can be
    over-ridden as required. 
    
    Returns the dataset object. 
    
    """
    # Unless otherwise specified, use HFA driver, because it has lots of capabilities 
    # we can test, and it is always a part of GDAL. 
    driver = gdal.GetDriverByName(driverName)
    
    ds = driver.Create(filename, numCols, numRows, numBands, dtype, creationOptions)
    if ds is None:
        raise rioserrors.ImageOpenError('Cannot create an image')
        
    geotransform = (xLeft, xPix, 0, yTop, 0, -yPix)
    ds.SetGeoTransform(geotransform)
    
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)
    projWKT = sr.ExportToWkt()
    ds.SetProjection(projWKT)
    
    return ds


def genRampArray(nRows=DEFAULT_ROWS, nCols=DEFAULT_COLS):
    """
    Generate a simple 2-d linear ramp. Returns a numpy array of the data
    """
    (x, y) = numpy.mgrid[:nRows, :nCols]
    ramp = ((x + y) * 100.0 / (nRows - 1 + nCols - 1)).astype(numpy.uint8)
    return ramp


def genRampImageFile(filename, reverse=False, xLeft=DEFAULT_XLEFT,
        yTop=DEFAULT_YTOP, nullVal=None):
    """
    Generate a test image of a simple 2-d linear ramp. 
    """
    ds = createTestFile(filename, xLeft=xLeft, yTop=yTop)
    ramp = genRampArray()
    if reverse:
        # Flip left-to-right
        ramp = ramp[:, ::-1]
    
    band = ds.GetRasterBand(1)
    band.WriteArray(ramp)
    if nullVal is not None:
        band.SetNoDataValue(nullVal)
    del ds


def genThematicFile(filename):
    """
    Generate a thematic file
    """
    ds = createTestFile(filename)
    
    band = ds.GetRasterBand(1)
    arr = numpy.zeros((DEFAULT_ROWS, DEFAULT_COLS))
    band.WriteArray(arr)
    
    band.SetMetadataItem('LAYER_TYPE', 'thematic')
    del ds


def genRowColImage(filename, nrows, ncols, xPix, yPix, xLeft, yTop):
    """
    Generate a 2-layer image. For each pixel, the two layer values
    are the row and column number for that pixel
    """
    ds = createTestFile(filename, numRows=nrows, numCols=ncols,
        dtype=gdal.GDT_UInt16, numBands=2, xLeft=xLeft, yTop=yTop,
        xPix=xPix, yPix=yPix)

    (row, col) = numpy.mgrid[:nrows, :ncols]
    row = row.astype(numpy.uint16)
    col = col.astype(numpy.uint16)

    band = ds.GetRasterBand(1)
    band.WriteArray(row)
    band = ds.GetRasterBand(2)
    band.WriteArray(col)
    band.FlushCache()
    ds.FlushCache()
    del ds


def genVectorSquare(filename, epsg=DEFAULT_EPSG):
    """
    Generate a square, which would lie inside the rasters generated by the
    routines above.
    
    """
    driver = ogr.GetDriverByName('ESRI Shapefile')
    ds = driver.CreateDataSource(filename)
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(epsg)

    layer = ds.CreateLayer(filename, geom_type=ogr.wkbPolygon, srs=sr)
    
    squareSize = 20
    xmin = DEFAULT_XLEFT + 10.6 * DEFAULT_PIXSIZE
    xmax = xmin + squareSize * DEFAULT_PIXSIZE
    ymin = DEFAULT_YTOP - 30.6 * DEFAULT_PIXSIZE
    ymax = ymin + squareSize * DEFAULT_PIXSIZE
    
    corners = [
        [xmin, ymax], [xmax, ymax], [xmax, ymin], [xmin, ymin], [xmin, ymax]
    ]
    cornersStrList = ["%s %s"%(x, y) for (x, y) in corners]
    cornersStr = ','.join(cornersStrList)
    squareWKT = "POLYGON((%s))" % cornersStr
    geom = ogr.Geometry(wkt=squareWKT)
    featureDefn = ogr.FeatureDefn()
    feature = ogr.Feature(featureDefn)
    feature.SetGeometry(geom)
    layer.CreateFeature(feature)
    
    del layer
    del ds


def report(testName, message):
    """
    Report a test result
    """
    fullMessage = "%s: %s" % (testName, message)
    print(fullMessage)


def reportStart(testName):
    """
    Report the beginning of a given test
    """
    print("\n####################")
    print("Starting test:", testName)


def removeRasterFile(filename):
    """
    Remove the given GDAL raster file, using the appropriate driver.
    Mainly called to remove temporary files created by the tests. 
    """
    drvr = gdal.IdentifyDriver(filename)
    if drvr is not None:
        drvr.Delete(filename)


def removeVectorFile(filename):
    """
    Remove the given OGR vector file, using the appropriate driver. 
    """
    ds = ogr.Open(filename)
    drvr = ds.GetDriver()
    del ds
    drvr.DeleteDataSource(filename)


def testAll():
    """
    Runs all the tests - called from testrios.py

    Returns number of tests that fail
    """
    failureCount = 0

    from . import testavg
    ok = testavg.run()
    if not ok:
        failureCount += 1

    from . import testresample
    ok = testresample.run()
    if not ok:
        failureCount += 1

    from . import testcolortable
    ok = testcolortable.run()
    if not ok:
        failureCount += 1

    from . import testvector
    ok = testvector.run()
    if not ok:
        failureCount += 1

    from . import testreaderinfo
    ok = testreaderinfo.run()
    if not ok:
        failureCount += 1

    from . import testcoords
    ok = testcoords.run()
    if not ok:
        failureCount += 1

    from . import teststats
    ok = teststats.run()
    if not ok:
        failureCount += 1
    
    from . import testrat
    ok = testrat.run()
    if not ok:
        failureCount += 1

    from . import testcolortable
    ok = testcolortable.run()
    if not ok:
        failureCount += 1

    from . import testratcolortable
    ok = testratcolortable.run()
    if not ok:
        failureCount += 1

    from . import testratstats
    ok = testratstats.run()
    if not ok:
        failureCount += 1

    from . import testratapplier
    ok = testratapplier.run()
    if not ok:
        failureCount += 1
    
    from . import testlayerselection
    ok = testlayerselection.run()
    if not ok:
        failureCount += 1

    from . import testavgthreads
    ok = testavgthreads.run()
    if not ok:
        failureCount += 1

    from . import testavgsubproc
    ok = testavgsubproc.run()
    if not ok:
        failureCount += 1

    from . import testapplyreturn
    ok = testapplyreturn.run()
    if not ok:
        failureCount += 1

    from . import testavgmulti
    ok = testavgmulti.run()
    if not ok:
        failureCount += 1

    try:
        from . import testavgmpi
        ok = testavgmpi.run()
        if not ok:
            failureCount += 1
    except ImportError:
        print("Skipped MPI test due to failed import - mpi4py needed")

    # After all tests
    print()
    print()
    report("ALL TESTS", "Completed, with %d failure(s)" % failureCount)

    return failureCount
