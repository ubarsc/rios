#!/usr/bin/env python
"""
Generate a set of test images to use in testing the system.

This set of routines uses only simple gdal routines, so as
to be independant of anything inside RIOS. After all, not much use 
using RIOS to generate the input images, if RIOS is the 
thing being tested. 

"""
from __future__ import print_function
import numpy
from osgeo import gdal
from osgeo import osr

from rios import rioserrors

DEFAULT_ROWS = 500
DEFAULT_COLS = 500
DEFAULT_DTYPE = gdal.GDT_Byte

def createTestFile(filename, numRows=DEFAULT_ROWS, numCols=DEFAULT_COLS, 
    dtype=DEFAULT_DTYPE, numBands=1, epsg=28355, xLeft=50000, yTop=700000, xPix=10, yPix=10):
    """
    Create a simple test file, on a standard footprint. Has some fairly arbitrary
    default values for all the relevant characteristics, which can be
    over-ridden as required. 
    
    Returns the dataset object. 
    
    """
    # Using HFA driver, because it has lots of capabilities we can test, and
    # it is always a part of GDAL. 
    driver = gdal.GetDriverByName('HFA')
    creationOptions = ['COMPRESS=YES']
    
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
    ramp = ((x + y) * 100.0 / (nRows-1 + nCols-1)).astype(numpy.uint8)
    return ramp


def genRampImageFile(filename, reverse=False):
    """
    Generate a test image of a simple 2-d linear ramp. 
    """
    ds = createTestFile(filename)
    ramp = genRampArray()
    if reverse:
        # Flip left-to-right
        ramp = ramp[:, ::-1]
    
    band = ds.GetRasterBand(1)
    band.WriteArray(ramp)
    del ds


def report(testName, message):
    """
    Report a test result
    """
    fullMessage = "%s: %s" % (testName, message)
    print(fullMessage)
