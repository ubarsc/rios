#!/usr/bin/env python

"""
This file contains definitions that are
common to all the image reading and 
writing modules
"""

import numpy
from osgeo import gdalconst

from . import rioserrors

INTERSECTION=0
UNION=1


    
class Coord:
  """a simple class that contains one coord"""
  def __init__(self,x,y):
    self.x = x
    self.y = y

def wld2pix(transform,geox,geoy):
  """converts a set of map coords to pixel coords"""
  x = ( transform[0] * transform[5] - 
        transform[2] * transform[3] + transform[2] * geoy - 
        transform[5] * geox ) / ( transform[2] * transform[4] - transform[1] * transform[5] )
       
  y = ( transform[1] * transform[3] - transform[0] * transform[4] -
        transform[1] * geoy + transform[4] * geox ) / ( transform[2] * transform[4] - transform[1] * transform[5] )

  return Coord(x,y)

def pix2wld(transform,x,y):
  """converts a set of pixels coords to map coords"""
  geox = transform[0] + transform[1] * x + transform[2] * y
  geoy = transform[3] + transform[4] * x + transform[5] * y
          
  return Coord(geox,geoy)

# Mappings between numpy datatypes and GDAL datatypes.
# Note that ambiguities are resolved by the order - the first one found 
# is the one chosen. 
dataTypeMapping = [
    (numpy.uint8,gdalconst.GDT_Byte),
    (numpy.bool,gdalconst.GDT_Byte),
    (numpy.int16,gdalconst.GDT_Int16),
    (numpy.uint16,gdalconst.GDT_UInt16),
    (numpy.int32,gdalconst.GDT_Int32),
    (numpy.uint32,gdalconst.GDT_UInt32),
    (numpy.single,gdalconst.GDT_Float32),
    (numpy.float,gdalconst.GDT_Float64)
]

def GDALTypeToNumpyType(gdaltype):
    """
    Given a gdal data type returns the matching
    numpy data type
    """
    for (numpy_type,test_gdal_type) in dataTypeMapping:
        if test_gdal_type == gdaltype:
            return numpy_type
    raise rioserrors.TypeConversionError("Unknown GDAL datatype: %s"%gdaltype)

def NumpyTypeToGDALType(numpytype):
    """
    For a given numpy data type returns the matching
    GDAL data type
    """
    for (test_numpy_type,gdaltype) in dataTypeMapping:
        if test_numpy_type == numpytype:
            return gdaltype
    raise rioserrors.TypeConversionError("Unknown numpy datatype: %s"%numpytype)
