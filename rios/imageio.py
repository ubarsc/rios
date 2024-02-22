"""
This file contains definitions that are
common to all the image reading and 
writing modules
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

import warnings
from osgeo import gdal
from osgeo import gdal_array

INTERSECTION = 0
UNION = 1
BOUNDS_FROM_REFERENCE = 2       # Bounds of working region are taken from given reference grid


class Coord:
    """a simple class that contains one coord"""
    def __init__(self, x, y):
        self.x = x
        self.y = y


def wld2pix(transform, geox, geoy):
    """converts a set of map coords to pixel coords"""
    inv_transform = gdal.InvGeoTransform(transform)
    x, y = gdal.ApplyGeoTransform(inv_transform, geox, geoy)
    return Coord(x, y)


def pix2wld(transform, x, y):
    """converts a set of pixels coords to map coords"""
    geox, geoy = gdal.ApplyGeoTransform(transform, x, y)
    return Coord(geox, geoy)


def GDALTypeToNumpyType(gdaltype):
    """
    Given a gdal data type returns the matching
    numpy data type
    """
    warnings.warn("Future versions of RIOS may remove this function. " +
        "Use gdal_array.GDALTypeCodeToNumericTypeCode instead",
        DeprecationWarning, stacklevel=2)
    return gdal_array.GDALTypeCodeToNumericTypeCode(gdaltype)


def NumpyTypeToGDALType(numpytype):
    """
    For a given numpy data type returns the matching
    GDAL data type
    """
    warnings.warn("Future versions of RIOS may remove this function. " +
        "Use gdal_array.NumericTypeCodeToGDALTypeCode instead",
        DeprecationWarning, stacklevel=2)
    return gdal_array.NumericTypeCodeToGDALTypeCode(numpytype)
