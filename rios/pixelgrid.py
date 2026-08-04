"""
Utility class PixelGridDefn, which defines a pixel grid,
plus useful operations on it. 

Utility function findCommonRegion() to find the union 
or intersection region of a list GDAL datasets, in a 
reference grid. 

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

from . import const
from . import rioserrors
from . import fileinfo
from osgeo import osr
from osgeo import gdal


class PixelGridDefn(object):
    """
    Definition of a pixel grid, including
    the size and extent, and by implication the
    resolution and alignment. 
    
    Methods are defined for relationships with
    other instances, including:

        * intersection()
        * union()
        * reproject()
        * alignedWith()
        * isComparable()
    
    Attributes defined on the object:

        * xMin
        * xMax
        * yMin
        * yMax
        * xRes
        * yRes
        * projection
        
    The bounds define the external corners of the image, i.e. the
    top-left corner of the top-left pixel, through to the bottom-right
    corner of the bottom-right pixel. This is consistent with GDAL
    conventions.

    The projection is given as a WKT string.

    The constructor takes the projection, and EITHER a complete GDAL
    geotransform tuple, with the number of rows and columns, OR a grid
    specified with all the extent limits and the pixel resolutions
    (xRes and yRes). If the geotransform is given, then the xMin, xMax, xRes
    and so on are calculated from it.
    
    """
    def __init__(self, geotransform=None, nrows=None, ncols=None, projection=None,
            xMin=None, xMax=None, yMin=None, yMax=None, xRes=None, yRes=None):
        """
        Can define in terms of a GDAL geotransform plus
        the number of rows and columns. 
        Can also be defined by giving xMin, xMax, yMin, yMax, 
        and xRes, yRes directly. 
        
        Projection is given as a WKT string. 
        
        """
        self.projection = projection
        self.xMin = xMin
        self.xMax = xMax
        self.yMin = yMin
        self.yMax = yMax
        self.xRes = xRes
        self.yRes = yRes
        if geotransform is not None and nrows is not None and ncols is not None:
            self.xRes = geotransform[1]
            self.yRes = abs(geotransform[5])
            self.xMin = geotransform[0]
            self.yMax = geotransform[3]
            self.xMax = self.xMin + ncols * self.xRes
            self.yMin = self.yMax - nrows * self.yRes
    
    def __str__(self):
        s = "xMin:%s,xMax:%s,yMin:%s,yMax:%s,xRes:%s,yRes:%s" % (self.xMin, self.xMax,
            self.yMin, self.yMax, self.xRes, self.yRes)
        return s
    
    def alignedWith(self, other):
        """
        Returns True if self is aligned with other. This means that
        they represent the same grid, with different extents. 
        
        Alignment is checked within a small tolerance, so that exact 
        floating point matches are not required. However, notionally it
        is possible to get a match which shouldn't be. The tolerance is
        calculated as::
 
            tolerance = 0.01 * pixsize / npix

        and if a mis-alignment is <= tolerance, it is assumed to be zero. 
        For further details, read the source code. 
        
        """
        aligned = True
        if not self.isComparable(other):
            aligned = False
        
        # Calculate a tolerance, based on the pixel size and the number of pixels,
        # so that when the tolerance is accumulated across the whole grid, the 
        # total still comes out to be well under a pixel. 
        # First get the largest dimension of either grid
        npix = self.getNumPix(self.xMax, self.xMin, self.xRes)
        npix = max(npix, self.getNumPix(other.xMax, other.xMin, other.xRes))
        npix = max(npix, self.getNumPix(self.yMax, self.yMin, self.yRes))
        npix = max(npix, self.getNumPix(other.yMax, other.yMin, other.yRes))
        res = min(self.xRes, self.yRes)
        tolerance = 0.001 * res / npix
        
        xMinSnapped = self.snapToGrid(self.xMin, other.xMin, self.xRes)
        if abs(xMinSnapped - self.xMin) > tolerance:
            aligned = False
        yMaxSnapped = self.snapToGrid(self.yMax, other.yMax, self.yRes)
        if abs(yMaxSnapped - self.yMax) > tolerance:
            aligned = False

        return aligned

    def intersection(self, other):
        """
        Returns a new instance which is the intersection 
        of self and other. 
        
        """
        if not self.isComparable(other):
            return None
            
        xMin = max(self.xMin, other.xMin)
        xMax = min(self.xMax, other.xMax)
        yMin = max(self.yMin, other.yMin)
        yMax = min(self.yMax, other.yMax)

        if xMin >= xMax or yMin >= yMax:
            msg = "Images don't intersect"
            raise rioserrors.IntersectionError(msg)
        
        newPixelGrid = PixelGridDefn(xMin=xMin, xMax=xMax, yMin=yMin, yMax=yMax, 
            xRes=self.xRes, yRes=self.yRes, projection=self.projection)
        return newPixelGrid
    
    def union(self, other):
        """
        Returns a new instance which is the union of self
        with other. 
        
        """
        if not self.isComparable(other):
            return None
            
        xMin = min(self.xMin, other.xMin)
        xMax = max(self.xMax, other.xMax)
        yMin = min(self.yMin, other.yMin)
        yMax = max(self.yMax, other.yMax)
        
        newPixelGrid = PixelGridDefn(xMin=xMin, xMax=xMax, yMin=yMin, yMax=yMax, 
            xRes=self.xRes, yRes=self.yRes, projection=self.projection)
        return newPixelGrid

    def isComparable(self, other):
        """
        Checks whether self is comparable with other. Returns
        True or False. Grids are comparable if they have equal pixel
        size, and the same projection. 
        
        """
        comparable = True
        # Check resolution
        if not self.equalPixSize(other):
            comparable = False
        # Check projection
        if not self.equalProjection(other):
            comparable = False
        return comparable

    def equalPixSize(self, other):
        """
        Returns True if pixel size of self is equal to that of other. 
        Currently only checks absolute equality, probably should 
        work out a tolerance. 
        
        """
        return (self.xRes == other.xRes and self.yRes == other.yRes)
    
    def equalProjection(self, other):
        """
        Returns True if the projection of self is the same as the 
        projection of other
        
        """
        selfProj = str(self.projection) if self.projection is not None else ''
        otherProj = str(other.projection) if other.projection is not None else ''
        srSelf = osr.SpatialReference(wkt=selfProj)
        srOther = osr.SpatialReference(wkt=otherProj)
        return bool(srSelf.IsSame(srOther))
        
    def equivalentProjection(self, otherspatialref, pixtolerance):
        """
        Similar to equalProjection but does a less accurate test
        by checking converting coordinates from projection of self
        to otherspatialref and checking they are within pixtolerance
        pixels of each other.
        The coordinates used for this are the four corners and centre
        of image.
        """
        srSelf = osr.SpatialReference(str(self.projection))
        fileinfo.preventGdal3axisSwap(srSelf)
        # Make a private copy of the other SR, to avoid damaging what they gave us. 
        otherspatialrefCopy = osr.SpatialReference(wkt=otherspatialref.ExportToWkt())
        fileinfo.preventGdal3axisSwap(otherspatialrefCopy)
        transform = osr.CoordinateTransformation(srSelf, otherspatialrefCopy)

        xtol = pixtolerance * self.xRes
        ytol = pixtolerance * self.yRes
        
        points = []
        points.append((self.xMin, self.yMax))  # upper left
        points.append((self.xMax, self.yMax))  # upper right
        points.append((self.xMax, self.yMin))  # bottom right
        points.append((self.xMin, self.yMin))  # bottom left
        points.append((self.xMin + ((self.xMax - self.xMin) / 2), 
            self.yMin + ((self.yMax - self.yMin) / 2)))  # middle

        equal = True
        for (x, y) in points:
            (otherx, othery, z) = transform.TransformPoint(x, y)
            if abs(x - otherx) > xtol or abs(y - othery) > ytol:
                equal = False
                break

        return equal
    
    def makeGeoTransform(self):
        """
        Returns a GDAL geotransform tuple from bounds and resolution
        
        """
        geotransform = (self.xMin, self.xRes, 0.0, self.yMax, 0.0, -self.yRes)
        return geotransform
        
    def reproject(self, targetGrid):
        """
        Returns a new instance which is the reprojection of self to be
        in the same projection and pixel size as targetGrid.

        The bounding box is created by reprojecting the densely-sampled
        edges, so that the extent will fully cover reprojections which curve
        an edge outwards from the source bounding box. For small extents
        this is usually trivial, but for large extents it can become quite
        important.

        The edge curves are sampled at many points along each edge, which
        obviously does not absolutely guarantee the fullest extent. Ideally one
        would do a gradient search to find the local extreme point along the
        edge, but I was not convinced that there could never be multiple local
        extremes, depending on the combination of projections and extents
        involved, so I concluded that a fairly dense sampling was sufficiently
        good. However, it should be noted that a large region, with a suitable
        combination of projections, may still clip off small parts of the
        bounding box.
        
        """
        srSelf = osr.SpatialReference(str(self.projection))
        fileinfo.preventGdal3axisSwap(srSelf)
        srTarget = osr.SpatialReference(str(targetGrid.projection))
        fileinfo.preventGdal3axisSwap(srTarget)
        t = osr.CoordinateTransformation(srSelf, srTarget)

        # Make dense lines for each of the 4 edges, with n points on each edge
        n = 101
        (xMin, xMax, yMin, yMax) = (self.xMin, self.xMax, self.yMin, self.yMax)
        edge1 = self.densifyInterval(xMin, yMax, xMax, yMax, n)
        edge2 = self.densifyInterval(xMax, yMin, xMax, yMax, n)
        edge3 = self.densifyInterval(xMin, yMin, xMax, yMin, n)
        edge4 = self.densifyInterval(xMin, yMin, xMin, yMax, n)
        allEdges = numpy.concatenate((edge1, edge2, edge3, edge4), axis=0)

        # Reproject dense outline into target projection
        allEdges_tgt = t.TransformPoints(allEdges)
        allEdges_tgt = numpy.array(allEdges_tgt)

        # Find the bounding box
        xMin_tgt = allEdges_tgt[:, 0].min()
        xMax_tgt = allEdges_tgt[:, 0].max()
        yMin_tgt = allEdges_tgt[:, 1].min()
        yMax_tgt = allEdges_tgt[:, 1].max()
        
        # Snap bounds to align with those in target grid
        xMin_tgt = self.snapToGrid(xMin_tgt, targetGrid.xMin, targetGrid.xRes)
        xMax_tgt = self.snapToGrid(xMax_tgt, targetGrid.xMin, targetGrid.xRes)
        yMin_tgt = self.snapToGrid(yMin_tgt, targetGrid.yMin, targetGrid.yRes)
        yMax_tgt = self.snapToGrid(yMax_tgt, targetGrid.yMin, targetGrid.yRes)
        
        # Construct a new pixel grid object
        newPixelGrid = PixelGridDefn(xMin=xMin_tgt, xMax=xMax_tgt,
            yMin=yMin_tgt, yMax=yMax_tgt, xRes=targetGrid.xRes,
            yRes=targetGrid.yRes, projection=targetGrid.projection)
        
        return newPixelGrid

    @staticmethod
    def densifyInterval(x1, y1, x2, y2, n):
        """
        Create a densely sampled sequence of points along an interval, given
        the end points of the interval, (x1, y1) and (x2, y2). The total number
        of points desired is n, including the given end points.

        This is an internal utility function, used by reproject().

        Returns a single array of shape (n, 2), with x in the first column.
        """
        nn = n * 1j
        x = numpy.mgrid[x1:x2:nn].reshape((n, 1))
        y = numpy.mgrid[y1:y2:nn].reshape((n, 1))
        xy = numpy.concatenate((x, y), axis=1)
        return xy
    
    def getDimensions(self):
        """
        Utility method which returns the number of rows and columns
        in the grid. Returns a tuple::

            (nrows, ncols)

        calculated from the min/max/res values. 
        
        """
        nrows = self.getNumPix(self.yMax, self.yMin, self.yRes)
        ncols = self.getNumPix(self.xMax, self.xMin, self.xRes)
        return (nrows, ncols)

    @staticmethod
    def roundAway(x):
        """
        Simulates Python 2 round behavour
        This is what we want as it rounds away from 0.
        The decimal module seems to be the only way to do this properly
        """
        import decimal
        dec = decimal.Decimal(x).quantize(decimal.Decimal('1'), 
                rounding=decimal.ROUND_HALF_UP)
        return float(dec.to_integral_value())
        
    @staticmethod
    def getNumPix(gridMax, gridMin, gridRes):
        """
        Works out how many pixels lie between the given min and max, 
        at the given resolution. This is for internal use only. 
        """
        npix = int(PixelGridDefn.roundAway((gridMax - gridMin) / gridRes))
        return npix
    
    @staticmethod
    def snapToGrid(val, valOnGrid, res):
        """
        Returns the nearest value to val which is a whole multiple of
        res away from valOnGrid, so that val is effectively on the same
        grid as valOnGrid. This is for internal use only. 
        
        """
        diff = val - valOnGrid
        numPix = diff / res
        numWholePix = PixelGridDefn.roundAway(numPix)
        snappedVal = valOnGrid + numWholePix * res
        return snappedVal


def findCommonRegion(gridList, refGrid, combine=const.INTERSECTION):
    """
    Returns a PixelGridDefn for the combination of all the grids 
    in the given gridList. The output grid is in the same coordinate 
    system as the reference grid. 
    
    The combine parameter controls whether UNION, INTERSECTION 
    or BOUNDS_FROM_REFERENCE is performed. 
    
    """
    if combine == const.BOUNDS_FROM_REFERENCE:
        newGrid = refGrid
    else:
        newGrid = None
        for grid in gridList:
            if not refGrid.alignedWith(grid):
                grid = grid.reproject(refGrid)

            if newGrid is None:
                newGrid = grid
            else:
                if combine == const.INTERSECTION:
                    newGrid = newGrid.intersection(grid)
                elif combine == const.UNION:
                    newGrid = newGrid.union(grid)
        
    return newGrid


def pixelGridFromFile(filename):
    """
    Create a PixelGridDefn object for the given image file
    
    """
    ds = gdal.Open(filename)
    geotransform = ds.GetGeoTransform()
    nrows = ds.RasterYSize
    ncols = ds.RasterXSize
    projection = ds.GetProjection()
    pixgrid = PixelGridDefn(geotransform=geotransform, nrows=nrows, ncols=ncols, 
        projection=projection)
    return pixgrid
