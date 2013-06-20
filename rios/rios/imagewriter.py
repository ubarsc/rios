
"""
Contains the ImageWriter class

"""
# This file is part of RIOS - Raster I/O Simplification
# Copyright (C) 2012  Sam Gillingha, Neil Flood
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
import math

import numpy
from osgeo import gdal

from . import imageio
from . import rioserrors
from . import rat

def setDefaultDriver():
    """
    Sets some default values into global variables, defining
    what defaults we should use for GDAL driver. On any given
    output file these can be over-ridden, and can be over-ridden globally
    using the environment variables 
        $RIOS_DFLT_DRIVER
        $RIOS_DFLT_DRIVEROPTIONS
    
    If RIOS_DFLT_DRIVER is set, then it should be a gdal short driver name
    If RIOS_DFLT_DRIVEROPTIONS is set, it should be a space-separated list
    of driver creation options, e.g. "COMPRESS=LZW TILED=YES", and should
    be appropriate for the selected GDAL driver. This can also be 'None'
    in which case an empty list of creation options is passed to the driver.
    
    If not otherwise supplied, the default is to use the HFA driver, with compression. 
        
    """
    global DEFAULTDRIVERNAME, DEFAULTCREATIONOPTIONS
    DEFAULTDRIVERNAME = os.getenv('RIOS_DFLT_DRIVER', default='HFA')
    DEFAULTCREATIONOPTIONS = ['COMPRESSED=TRUE','IGNOREUTM=TRUE']
    creationOptionsStr = os.getenv('RIOS_DFLT_DRIVEROPTIONS')
    if creationOptionsStr is not None:
        if creationOptionsStr == 'None':
            # hack for KEA which needs no creation options
            # and LoadLeveler which deletes any env variables
            # set to an empty values
            DEFAULTCREATIONOPTIONS = []
        else:
            DEFAULTCREATIONOPTIONS = creationOptionsStr.split()

setDefaultDriver()
    

def allnotNone(items):
    for i in items:
        if i is None:
            return False
    return True
    
def anynotNone(items):
    for i in items:
        if i is not None:
            return True
    return False

class ImageWriter(object):
    """
    This class is the opposite of the ImageReader class and is designed
    to be used in conjunction. The easiest way to use it is pass the
    info returned by the ImageReader for first iteration to the constructor.
    Otherwise, image size etc must be passed in.
    
    The write() method can be used to write a block (numpy array)at a 
    time to the output image - this is designed to be used at each 
    iteration through the ImageReader object. Otherwise, the writeAt() 
    method can be used to write blocks to arbitary locations.
    
    """
    def __init__(self, filename, drivername=DEFAULTDRIVERNAME, creationoptions=DEFAULTCREATIONOPTIONS,
                    nbands=None, gdaldatatype=None, firstblock=None, 
                    info=None, 
                    xsize=None, ysize=None, transform=None, projection=None,
                        windowxsize=None, windowysize=None, overlap=None):
        """
        filename is the output file to be created. Set driver to name of
        GDAL driver, default it HFA. creationoptions will also need to be
        set if not using HFA since the default probably does not make sense
        for other drivers.
        
        Either pass nbands and gdaldatatype OR firstblock. If you pass 
        firstblock, nbands and gdaldataype will be determined from that block
        and that block written to file.
        
        Also, either pass info (the first argument returned from each iteration
        through ImageReader, generally create this class on the first iteration)
        or xsize, ysize, transform, projection, windowxsize, windowysize and overlap
        If you pass info, these other values will be determined from that
        
        """
                    
        noninfoitems = [xsize,ysize,transform,projection,windowxsize,windowysize,overlap]
        if info is None:
            # check we have the other args
            if not allnotNone(noninfoitems):
                msg = 'If not passing info object, must pass all other image info'
                raise rioserrors.ParameterError(msg)

            # just save these values directly
            self.overlap = overlap
            self.windowxsize = windowxsize
            self.windowysize = windowysize
            self.xtotalblocks = int(math.ceil(float(xsize) / windowxsize))
            self.ytotalblocks = int(math.ceil(float(ysize) / windowysize))
            
        else:
            if anynotNone(noninfoitems):
                msg = 'Passed info object, but other args not None'
                raise rioserrors.ParameterError(msg)
                    
            # grab what we need from the info object
            (xsize,ysize) = info.getTotalSize()
            transform = info.getTransform()
            projection = info.getProjection()
            (self.windowxsize,self.windowysize) = info.getWindowSize()
            self.overlap = info.getOverlapSize()
            (self.xtotalblocks,self.ytotalblocks) = info.getTotalBlocks()

        if firstblock is None and not allnotNone([nbands,gdaldatatype]):
            msg = 'if not passing firstblock, must pass nbands and gdaltype'
            raise rioserrors.ParameterError(msg)
                        
        elif firstblock is not None and anynotNone([nbands,gdaldatatype]):
            msg = 'Must pass one either firstblock or nbands and gdaltype, not all of them'
            raise rioserrors.ParameterError(msg)
                        
        if firstblock is not None:
        	# RIOS only works with 3-d image arrays, where the first dimension is 
            # the number of bands. Check that this is what the user gave us to write. 
            if len(firstblock.shape) != 3:
                raise rioserrors.ArrayShapeError(
                    "Shape of array to write must be 3-d. Shape is actually %s"%repr(firstblock.shape))

            # get the number of bands out of the block
            (nbands,y,x) = firstblock.shape
            # and the datatype
            gdaldatatype = imageio.NumpyTypeToGDALType(firstblock.dtype)
                        
                    
        # Create the output dataset
        driver = gdal.GetDriverByName(drivername)
        self.ds = driver.Create(str(filename), xsize, ysize, nbands, gdaldatatype, creationoptions)
        if self.ds is None:
            msg = 'Unable to create output file %s' % filename
            raise rioserrors.ImageOpenError(msg)
                    
        self.ds.SetProjection(projection)
        self.ds.SetGeoTransform(transform)
            
        # start writing at the first block
        self.blocknum = 0
            
        # if we have a first block then write it
        if firstblock is not None:
            self.write(firstblock)
            
            
    def getGDALDataset(self):
        """
        Returns the underlying GDAL dataset object
        """
        return self.ds
        
    def getCurrentBlock(self):
        """
        Returns the number of the current block
        """
        return self.blocknum
                    
    def setThematic(self):
        """
        Sets the output file to thematic. If file is multi-layer,
        then all bands are set to thematic. 
        """
        for i in range(1, self.ds.RasterCount+1):
            band = self.ds.GetRasterBand(i)
            band.SetMetadataItem('LAYER_TYPE','thematic')
                        
    def setColorTable(self, colortable, band=1):
        """
        Sets the output color table. Pass a list
        of sequences of colors, or a 2d array, as per the
        docstring for rat.setColorTable(). 
        """
        colorTableArray = numpy.array(colortable)
        rat.setColorTable(self.ds, colorTableArray, layernum=band)
        
    def setLayerNames(self,names):
        """
        Sets the output layer names. Pass a list
        of layer names, one for each output band
        """
        bandindex = 1
        for name in names:
            bh = self.ds.GetRasterBand(bandindex)
            bh.SetDescription(name)            
            bandindex += 1
        
    def write(self, block):
        """
        Writes the numpy block to the current location in the file,
        and updates the location pointer for next write
        """
        
        # convert the block to row/column
        yblock = self.blocknum // self.xtotalblocks
        xblock = self.blocknum % self.xtotalblocks
        
        # calculate the coords of this block in pixels
        xcoord = xblock * self.windowxsize
        ycoord = yblock * self.windowysize
        
        self.writeAt(block, xcoord, ycoord)
        
        # so next time we write the next block
        self.blocknum += 1
        
    def writeAt(self, block, xcoord, ycoord):
        """
        writes the numpy block to the specified pixel coords
        in the file
        """
        # check they asked for block is valid
        brxcoord = xcoord + block.shape[-1] - self.overlap*2
        brycoord = ycoord + block.shape[-2] - self.overlap*2
        if brxcoord > self.ds.RasterXSize or brycoord > self.ds.RasterYSize:
            raise rioserrors.OutsideImageBoundsError()
            
        # check they did actually pass a 3d array
        # (all arrays are 3d now - PyModeller had 2 and 3d)
        if block.ndim != 3:
            raise rioserrors.ParameterError("Only 3 dimensional arrays are accepted now")
            
        # write each band
        for band in range(self.ds.RasterCount):
        
            bh = self.ds.GetRasterBand(band + 1)
            slice_bottomMost = block.shape[-2] - self.overlap
            slice_rightMost = block.shape[-1] - self.overlap
            
            # take off overlap if present
            outblock = block[band, self.overlap:slice_bottomMost, self.overlap:slice_rightMost]
                
            bh.WriteArray(outblock, xcoord, ycoord)

    def setAttributeColumn(self, colName, sequence, colType=None, bandNumber=1):
        """
        Puts the sequence into colName in the output file. See rios.rat.writeColumn
        for more information. You probably also want to call setThematic().
        """
        rat.writeColumn(self.ds, colName, sequence, colType, bandNumber)
    
    def reset(self):
        """
        Resets the location pointer so that the next
        write() call writes to the start of the file again
        """
        self.blocknum = 0
    
    def close(self, calcStats=False, statsIgnore=None, progress=None, omitPyramids=False):
        """
        Closes the open dataset
        """
        if calcStats:
            from . import calcstats
            from .cuiprogress import SilentProgress
            if progress is None:
                progress = SilentProgress()
            calcstats.addStatistics(self.ds, progress, statsIgnore)
            if not omitPyramids:
                calcstats.addPyramid(self.ds, progress)
        
        self.ds.FlushCache()
        del self.ds
        self.ds = None
