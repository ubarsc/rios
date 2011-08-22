
"""
Contains the ImageWriter class

"""

import math
from osgeo import gdal
from . import imageio
from . import rioserrors
from . import rat

DEFAULTCREATIONOPTIONS = ['COMPRESSED=TRUE','IGNOREUTM=TRUE']
DEFAULTDRIVERNAME = 'HFA'

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
            # get the number of bands out of the block
            nbands = 1
            if len(firstblock.shape) != 2:
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
        Sets the output file to thematic
        """
        if self.ds.RasterCount != 1:
            msg = 'Only able to set single layer images as Thematic'
            raise rioserrors.ThematicError(msg)
        
        band1 = self.ds.GetRasterBand(1)
        band1.SetMetadataItem('LAYER_TYPE','thematic')
                        
    def setColorTable(self, colourtable, band=1):
        """
        Sets the output colour table. Pass a list
        of sequences of colours, or a 2d array
        """
        bandh = self.ds.GetRasterBand(band)
        
        gdalct = gdal.ColorTable()
        count = 0
        for col in colourtable:
            gdalct.SetColorEntry(count,tuple(col))
            count += 1
        bandh.SetRasterColorTable(gdalct)
        
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
    
    def close(self,calcStats=False,statsIgnore=None,progress=None):
        """
        Closes the open dataset
        """
        if calcStats:
            from . import calcstats
            calcstats.calcStats(self.ds,progress,statsIgnore)
        
        self.ds.FlushCache()
        del self.ds
        self.ds = None
