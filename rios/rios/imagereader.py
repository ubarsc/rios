
"""
Contains the ImageReader class

"""

import sys
import copy
import numpy
from osgeo import gdal
from . import imageio
from . import inputcollection
from . import readerinfo
from . import rioserrors

if sys.version_info[0] > 2:
    # hack for Python 3 which uses str instead of basestring
    # we just use basestring
    basestring = str

DEFAULTFOOTPRINT = imageio.INTERSECTION
DEFAULTWINDOWXSIZE = 200
DEFAULTWINDOWYSIZE = 200
DEFAULTOVERLAP = 0
DEFAULTLOGGINGSTREAM = sys.stdout

class ImageIterator(object):
    """
    Class to allow iteration across an ImageReader instance.
    Do not instantiate this class directly - it is created
    by ImageReader.__iter__().
    
    See http://docs.python.org/library/stdtypes.html#typeiter
    for a description of how this works. There is another way,
    see: http://docs.python.org/reference/expressions.html#yieldexpr
    but it seemed too much like Windows 3.1 programming which
    scared me!
    
    Returns a tuple containing an ReaderInfo class, plus a numpy
    array for each iteration
    
    """
    def __init__(self,reader):
        # reader = an ImageReader instance
        self.reader = reader
        self.nblock = 0 # start at first block
        
    def __iter__(self):
        # For iteration support - just return self.
        return self

    def next(self):
        # for Python 2.x
        return self.__next__()
        
    def __next__(self):
        # for iteration support. Raises a StopIteration
        # if we have read beyond the end of the image
        try:
            # get ImageReader.readBlock() to do the work
            # this raises a OutsideImageBounds exception,
            # but the iteration protocol expects a 
            # StopIteration exception.
            returnTuple = self.reader.readBlock(self.nblock)
        except rioserrors.OutsideImageBoundsError:
            raise StopIteration()
            
        # look at the next block next time
        self.nblock += 1
        
        return returnTuple

class ImageReader(object):
    """
    Class that reads a list of files and 
    iterates through them block by block
    """
    def __init__(self, imageContainer,
				footprint=DEFAULTFOOTPRINT,
				windowxsize=DEFAULTWINDOWXSIZE, windowysize=DEFAULTWINDOWYSIZE,
				overlap=DEFAULTOVERLAP, statscache=None,
                loggingstream=sys.stdout):
        """
        imageContainer is a list or dictionary that contains
        the filenames of the images to be read.
        If a list is passed, a list of blocks is returned at 
        each iteration, if a dictionary a dictionary is
        returned at each iteration with the same keys.
        
        footprint can be either INTERSECTION or UNION
        
        windowxsize and windowysize specify the size
        of the block to be read at each iteration
        
        overlap specifies the number of pixels to overlap
        between each block
        
        statscache if specified, should be an instance of 
        readerinfo.StatisticsCache. If None, cache is
        created per instance of this class. If doing
        multiple reads on same datasets, consider having 
        a single instance of statscache between all instances
        of this class.
        
        Set loggingstream to a file like object if you wish
        logging about resampling to be sent somewhere else
        rather than stdout.
        """

        # grab the imageContainer so we can always know what 
        # type of container they passed in
        self.imageContainer = imageContainer
      
        if isinstance(imageContainer,dict):
            # Convert the given imageContainer into a list suitable for
            # the standard InputCollection. 
            imageList = []
            for name in imageContainer.keys():
                filename = imageContainer[name]
                if isinstance(filename, list):
                    # We have actually been given a list of filenames, so tack then all on to the imageList
                    imageList.extend(filename)
                elif isinstance(filename, basestring):
                    # We just have a single filename
                    imageList.append(filename)
                else:
                    msg = "Dictionary must contain either lists or strings. Got '%s' instead" % type(filename)
                    raise rioserrors.ParameterError(msg)

        
        elif isinstance(imageContainer,basestring):
            # they passed a string, just make a list out of it
            imageList = [imageContainer]

        else:
            # we hope they passed a tuple or list. Don't need to do much
            imageList = imageContainer
        
        # create an InputCollection with our inputs
        self.inputs = inputcollection.InputCollection(imageList,loggingstream=loggingstream)
        
        # save the other vars
        self.footprint = footprint
        self.windowxsize = windowxsize
        self.windowysize = windowysize
        self.overlap = overlap
        self.statscache = statscache
        self.loggingstream = loggingstream
        
        # these are None until prepare() is called
        self.workingGrid = None
        self.info = None

    def __len__(self):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types
        
        # need self.info to be created so run prepare()
        if self.info is None:
            self.prepare()

        # get the total number of blocks for image            
        (xtotalblocks,ytotalblocks) = self.info.getTotalBlocks()
        
        # return the total number of blocks as our len()
        return xtotalblocks * ytotalblocks
        
    def __getitem__(self,key):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types
        # for indexing, returns tuple from readBlock()

        # need self.info to be created so run prepare()
        if self.info is None:
            self.prepare()

        # if they have passed a negative block - count
        # back from the end           
        if key < 0:
            # get total number of blocks
            (xtotalblocks,ytotalblocks) = self.info.getTotalBlocks()
            # add the key (remember, its negative)
            key = (xtotalblocks * ytotalblocks) + key
            if key < 0:
                # still negative - not enough blocks
                raise KeyError()
        
        try:
            # get readBlock() to do the work
            # this raises a OutsideImageBounds exception,
            # but the container protocol expects a 
            # KeyError exception.
            returnTuple = self.readBlock(key)
        except rioserrors.OutsideImageBoundsError:
            raise KeyError()
            
        return returnTuple
            

    def __iter__(self):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types

        # need self.info to be created so run prepare()
        if self.info is None:
            self.prepare()

        # return in ImageIterator instance
        # with a reference to this object            
        return ImageIterator(self)

    def allowResample(self, resamplemethod="near", refpath=None, refgeotrans=None, 
            refproj=None, refNCols=None, refNRows=None):
        """
        By default, resampling is disabled (all datasets must
        match). Calling this enables it. 
        Either refgeotrans, refproj, refNCols and refNRows must be passed, 
        or refpath passed and the info read from that file.
        
        If resampling is needed it will happen before the call returns.
        
        """
        # set the reference in our InputCollection
        self.inputs.setReference(refpath, refgeotrans, refproj,
                refNCols, refNRows)
             
        try:   
            # resample all in collection to reference
            self.inputs.resampleAllToReference(self.footprint, resamplemethod)
        finally:
            # if the user interrupted, then ensure all temp
            # files removed.
            self.inputs.cleanup()
        
    def prepare(self):
        """
        Prepare to read from images. These steps are not
        done in the constructor, but are done just before
        reading in case allowResample() is called which
        will resample the inputs.
        
        """
    
        # if resampled has happened then they should all match
        if not self.inputs.checkAllMatch():
            msg = 'Inputs do not match - must enable resampling'
            raise rioserrors.ResampleNeededError(msg)
        
        # set the working grid based on the footprint
        self.workingGrid = self.inputs.findWorkingRegion(self.footprint)
        
        # create a statscache if not passed to constructor.
        # Created once per dataset so stats
        # only have to be calculated once per image - it
        # returns cached value for subsequent calls.
        if self.statscache is None:
            self.statscache = readerinfo.StatisticsCache()
        
        # create a ReaderInfo class with the info it needs
        # a copy of this class is passed with each iteration
        self.info = readerinfo.ReaderInfo(self.workingGrid, self.statscache, 
                        self.windowxsize, self.windowysize, self.overlap, self.loggingstream)
        
    def readBlock(self,nblock):
        """
        Read a block. This is normally called from the
        __getitem__ method when this class is indexed, 
        or from the ImageIterator when this class is 
        being iterated through.
        
        A block is read from each image and returned
        in a tuple along with a ReaderInfo instance.
        
        nblock is a single index, and will be converted
        to row/column.
        
        """
        
        # need self.info to be created so run prepare()
        if self.info is None:
            self.prepare()
           
        # do a shallow copy of the ReaderInfo.
        # this copy will have the fields filled in
        # that relate to the whole image.
        # We will then fill in the fields that relate
        # to this block. 
        # This means that calls to read other blocks
        # wont clobber the per block info, and user 
        # writing back into the object wont stuff up
        # the system 
        # because it is a shallow copy, statscache should
        # still be pointing to a single object
        info = copy.copy(self.info)
        
        # get the size of the are we are to read
        (xsize,ysize) = info.getTotalSize()
        
        # get the number of blocks are to read
        (xtotalblocks,ytotalblocks) = info.getTotalBlocks()
        
        # check they asked for block is valid
        if nblock >= (xtotalblocks * ytotalblocks):
            raise rioserrors.OutsideImageBoundsError()
        
        # convert the block to row/column
        yblock = nblock // xtotalblocks
        xblock = nblock % xtotalblocks
        
        # set this back to our copy of the info object
        info.setBlockCount(xblock,yblock)
    
        # calculate the coords of this block in pixels
        xcoord = xblock * self.windowxsize
        ycoord = yblock * self.windowysize
        
        # convert this to world coords
        blocktl = imageio.pix2wld( info.transform, xcoord, ycoord )

        # work out the bottom right coord for this block
        nBlockBottomX = (( xblock + 1 ) * self.windowxsize)
        nBlockBottomY = (( yblock + 1 ) * self.windowysize)
        
        # make adjuctment if we are at the edge of the image
        # and there are smaller blocks
        if nBlockBottomX > xsize:
          nBlockBottomX = xsize
        if nBlockBottomY > ysize:
          nBlockBottomY = ysize

        # work out the world coords for the bottom right
        blockbr = imageio.pix2wld( info.transform, nBlockBottomX, nBlockBottomY )
        
        # set this back to our copy of the info object
        info.setBlockBounds(blocktl,blockbr)

        # work out number of pixels of this block
        blockwidth = nBlockBottomX - xcoord
        blockheight = nBlockBottomY - ycoord
        
        # set this back to our copy of the info object
        info.setBlockSize(blockwidth,blockheight)
        
        # start creating our tuple. Start with an empty list
        # and append the blocks.
        blockList = []
        
        try:
        
            # read all the files using our iterable InputCollection
            for (image,ds,pixgrid,nullValList,datatype) in self.inputs:
            
                # get the pixel coords for this block for this file
                tl = imageio.wld2pix(pixgrid.makeGeoTransform(),blocktl.x,blocktl.y)
            
                # just read in the dataset (will return how many layers it has)
                # will just use the datatype of the image
                block = self.readBlockWithMargin(ds,int(round(tl.x)),int(round(tl.y)),blockwidth,blockheight,
                             datatype, self.overlap, nullValList)

                # add this block to our list
                blockList.append(block)
            
                # set the relationship between numpy array
                # and dataset in case the user needs the dataset object
                # and/or the original filename
                info.setBlockDataset(block, ds, image)
                
        finally:
            # if there is any exception thrown here, make
            # sure temporary resampled files are deleted.
            # doesn't seem the destructor is called in this case.
            self.inputs.cleanup()
        
        
        if isinstance(self.imageContainer,dict):
            # we need to use the original keys passed in
            # to the constructor and return a dictionary
            blockDict = {}
            i = 0
            for name in self.imageContainer.keys():
                filename = self.imageContainer[name]
                if isinstance(filename, list):
                    listLen = len(filename)
                    blockDict[name] = []
                    for j in range(listLen):
                        blockDict[name].append(blockList[i])
                        i += 1
                elif isinstance(filename, basestring):
                    blockDict[name] = blockList[i]
                    i += 1
                                    
            # blockContainer is a dictionary
            blockContainer = blockDict
         
        elif isinstance(self.imageContainer,basestring):
            # blockContainer is just a single block
            blockContainer = blockList[0]

        else:   
            # blockContainer is a tuple
            blockContainer = tuple(blockList)
            
        # return a tuple with the info object and
        # our blockContainer
        return (info, blockContainer)
        
        
    @staticmethod
    def readBlockWithMargin(ds, xoff, yoff, xsize, ysize, datatype, margin=0, nullValList=None):
        """
        A 'drop-in' look-alike for the ReadAsArray function in GDAL,
        but with the option of specifying a margin width, such that
        the block actually read and returned will be larger by that many pixels. 
        The returned array will ALWAYS contain these extra rows/cols, and 
        if they do not exist in the file (e.g. because the margin would push off 
        the edge of the file) then they will be filled with the given nullVal. 
        Otherwise they will be read from the file along with the rest of the block. 
        
        Variables within this function which have _margin as suffix are intended to 
        designate variables which include the margin, as opposed to those without. 
        
        This routine will cope with any specified region, even if it is entirely outside
        the given raster. The returned block would, in that case, be filled
        entirely with the null value. 
        
        """
        
        # Create the final array, with margin, but filled with the null value. 
        nLayers = ds.RasterCount
        xSize_margin = xsize + 2 * margin
        ySize_margin = ysize + 2 * margin
        outBlockShape = (nLayers, ySize_margin, xSize_margin)
        
        # Create the empty output array, filled with the appropriate null value. 
        block_margin = numpy.zeros(outBlockShape, dtype=datatype)
        if nullValList is not None and len(nullValList) > 0:
            # We really need something as a fill value, so if any of the 
            # null values in the list is None, then replace it with 0. 
            fillValList = [nullVal for nullVal in nullValList]
            for i in range(len(fillValList)):
                if fillValList[i] is None:
                    fillValList[i] = 0
            # Now use the appropriate null value for each layer as the 
            # initial value in the output array for the block. 
            if len(outBlockShape) == 2:
                block_margin.fill(fillValList[0])
            else:
                for (i, fillVal) in enumerate(fillValList):
                    block_margin[i].fill(fillVal)
        
        
        # Calculate the bounds of the block which we will actually read from the file,
        # based on what we have been asked for, what margin size, and how close we
        # are to the edge of the file. 
        
        # The bounds of the whole image in the file
        imgLeftBound = 0
        imgTopBound = 0
        imgRightBound = ds.RasterXSize
        imgBottomBound = ds.RasterYSize
        
        # The region we will, in principle, read from the file. Note that xSize_margin 
        # and ySize_margin are already calculated above
        xoff_margin = xoff - margin
        yoff_margin = yoff - margin
        
        # Restrict this to what is available
        xoff_margin_file = max(xoff_margin, imgLeftBound)
        xoff_margin_file = min(xoff_margin_file, imgRightBound-1)
        xSize_margin_file = min(xSize_margin, imgRightBound - xoff_margin_file)
        if xoff_margin < imgLeftBound:
            xSize_margin_file = xSize_margin_file - (imgLeftBound - xoff_margin)
        if xoff_margin >= imgRightBound:
            xSize_margin_file = 0
        yoff_margin_file = max(yoff_margin, imgTopBound)
        yoff_margin_file = min(yoff_margin_file, imgBottomBound-1)
        ySize_margin_file = min(ySize_margin, imgBottomBound - yoff_margin_file)
        if yoff_margin < imgTopBound:
            ySize_margin_file = ySize_margin_file - (imgTopBound - yoff_margin)
        if yoff_margin >= imgBottomBound:
            ySize_margin_file = 0
        
        # How many pixels on each edge of the block we end up NOT reading from 
        # the file, and thus have to leave as null in the array
        notRead_left = xoff_margin_file - xoff_margin
        notRead_right = xSize_margin - (notRead_left + xSize_margin_file)
        notRead_top = yoff_margin_file - yoff_margin
        notRead_bottom = ySize_margin - (notRead_top + ySize_margin_file)
        
        # The upper bounds on the slices specified to receive the data
        slice_right = xSize_margin - notRead_right
        slice_bottom = ySize_margin - notRead_bottom
        
        if xSize_margin_file > 0 and ySize_margin_file > 0:
            # Now read in the part of the array which we can actually read from the file.
            block_margin[..., notRead_top:slice_bottom, notRead_left:slice_right] = (
                ds.ReadAsArray(xoff_margin_file, yoff_margin_file, xSize_margin_file, ySize_margin_file))

        return block_margin
        
    def close(self):
        """
        Closes all open datasets
        """
        self.inputs.close()
