"""
Apply a function to a whole Raster Attribute Table (RAT), block by block,
so as to avoid using large amounts of memory. Transparently takes care of 
the details of reading and writing columns from the RAT. 

This was written in rough mimicry of the RIOS image applier functionality. 

The most important components are the :func:`rios.ratapplier.apply` function, and 
the :class:`rios.ratapplier.RatApplierControls` class. Pretty much everything else is for internal 
use only. The docstring for the :func:`rios.ratapplier.apply` function gives a simple example
of its use. 

In order to work through the RAT(s) block by block, we rely on having
available routines to read/write only a part of the RAT. This is available
with GDAL 1.11 or later. If this is not available, we fudge the same thing 
by reading/writing whole columns, i.e. the block size is the full length 
of the RAT. This last case is not efficient with memory, but at least 
provides the same functionality. 

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

# Some design notes.
# The use of the __getattr__/__setattr__ for on-the-fly reading of data blocks
# is more efficient for reading only selected columns from a large RAT, however, 
# it means that a lot of state information is carried around and may make it
# much harder if we ever try to multi-thread the calculation loop. So perhaps
# we won't do that. 

from __future__ import division, print_function

import numpy
from osgeo import gdal
try:
    import ratzarr
except ImportError:
    ratzarr = None

from . import rat
from . import rioserrors

# Some constants relating to how we control the length of the
# output RAT (RCM = Row Count Method)
RCM_EQUALS_INPUT = 0
"Same as input"
RCM_FIXED = 1
"Fixed size"
RCM_INCREMENT = 2
"Incremented as required"


def apply(userFunc, inRats, outRats, otherargs=None, controls=None):
    """
    Apply the given function across the whole of the given raster attribute tables.
    The attribute table is processing one chunk at a time allowing very large tables
    without running out of memory.
    
    All raster files must already exist, but new columns can be created. 
    
    Normal pattern is something like the following::
    
        inRats = ratapplier.RatAssociations()
        outRats = ratapplier.RatAssociations()
        
        inRats.vegclass = ratapplier.RatHandle('vegclass.kea')
        outRats.vegclass = ratapplier.RatHandle('vegclass.kea')
        
        ratapplier.apply(myFunc, inRats, outRats)
        
        def myFunc(info, inputs, outputs):
            outputs.vegclass.colSum = inputs.vegclass.col1 + inputs.vegclass.col2

    The :class:`rios.ratapplier.RatHandle` defaults to using the RAT from the first layer of 
    the image which is usual for thematic imagery. 
    This can be overridden using the layernum parameter. The names of the columns are reflected in 
    the names of the fields on the inputs and outputs parameters and multiple input and output RAT's can be specified
    
    The otherargs argument can be any object, and is typically an instance
    of :class:`rios.ratapplier.OtherArguments`. It will be passed in to each call of the user function, 
    unchanged between calls, so that other values can be passed in, and 
    calculated quantities passed back. The values stored on this object are not
    directly associated with rows of the RAT, and must be managed entirely by
    the user. If it is not required, it need not be passed. 
    
    The controls object is an instance of the :class:`rios.ratapplier.RatApplierControls` class, and 
    is only required if the default control settings are to be changed. 
    
    The info object which is passed to the user function is an instance of 
    the :class:`rios.ratapplier.RatApplierState` class. 

    By default new columns are marked as 'Generic'. If they need to be marked as having a specific usage, the following syntax is used::

        def addCols(info, inputs, outputs):
            "Add two columns and output"
            outputs.outimg.colSum = inputs.inimg.col1 + inputs.inimg.col4
            outputs.outImg.Red = someRedValue      # some calculated red value, in 0-255 range
            outputs.outImg.setUsage('Red', gdal.GFU_Red)

    **Statistics**

    Since the RAT is now read one chunk at a time calling numpy functions like mean() 
    etc will only return statistics for the current chunk, not globally. The solution is to use the 
    :class:`rios.fileinfo.RatStats` class::

        from rios.fileinfo import RatStats

        columnsOfInterest = ['col1', 'col4']
        ratStatsObj = RatStats('file.img', columnlist=columnsOfInterest)

        print(ratStatsObj.col1.mean, ratStatsObj.col4.mean)

    Each column attribute is an instance of :class:`rios.fileinfo.ColumnStats` and is intended to be 
    passed through the apply function via the otherargs mechanism.

    **Non-GDAL RATs**

    An alternative form of RAT is supported, based on Zarr arrays. Instead of
    using the RatHandle class to connect to a RAT in a GDAL file, use the
    RatZarrHandle class to associate with a Zarr file. This has a very specific
    internal structure, and is intended for use in writing columns outside
    of the GDAL raster file, if it cannot be written for some reason. The main
    use case is to read and/or write a RAT stored on AWS S3.

    Example::

        inRats.vegclass = ratapplier.RatHandle('vegclass.kea')
        outRats.extra = ratapplier.RatZarrHandle('s3://mybucket/extra.zarr')

    This can then used in the same way as a GDAL-based RAT.
    
    It requires the ratzarr package (https://github.com/ubarsc/ratzarr).
    """
    # Get a default controls object if we have not been given one
    if controls is None:
        controls = RatApplierControls()
    
    # Open all files. 
    allFileHandles = FileHandlesCollection(inRats, outRats)
    allFileHandles.checkConsistency()
    rowCount = controls.rowCount
    if rowCount is None:
        rowCount = allFileHandles.getRowCount()

    # The current state of processing, i.e. where are we up to as 
    # we progress through the table(s)
    state = RatApplierState(rowCount)
    
    inBlocks = BlockCollection(inRats, state, allFileHandles, controls)
    outBlocks = BlockCollection(outRats, state, allFileHandles, controls)
    
    # A list of the names for those RATs which are output
    outputRatHandleNameList = list(outRats.__dict__.keys())

    numBlocks = int(numpy.ceil(float(rowCount) / controls.blockLen))
    
    if controls.progress is not None:
        controls.progress.setTotalSteps(100)
        controls.progress.setProgress(0)
    lastpercent = 0

    # Loop over all blocks in the RAT(s)
    for i in range(numBlocks):
        state.setBlock(i, controls.blockLen)

        # Set up the arguments for the userFunc
        functionArgs = (state, inBlocks, outBlocks)
        if otherargs is not None:
            functionArgs += (otherargs, )
            
        # Call the user function
        userFunc(*functionArgs)
        
        # Now write the output blocks
        outBlocks.writeCache(outputRatHandleNameList, controls, state)
        
        # Clear block caches
        inBlocks.clearCache()
        outBlocks.clearCache()

        if controls.progress is not None:
            percent = int((i * 100) / numBlocks)
            if percent != lastpercent:
                controls.progress.setProgress(percent)
                lastpercent = percent                
    
    outBlocks.finaliseRowCount(outputRatHandleNameList)
    allFileHandles.close()

    if controls.progress is not None:
        controls.progress.setProgress(100)


def copyRAT(input, output, progress=None, omitColumns=None):
    """
    Given an input and output filenames copies the RAT from 
    the input and writes it to the output.

    if omitColumns is set, then it should be a sequence of
    columns names that are to be omitted from the copying.
    For example, the 'Histogram' column may need to be omitted
    so that the pixel counts stay the correct values in the output
    image.
    """
    inRats = RatAssociations()
    outRats = RatAssociations()
        
    inRats.inclass = RatHandle(input)
    outRats.outclass = RatHandle(output)

    controls = RatApplierControls()
    controls.progress = progress

    otherArgs = OtherArguments()
    otherArgs.colNames = rat.getColumnNames(input)
    if omitColumns is not None:
        for colName in omitColumns:
            otherArgs.colNames.remove(colName)
    if len(otherArgs.colNames) > 0:
        apply(internalCopyRAT, inRats, outRats, otherArgs, controls)


def internalCopyRAT(info, inputs, outputs, otherArgs):
    """
    Called from copyRAT. Copies the RAT
    """
    for columnName in otherArgs.colNames:
        data = getattr(inputs.inclass, columnName)
        setattr(outputs.outclass, columnName, data)

        usage = inputs.inclass.getUsage(columnName)
        outputs.outclass.setUsage(columnName, usage)


class RatHandle(object):
    """
    A handle onto the RAT for a single image layer. This is used as an 
    easy way for the user to nominate both a filename and a layer number. 
    """
    def __init__(self, filename, layernum=1):
        """
        This is how the user specifies a RAT stored in a GDAL file.

        filename is a string, layernum is an integer (first layer is 1)
        """
        self.filename = filename
        self.layernum = layernum

    def __hash__(self):
        "Hash a tuple of (filename, layernum)"
        return hash((self.filename, self.layernum))
    

class RatZarrHandle:
    """
    Equivalent of RatHandle, but for a RAT stored in a Zarr file

    New in version 2.0.9
    """
    def __init__(self, filename):
        """
        This is how the user specifies a RAT stored in a Zarr file.

        filename is a string. For local files, should be just a path string,
        for AWS S3 files it should have the form s3://bucket/path

        New in version 2.0.9
        """
        if ratzarr is None:
            msg = "Using RatZarrHandle, but ratzarr module is unavailable"
            raise ValueError(msg)

        self.filename = filename

    def __hash__(self):
        """
        Just hash the filename field
        """
        return hash(self.filename)

    def __eq__(self, other):
        return (self.filename == other.filename)


class RatAssociations(object):
    """
    Class associating external raster attribute tables with internal names. 
    Each attribute defined on this object should be a RatHandle object. 
    
    """
    def getRatList(self):
        """
        Return a list of the names of the RatHandle objects defined on this object
        """
        return self.__dict__.keys()
        

class RatApplierState(object):
    """
    Current state of RAT applier. An instance of this class is passed as the first
    argument to the user function. 
    
    Attributes:
    
        * blockNdx                Index number of current block (first block is zero, second block is 1, ...)
        * startrow                RAT row number of first row of current block (first row is zero)
        * blockLen                Number of rows in current block
        * inputRowNumbers         Row numbers in whole input RAT(s) corresponding to current block
        * rowCount                The total number of rows in the input RAT(s)
        
    """
    def __init__(self, rowCount):
        # The start row number of the current block
        self.startrow = 0
        # The array of row numbers for the current block
        self.inputRowNumbers = None
        # The block length (mostly constant, but different on the last block)
        self.blockLen = None
        # The number of rows in the whole RAT(s). Constant over the block loop. 
        self.rowCount = rowCount
    
    def setBlock(self, i, requestedBlockLen):
        """
        Sets the state to be pointing at the i-th block. i starts at zero. 
        """
        self.blockNdx = i
        self.startrow = i * requestedBlockLen
        endrow = self.startrow + requestedBlockLen - 1
        endrow = min(endrow, self.rowCount - 1)
        self.blockLen = endrow - self.startrow + 1
        self.inputRowNumbers = numpy.arange(self.startrow, self.startrow + self.blockLen)


class RatApplierControls(object):
    """
    Controls object for the ratapplier. An instance of this class can
    be given to the apply() function, to control its behaviour. 
    
    """
    def __init__(self):
        self.blockLen = 100000
        self.rowCount = None
        self.outRowCountMethod = RCM_EQUALS_INPUT
        self.fixedOutRowCount = None
        self.rowCountIncrementSize = None
        self.progress = None
        self.useStringDType = False
    
    def setBlockLength(self, blockLen):
        "Change the number of rows used per block"
        self.blockLen = blockLen
    
    def setRowCount(self, rowCount):
        """
        Set the total number of rows to be processed. This is normally only useful
        when doing something like writing an output RAT without any input RAT,
        so the number of rows is otherwise undefined. 
        """
        self.rowCount = rowCount
    
    def outputRowCountHandling(self, method=RCM_EQUALS_INPUT, totalsize=None, incrementsize=None):
        """
        Determine how the row count of the output RAT(s) is handled. The method
        argument can be one of the following constants:

            * RCM_EQUALS_INPUT        Output RAT(s) have same number of rows as input RAT(s)
            * RCM_FIXED               Output row count is set to a fixed size
            * RCM_INCREMENT           Output row count is incremented as required
            
        
        The totalsize and incrementsize arguments, if given, should be int.
        
        totalsize is used to set the output row count when the method is RCM_FIXED. 
        It is required, if the method is RCM_FIXED. 
        
        incrementsize is used to determine how much the row count is
        incremented by, if the method is RCM_INCREMENT. If not given,
        it defaults to the length of the block being written. 
        
        The most common case if the default (i.e. RCM_EQUALS_INPUT). If the
        output RAT row count will be different from the input, and the count can 
        be known in advance, then you should use RCM_FIXED to set that size. Only 
        if the output RAT row count cannot be determined in advance should 
        you use RCM_INCREMENT. 
        
        For some raster formats, using RCM_INCREMENT will result in wasted
        space, depending on the incrementsize used. Caution is recommended. 
        
        """
        self.outRowCountMethod = method
        self.fixedOutRowCount = totalsize
        self.rowCountIncrementSize = incrementsize

    def setProgress(self, progress):
        """
        Set the progress display object. Default is no progress
        object. 
        """
        self.progress = progress

    def setUseStringDType(self, useStringDType):
        """
        Set whether to use the numpy-2.x StringDType when reading GFT_String
        columns. If this is True, then when data is read from a GFT_String
        column, it will be converted to StringDType (i.e. an array of
        variable-length strings) before presenting it to the user.

        The default is the old behaviour, i.e. the returned string arrays
        are fixed-width bytes string arrays.

        If StringDType is unavailable (numpy < 2.0), this flag is
        always False.
        """
        if hasattr(numpy.dtypes, 'StringDType'):
            self.useStringDType = useStringDType


class OtherArguments(object):
    """
    Simple empty class which can be used to pass arbitrary arguments in and 
    out of the apply() function, to the user function. Anything stored on
    this object persists between iterations over blocks. 
    
    """
    pass
    

class BlockCollection(object):
    """
    Hold a set of RatBlockAssociation objects, for all currently open RATs
    """
    def __init__(self, ratAssoc, state, allFileHandles, controls):
        """
        Create a RatBlockAssociation entry for every RatHandle in ratAssoc
        """
        for ratHandleName in ratAssoc.getRatList():
            ratHandle = getattr(ratAssoc, ratHandleName)
            fileHandles = allFileHandles.fileHandlesDict[ratHandle]
            ratBlockAssoc = RatBlockAssociation(state, fileHandles, controls)
            setattr(self, ratHandleName, ratBlockAssoc)
        
    def clearCache(self):
        """
        Clear all caches
        """
        for ratHandleName in self.__dict__:
            ratBlockAssoc = getattr(self, ratHandleName)
            ratBlockAssoc.clearCache()
            
    def writeCache(self, outputRatHandleNameList, controls, state):
        """
        Write all cached data blocks
        """
        for ratHandleName in outputRatHandleNameList:
            ratBlockAssoc = getattr(self, ratHandleName)
            ratBlockAssoc.writeCache(controls, state)
    
    def finaliseRowCount(self, outputRatHandleNameList):
        """
        Called after the block loop completes, to reset the row count of
        each output RAT, in case it had been over-allocated. 
        
        In some raster formats, this will not reclaim space, but we still would
        like the row count to be correct. 
        
        """
        for ratHandleName in outputRatHandleNameList:
            ratBlockAssoc = getattr(self, ratHandleName)
            ratBlockAssoc.finaliseRowCount()


class RatBlockAssociation(object):
    """
    Hold one or more blocks of data from RAT columns of a single RAT. This
    class is kind of at the heart of the module. 
    
    Most generic attributes on this class are blocks of data read from and
    written to the RAT, and so are not actually attributes at all, but are 
    managed by the __setattr__/__getattr__ over-ride methods. Their names are
    the names of the columns to which they correspond. However, there are a 
    number of genuine attributes which also need to be present, for internal 
    use, and it is obviously important that their names not be the same as 
    any columns. Since we obviously cannot guarantee this, we have named them 
    beginning with "Z\\_\\_", in the hope that no-one ever has a column with
    a name like this. These are all created within the __init__ method. 
    
    The main purpose of using __getattr__ is to avoid reading columns which 
    the userFunc is not actually using. As a consequence, one also needs to
    use __setattr__ to handle the data the same way. 
    
    """
    def __init__(self, state, fileHandles, controls):
        """
        Pass in the RatApplierState object, so we can always see where we 
        are up to, and the associated FileHandles object, so we can get to 
        the file.
        
        Note the use of object.__setattr__() to create the normal attributes
        on the object, so they do not behave as RAT column blocks. 
        
        """
        object.__setattr__(self, 'Z__state', state)
        object.__setattr__(self, 'Z__cache', {})
        object.__setattr__(self, 'Z__fileHandles', fileHandles)
        object.__setattr__(self, 'Z__controls', controls)
        object.__setattr__(self, 'Z__outputRowCount', 0)
            
        # Column usage in a form which the user function can change.
        if fileHandles.gdalRat is not None:
            object.__setattr__(self, 'Z__columnUsage', {})
            for name in self.Z__fileHandles.columnNdxByName:
                ndx = self.Z__fileHandles.columnNdxByName[name]
                self.Z__columnUsage[name] = self.Z__fileHandles.gdalRat.GetUsageOfCol(ndx)
        
        # The attributes which we should consider to be column names
        object.__setattr__(self, 'Z__columnNameSet', set())
    
    def setUsage(self, columnName, usage):
        """
        Set the usage of the given column. 
        """
        if self.Z__fileHandles.gdalRat is not None:
            self.Z__columnUsage[columnName] = usage
    
    def getUsage(self, columnName):
        """
        Return the usage of the given column
        """
        usage = gdal.GFU_Generic
        if self.Z__fileHandles.gdalRat is not None:
            if columnName in self.Z__columnUsage:
                usage = self.Z__columnUsage[columnName]
        return usage
        
    def __getattr__(self, columnName):
        """
        Read the column data on the fly. Caches it in self.__cache, keyed by
        (columnName, state.startrow). Returns a numpy array of the requested
        block of data. 
        
        """
        key = self.__makeKey(columnName)

        if key not in self.Z__cache:
            ratObj = self.Z__fileHandles.getRatObj()
            if isinstance(ratObj, gdal.RasterAttributeTable):
                colNdx = self.Z__fileHandles.columnNdxByName[columnName]
                colType = ratObj.GetTypeOfCol(colNdx)
                dataBlock = ratObj.ReadAsArray(colNdx, start=self.Z__state.startrow, 
                        length=self.Z__state.blockLen)
                if self.Z__controls.useStringDType and (colType == gdal.GFT_String):
                    dataBlock = dataBlock.astype(numpy.dtypes.StringDType)
            elif isinstance(ratObj, ratzarr.RatZarr):
                dataBlock = ratObj.readBlock(columnName, self.Z__state.startrow, 
                                 self.Z__state.blockLen)
            self.Z__cache[key] = dataBlock
        value = self.Z__cache[key]
        return value
    
    def __setattr__(self, attrName, attrValue):
        """
        Stash the given data block into the cache, to be written out after the
        user's function has returned. 
        
        """
        key = self.__makeKey(attrName)
        self.Z__cache[key] = attrValue
        self.Z__columnNameSet.add(attrName)

    def __makeKey(self, columnName):
        """
        Key includes the startrow so we cannot accidentally use data from one block as 
        though it were from another. 
        """
        return (columnName, self.Z__state.startrow)
    
    def clearCache(self):
        """
        Clear the current cache of data blocks
        """
        object.__setattr__(self, 'Z__cache', {})
    
    def writeCache(self, controls, state):
        """
        Write all cached data blocks. Creates the columns if they do not already exist. 
        """
        rowsToWrite = None
        # Loop over all columns names which have been set on this object
        for columnName in self.Z__columnNameSet:
            fileHandles = self.Z__fileHandles
            ratObj = fileHandles.getRatObj()
            key = self.__makeKey(columnName)
            dataBlock = self.Z__cache[key]
            if rowsToWrite is None:
                rowsToWrite = len(dataBlock)
            # Check that all the dataBlocks being written to this RAT
            # have the same number of rows
            if len(dataBlock) != rowsToWrite:
                msg = "Data block for column '%s' has inconsistent length: %d!=%d" % (columnName, 
                    len(dataBlock), rowsToWrite)
                raise rioserrors.RatBlockLengthError(msg)

            isGdalRat = isinstance(ratObj, gdal.RasterAttributeTable)
            isZarrRat = (ratzarr is not None and
                         isinstance(ratObj, ratzarr.RatZarr))

            # Check if the column needs to be created
            if (isGdalRat and
                    columnName not in fileHandles.columnNdxByName):
                columnType = rat.inferColumnType(dataBlock)
                if columnType is None:
                    msg = "Can't infer GFT type from {} for column '{}'".format(
                        type(dataBlock[0]), columnName)
                    raise rioserrors.AttributeTableTypeError(msg)
                columnUsage = self.getUsage(columnName)
                ratObj.CreateColumn(columnName, columnType, columnUsage)
                # Work out the new column index
                columnNdx = ratObj.GetColumnCount() - 1
                fileHandles.columnNdxByName[columnName] = columnNdx
            elif isZarrRat and not ratObj.colExists(columnName):
                self.checkZarrfileParams()
                ratObj.createColumn(columnName, dataBlock.dtype)

            # Write the block of data into the RAT column
            if len(dataBlock) > 0:
                if (fileHandles.getRowCount() <
                        (self.Z__outputRowCount + rowsToWrite)):
                    newOutputRowCount = self.guessNewRowCount(rowsToWrite, controls, state)
                    fileHandles.setRowCount(newOutputRowCount)

                # If they have given a StringDType, convert to bytes for GDAL
                if (hasattr(numpy.dtypes, 'StringDType') and
                        isinstance(dataBlock.dtype, numpy.dtypes.StringDType)):
                    maxLen = numpy.strings.str_len(dataBlock).max()
                    dt = "|S{}".format(maxLen)
                    dataBlock = dataBlock.astype(dt)

                if isGdalRat:
                    columnNdx = self.Z__fileHandles.columnNdxByName[columnName]
                    ratObj.WriteArray(dataBlock, columnNdx,
                                      self.Z__outputRowCount)
                elif isZarrRat:
                    ratObj.writeBlock(columnName, dataBlock,
                                      self.Z__outputRowCount)
            # There may be a problem with HFA Byte arrays, if we don't end up writing 256 rows....
        
        # Increment Z__outputRowCount, without triggering __setattr__.
        if rowsToWrite is not None:
            object.__setattr__(self, 'Z__outputRowCount',
                self.Z__outputRowCount + rowsToWrite)
    
    def guessNewRowCount(self, rowsToWrite, controls, state):
        """
        When we are writing to a new RAT, and we find that we need to write more
        rows than it currently has, we guess what we should set the row count to
        be, depending on how the controls have told us to do this. 
        """
        if controls.outRowCountMethod == RCM_EQUALS_INPUT:
            newRowCount = state.rowCount
        elif controls.outRowCountMethod == RCM_FIXED:
            newRowCount = controls.fixedOutRowCount
        elif controls.outRowCountMethod == RCM_INCREMENT:
            if controls.rowCountIncrementSize is None:
                increment = rowsToWrite
            else:
                increment = max(rowsToWrite, controls.rowCountIncrementSize)
            newRowCount = self.Z__outputRowCount + increment
        return newRowCount
    
    def finaliseRowCount(self):
        """
        If the row count for this RAT has been over-allocated, reset it back
        to the actual number of rows we wrote. 
        """
        fileHandles = self.Z__fileHandles
        trueRowCount = self.Z__outputRowCount
        rowCount = fileHandles.getRowCount()
        if rowCount != trueRowCount:
            if fileHandles.gdalRat is not None:
                fileHandles.gdalRat.SetRowCount(trueRowCount)
            elif fileHandles.rz is not None:
                fileHandles.rz.setRowCount(trueRowCount)

    def checkZarrfileParams(self):
        """
        Check if the Zarr file has only just been created. If so,
        initialize it with suitable parameters
        """
        rz = self.Z__fileHandles.rz
        if not isinstance(rz, ratzarr.RatZarr):
            # We are not a RatZarr. Should this be an error?
            return

        state = self.Z__state
        controls = self.Z__controls
        colNames = rz.getColumnNames()
        if len(colNames) == 0:
            rowCount = state.rowCount
            rz.setRowCount(rowCount)
            blockLen = controls.blockLen

            # Choose a suitable chunk size. We want something <= maxChunkSize,
            # because it appears that much larger than this causes memory
            # blowouts in zarr.
            maxChunkSize = 999999
            minChunkSize = 100000
            if blockLen < minChunkSize:
                chunkSize = blockLen
            else:
                j = int(numpy.ceil(blockLen / maxChunkSize))
                while (blockLen % j) != 0 and (blockLen / j) > minChunkSize:
                    j += 1
                if blockLen % j != 0:
                    msg = ("Cannot find Zarr chunk size as factor of " +
                           f"block length {blockLen}")
                    raise rioserrors.RatBlockLengthError(msg)
                chunkSize = int(blockLen / j)
            rz.setChunkSize(chunkSize)


class FileHandles(object):
    """
    Hang onto all the required file-related objects relating to a given
    opened RAT. For a GDAL RAT, these are the GDAL objects, for a
    Zarr-based RAT, just the RatZarr object. The unused objects are None.

    Attributes are:

        * **ds**                  The gdal.Dataset object
        * **band**                The gdal.Band object
        * **gdalRat**             The gdal.RasterAttributeTable object
        * **columnNdxByName**     A lookup table to get column index from column name
        * **rz**                  The RatZarr object
        
    """
    def __init__(self, ratHandle, update=False, sharedDS=None):
        """
        If update is True, the GDAL dataset is opened with gdal.GA_Update.
        If sharedDS is not None, this is used as the GDAL dataset, rather
        than opening a new one. 
        """
        self.ds = None
        self.band = None
        self.gdalRat = None
        self.rz = None
        self.columnNdxByName = None

        if isinstance(ratHandle, RatHandle):
            if sharedDS is None:
                if update:
                    self.ds = gdal.Open(ratHandle.filename, gdal.GA_Update)
                else:
                    self.ds = gdal.Open(ratHandle.filename)
            else:
                self.ds = sharedDS

            self.band = self.ds.GetRasterBand(ratHandle.layernum)
            self.gdalRat = self.band.GetDefaultRAT()

            # A lookup table so we can get column index from the name.
            # GDAL does not currently provide this, although my feeling is
            # perhaps it should. 
            self.columnNdxByName = {}
            for i in range(self.gdalRat.GetColumnCount()):
                name = self.gdalRat.GetNameOfCol(i)
                self.columnNdxByName[name] = i
        elif isinstance(ratHandle, RatZarrHandle):
            readOnly = not update
            create = not readOnly
            self.rz = ratzarr.RatZarr(ratHandle.filename, readOnly=readOnly,
                                      create=create)

    def getRatObj(self):
        """
        Return the RAT object. For Zarr, this is just the RatZarr object,
        while for GDAL it is the RasterAttributeTable object
        """
        ratObj = self.gdalRat
        if ratObj is None:
            ratObj = self.rz
        return ratObj

    def getRowCount(self):
        """
        Return the current row count of the RAT object
        """
        if self.gdalRat is not None:
            rowCount = self.gdalRat.GetRowCount()
        elif self.rz is not None:
            rowCount = self.rz.getRowCount()
        return rowCount

    def setRowCount(self, rowCount):
        """
        Set the row count on the appropriate RAT object
        """
        if self.gdalRat is not None:
            self.gdalRat.SetRowCount(rowCount)
        elif self.rz is not None:
            self.rz.setRowCount(rowCount)

    def close(self):
        """
        Close any open file handles
        """
        if self.ds is not None:
            self.ds.FlushCache()
            self.ds = None
        if self.rz is not None:
            self.rz = None


class FileHandlesCollection(object):
    """
    A set of all the FileHandles objects
    """
    def __init__(self, inRats, outRats):
        """
        Open all the raster files, storing a dictionary of FileHandles objects
        as self.fileHandlesDict. This is keyed by RatHandle objects. 
        
        Output files are opened first, with update=True. Any input files which
        are not open are then opened with update=False.

        Extra effort is made to cope with the very unlikely case of opening
        RATs on separate layers in the same image file, mostly because if
        it did happen, it would go horribly wrong. Such cases are able to share 
        the same gdal.Dataset object. 
        
        """
        self.fileHandlesDict = {}
        self.inputRatList = []
        
        # Do the output files first, so they get opened with update=True
        for ratHandleName in outRats.getRatList():
            ratHandle = getattr(outRats, ratHandleName)
            if ratHandle not in self.fileHandlesDict:
                sharedDS = self.checkExistingDS(ratHandle)
                self.fileHandlesDict[ratHandle] = FileHandles(ratHandle, update=True, sharedDS=sharedDS)
        for ratHandleName in inRats.getRatList():
            ratHandle = getattr(inRats, ratHandleName)
            if ratHandle not in self.fileHandlesDict:
                sharedDS = self.checkExistingDS(ratHandle)
                self.fileHandlesDict[ratHandle] = FileHandles(ratHandle, update=False, sharedDS=sharedDS)
            # A list of those handles which are for input, and are thus expected to already
            # have rows in them. 
            self.inputRatList.append(ratHandle)
    
    def getRowCount(self):
        """
        Return the number of rows in the RATs of all files. Actually
        just returns the row count of the first input RAT, assuming 
        that they are all the same (see self.checkConsistency())
        """
        if len(self.inputRatList) > 0:
            firstRatHandle = self.inputRatList[0]
            fileHandles = self.fileHandlesDict[firstRatHandle]
            rowCount = fileHandles.getRowCount()
        else:
            rowCount = None
        return rowCount
        
    def checkConsistency(self):
        """
        Check the consistency of the set of input RATs opened on the current instance.
        It is kind of assumed that the output rats will become consistent, although
        this is by no means guaranteed. 
        """
        rowCountList = []
        for ratHandle in self.inputRatList:
            fileHandles = self.fileHandlesDict[ratHandle]
            rowCount = fileHandles.getRowCount()
            filename = ratHandle.filename
            rowCountList.append((filename, rowCount))
        
        countList = [c for (f, c) in rowCountList]
        allSame = all([(c == countList[0]) for c in countList])
        if not allSame:
            msg = "RAT length mismatch\n%s\n" % '\n'.join(
                ["File: %s, rowcount:%s"%(fn, rc) for (fn, rc) in rowCountList])
            raise rioserrors.RatMismatchError(msg)

    def checkExistingDS(self, ratHandle):
        """
        Checks the current set of filenames in use, and if it finds one with the same
        filename as the given ratHandle, assumes that it is already open, but with a 
        different layer number. If so, return the gdal.Dataset associated with it,
        so it can be shared. If not found, return None. 
        
        """
        sharedDS = None
        for existingRatHandle in self.fileHandlesDict:
            if existingRatHandle.filename == ratHandle.filename:
                fileHandles = self.fileHandlesDict[existingRatHandle]
                sharedDS = fileHandles.ds
                if sharedDS is None:
                    sharedDS = fileHandles.rz
        return sharedDS

    def close(self):
        """
        Close all file handles
        """
        for (k, fh) in self.fileHandlesDict.items():
            fh.close()
