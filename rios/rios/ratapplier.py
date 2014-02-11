"""
Apply a function to a whole Raster Attribute Table (RAT), block by block,
so as to avoid using large amounts of memory. Transparently takes care of 
the details of reading and writing columns from the RAT. 

This was written in rough mimicry of the RIOS image applier functionality. 

The most important components are the apply() function, and 
the RatApplierControls class. Pretty much everything else is for internal 
use only. The docstring for the apply() function gives a simple example
of its use. 

In order to work through the RAT(s) block by block, we rely on having
available routines to read/write only a part of the RAT. This is available
with GDAL 2.0 or later. If an earlier version of GDAL is in use, we can
make use of the turbogdal add-on, which provides equivalent functionality. If
neither of these is available, we fudge the same thing by reading/writing
whole columns, i.e. the block size is the full length of the RAT. This
last case is not efficient with memory, but at least provides the same 
functionality. 

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

from . import rat
from . import rioserrors

# Test whether we have access to the GDAL RFC40 facilities
haveRFC40 = hasattr(gdal.RasterAttributeTable, 'ReadAsArray')


def apply(userFunc, inRats, outRats, otherargs=None, controls=None):
    """
    Apply the given function across the whole of the given raster attribute tables.
    
    All raster files must already exist, but new columns can be created. 
    
    Normal pattern is something like the following:
    
        inRats = ratapplier.RatAssociations()
        outRats = ratapplier.RatAssociations()
        
        inRats.vegclass = RatHandle('vegclass.kea')
        outRats.vegclass = RatHandle('vegclass.kea')
        
        ratapplier.apply(myFun, inRats, outRats)
        
    def myFunc(info, inputs, outputs):
        outputs.vegclass.colSum = inputs.vegclass.col1 + inputs.vegclass.col2
        
    """
    # Get a default controls object if we have not been given one
    if controls is None:
        controls = RatApplierControls()
    
    # Open all files. 
    allGdalHandles = GdalHandlesCollection(inRats, outRats)
    allGdalHandles.checkConsistency()
    rowCount = controls.rowCount
    if rowCount is None:
        rowCount = allGdalHandles.getRowCount()

    # If we can't read partial blocks, then set the block length to 
    # rowCount, i.e. there will be only one block. 
    if not haveRFC40:
        controls.setBlockLength(rowCount)

    # The current state of processing, i.e. where are we up to as 
    # we progress through the table(s)
    state = RatApplierState(rowCount)
    
    inBlocks = BlockCollection(inRats, state, allGdalHandles)
    outBlocks = BlockCollection(outRats, state, allGdalHandles)

    numBlocks = int(numpy.ceil(float(rowCount) / controls.blockLen))
    
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
        outBlocks.writeCache()
        
        # Clear block caches
        inBlocks.clearCache()
        outBlocks.clearCache()
        

class RatHandle(object):
    """
    A handle onto the RAT for a single image layer. This is used as an 
    easy way for the user to nominate both a filename and a layer number. 
    """
    def __init__(self, filename, layernum=1):
        """
        filename is a string, layernum is an integer (first layer is 1)
        """
        self.filename = filename
        self.layernum = layernum

    def __hash__(self):
        "Hash a tuple of (filename, layernum)"
        return hash((self.filename, self.layernum))
    

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
        blockNdx                Index number of current block (first block is zero, second block is 1, ...)
        startrow                RAT row number of first row of current block (first row is zero)
        blockLen                Number of rows in current block
        inputRowNumbers         Row numbers in whole input RAT(s) corresponding to current block
        rowCount                The total number of rows in the input RAT(s)
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
        endrow = min(endrow, self.rowCount-1)
        self.blockLen = endrow - self.startrow + 1
        self.inputRowNumbers = numpy.arange(self.startrow, self.startrow+self.blockLen)


class RatApplierControls(object):
    """
    Controls object for the ratapplier. An instance of this class can
    be given to the apply() function, to control its behaviour. 
    
    """
    def __init__(self):
        self.blockLen = 100000
        self.rowCount = None
    
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


class BlockCollection(object):
    """
    Hold a set of RatBlockAssociation objects, for all currently open RATs
    """
    def __init__(self, ratAssoc, state, allGdalHandles):
        """
        Create a RatBlockAssociation entry for every RatHandle in ratAssoc
        """
        for ratHandleName in ratAssoc.getRatList():
            ratHandle = getattr(ratAssoc, ratHandleName)
            gdalHandles = allGdalHandles.gdalHandlesDict[ratHandle]
            setattr(self, ratHandleName, RatBlockAssociation(state, gdalHandles))
        
    def clearCache(self):
        """
        Clear all caches
        """
        for ratHandleName in self.__dict__:
            ratBlockAssoc = getattr(self, ratHandleName)
            ratBlockAssoc.clearCache()
            
    def writeCache(self):
        """
        Write all cached data blocks
        """
        for ratHandleName in self.__dict__:
            ratBlockAssoc = getattr(self, ratHandleName)
            ratBlockAssoc.writeCache()
            

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
    beginning with "Z__", in the hope that no-one ever has a column with
    a name like this. These are all created within the __init__ method. 
    
    The main purpose of using __getattr__ is to avoid reading columns which 
    the userFunc is not actually using. As a consequence, one also needs to
    use __setattr__ to handle the data the same way. 
    
    """
    def __init__(self, state, gdalHandles):
        """
        Pass in the RatApplierState object, so we can always see where we 
        are up to, and the associated GdalHandles object, so we can get to 
        the file.
        
        Note the use of object.__setattr__() to create the normal attributes
        on the object, so they do not behave as RAT column blocks. 
        
        """
        object.__setattr__(self, 'Z__state', state)
        object.__setattr__(self, 'Z__cache', {})
        object.__setattr__(self, 'Z__gdalHandles', gdalHandles)
        object.__setattr__(self, 'Z__outputRowCount', 0)
            
        # Column usage in a form which the user function can change. 
        object.__setattr__(self, 'Z__columnUsage', {})
        for name in self.Z__gdalHandles.columnNdxByName:
            ndx = self.Z__gdalHandles.columnNdxByName[name]
            self.Z__columnUsage[name] = self.Z__gdalHandles.gdalRat.GetUsageOfCol(ndx)
        
        # The attributes which we should consider to be column names
        object.__setattr__(self, 'Z__columnNameSet', set())
    
    def setUsage(self, columnName, usage):
        """
        Set the usage of the given column. 
        """
        self.Z__columnUsage[columnName] = usage
    
    def getUsage(self, columnName):
        """
        Return the usage of the given column
        """
        usage = gdal.GFU_Generic
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
            gdalRat = self.Z__gdalHandles.gdalRat
            colNdx = self.Z__gdalHandles.columnNdxByName[columnName]
            if haveRFC40:
                dataBlock = gdalRat.ReadAsArray(colNdx, start=self.Z__state.startrow, 
                    length=self.Z__state.blockLen)
            else:
                gdalBand = self.Z__gdalHandles.band
                dataBlock = rat.readColumnFromBand(gdalBand, columnName)
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
    
    def writeCache(self):
        """
        Write all cached data blocks. Creates the columns if they do not already exist. 
        """
        rowsToWrite = None
        # Loop over all columns names which have been set on this object
        for columnName in self.Z__columnNameSet:
            gdalRat = self.Z__gdalHandles.gdalRat
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

            if haveRFC40:
                # Check if the column needs to be created
                if columnName not in self.Z__gdalHandles.columnNdxByName:
                    columnType = rat.inferColumnType(dataBlock)
                    columnUsage = self.getUsage(columnName)
                    gdalRat.CreateColumn(columnName, columnType, columnUsage)
                    # Work out the new column index
                    columnNdx = gdalRat.GetColumnCount() - 1
                    self.Z__gdalHandles.columnNdxByName[columnName] = columnNdx
                
                # Write the block of data into the RAT column
                columnNdx = self.Z__gdalHandles.columnNdxByName[columnName]
                if len(dataBlock) > 0:
                    if gdalRat.GetRowCount() < self.Z__outputRowCount+rowsToWrite:
                        gdalRat.SetRowCount(self.Z__outputRowCount+rowsToWrite)
                        
                    gdalRat.WriteArray(dataBlock, columnNdx, self.Z__outputRowCount)
                # There may be a problem with HFA Byte arrays, if we don't end up writing 256 rows....
            else:
                gdalBand = self.Z__gdalHandles.band
                rat.writeColumnToBand(gdalBand, columnName, dataBlock)
        
        # Increment Z__outputRowCount, without triggering __setattr__. 
        object.__setattr__(self, 'Z__outputRowCount', self.Z__outputRowCount + rowsToWrite)


class GdalHandles(object):
    """
    Hang onto all the required GDAL objects relating to a given opened RAT.
    Attributes are:
        ds                  The gdal.Dataset object
        band                The gdal.Band object
        gdalRat             The gdal.RasterAttributeTable object
        columnNdxByName     A lookup table to get column index from column name
        
    """
    def __init__(self, ratHandle, update=False, sharedDS=None):
        """
        If update is True, the GDAL dataset is opened with gdal.GA_Update.
        If sharedDS is not None, this is used as the GDAL dataset, rather
        than opening a new one. 
        """
        if sharedDS is None:
            if update:
                self.ds = gdal.Open(ratHandle.filename, gdal.GA_Update)
            else:
                self.ds = gdal.Open(ratHandle.filename)
        else:
            self.ds = sharedDS
        
        self.band = self.ds.GetRasterBand(ratHandle.layernum)
        self.gdalRat = self.band.GetDefaultRAT()
        
        # A lookup table so we can get column index from the name. GDAL does not
        # currently provide this, although my feeling is perhaps it should. 
        self.columnNdxByName = {}
        for i in range(self.gdalRat.GetColumnCount()):
            name = self.gdalRat.GetNameOfCol(i)
            self.columnNdxByName[name] = i


class GdalHandlesCollection(object):
    """
    A set of all the GdalHandles objects
    """
    def __init__(self, inRats, outRats):
        """
        Open all the raster files, storing a dictionary of GdalHandles objects
        as self.gdalHandlesDict. This is keyed by RatHandle objects. 
        
        Output files are opened first, with update=True. Any input files which
        are not open are then opened with update=False.

        Extra effort is made to cope with the very unlikely case of opening
        RATs on separate layers in the same image file, mostly because if
        it did happen, it would go horribly wrong. Such cases are able to share 
        the same gdal.Dataset object. 
        
        """
        self.gdalHandlesDict = {}
        self.inputRatList = []
        
        # Do the output files first, so they get opened with update=True
        for ratHandleName in outRats.getRatList():
            ratHandle = getattr(outRats, ratHandleName)
            if ratHandle not in self.gdalHandlesDict:
                sharedDS = self.checkExistingDS(ratHandle)
                self.gdalHandlesDict[ratHandle] = GdalHandles(ratHandle, update=True, sharedDS=sharedDS)
        for ratHandleName in inRats.getRatList():
            ratHandle = getattr(inRats, ratHandleName)
            if ratHandle not in self.gdalHandlesDict:
                sharedDS = self.checkExistingDS(ratHandle)
                self.gdalHandlesDict[ratHandle] = GdalHandles(ratHandle, update=False, sharedDS=sharedDS)
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
            gdalHandles = self.gdalHandlesDict[firstRatHandle]
            rowCount = gdalHandles.gdalRat.GetRowCount()
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
            rowCount = self.gdalHandlesDict[ratHandle].gdalRat.GetRowCount()
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
        for existingRatHandle in self.gdalHandlesDict:
            if existingRatHandle.filename == ratHandle.filename:
                sharedDS = self.gdalHandlesDict[existingRatHandle].ds
        return sharedDS
