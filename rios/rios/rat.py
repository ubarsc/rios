
"""
This module contains routines for reading and writing Raster
Attribute Tables (RATs). These are designed to be able to 
be called from outside of RIOS.

Within RIOS, these are called from the ReaderInfo and ImageWriter
classes.
"""
import sys
from osgeo import gdal
import numpy
from . import rioserrors

if sys.version_info[0] > 2:
    # hack for Python 3 which uses str instead of basestring
    # we just use basestring
    basestring = str

def readColumnFromBand(gdalBand, colName):
    """
    Given a GDAL Band, extract the Raster Attribute with the
    given name. Returns an array of ints or floats for numeric
    data types, or a list of strings.
    
    """
    # get the RAT for this band
    rat = gdalBand.GetDefaultRAT()

    # get the size of the RAT  
    numCols = rat.GetColumnCount()
    numRows = rat.GetRowCount()
  
    # if this is still None at the end
    # we didn't find the column
    colArray = None

    # loop thru the columns looking for the right one
    for col in range(numCols):
        if rat.GetNameOfCol(col) == colName:
            # found it - create the output array
            # and fill in the values
            dtype = rat.GetTypeOfCol(col)
            if dtype == gdal.GFT_Integer:
                colArray = numpy.zeros(numRows,int)
            elif dtype == gdal.GFT_Real:
                colArray = numpy.zeros(numRows,float)
            elif dtype == gdal.GFT_String:
                # for string attributes, create a list
                colArray = []
            else:
                msg = "Can't interpret data type of attribute"
                raise rioserrors.AttributeTableTypeError(msg)
            
            for row in range(numRows):
        
                # do it checking the type
                if dtype == gdal.GFT_Integer:
                    val = rat.GetValueAsInt(row,col)
                    colArray[row] = val
                elif dtype == gdal.GFT_Real:
                    val = rat.GetValueAsDouble(row,col)
                    colArray[row] = val
                else:
                    val = rat.GetValueAsString(row,col)
                    colArray.append(val)
                
            # exit loop
            break

    # couldn't find named column - raise exception
    if colArray is None:
        msg = "Unable to find column named '%s'" % colName
        raise rioserrors.AttributeTableColumnError(msg)
    
    # return the lookup array to the caller
    return colArray

def readColumn(imgFile, colName, bandNumber=1):
    """
    Given either an open gdal dataset, or a filename,
    extract the Raster Attribute with the
    given name. Returns an array of ints or floats for numeric
    data types, or a list of strings.
    """
    if isinstance(imgFile, basestring):
        ds = gdal.Open(str(imgFile))
    elif isinstance(imgFile, gdal.Dataset):
        ds = imgFile

    gdalBand = ds.GetRasterBand(bandNumber) 

    return readColumnFromBand(gdalBand, colName)

def getColumnNamesFromBand(gdalBand):
    """
    Return the names of the columns in the attribute table
    associated with the gdalBand as a list.
    """
    # get the RAT for this band
    rat = gdalBand.GetDefaultRAT()

    colNames = []

    numCols = rat.GetColumnCount()
    for col in range(numCols):
        name = rat.GetNameOfCol(col)
        colNames.append(name)

    return colNames

def getColumnNames(imgFile, bandNumber=1):
    """
    Given either an open gdal dataset, or a filename,
    Return the names of the columns in the attribute table
    associated with the gdalBand as a list.
    """
    if isinstance(imgFile, basestring):
        ds = gdal.Open(str(imgFile))
    elif isinstance(imgFile, gdal.Dataset):
        ds = imgFile

    gdalBand = ds.GetRasterBand(bandNumber) 

    return getColumnNamesFromBand(gdalBand)

def writeColumnToBand(gdalBand, colName, sequence, colType=None):
    """
    Given a GDAL band, Writes the data specified in sequence 
    (can be list, tuple or array etc)
    to the named column in the attribute table assocated with the
    gdalBand. colType must be one of gdal.GFT_Integer,gdal.GFT_Real,gdal.GFT_String.
    GDAL dataset must have been created, or opened with GA_Update
    """

    if colType is None:
        # infer from the type of the first element in the sequence
        if isinstance(sequence[0],int) or isinstance(sequence[0],numpy.integer):
            colType = gdal.GFT_Integer
        elif isinstance(sequence[0],float) or isinstance(sequence[0],numpy.floating):
            colType = gdal.GFT_Real
        elif isinstance(sequence[0],basestring):
            colType = gdal.GFT_String
        else:
            msg = "Can't infer type of column for sequence of %s" % type(sequence[0])
            raise rioserrors.AttributeTableTypeError(msg)

    # check it is acually a valid type
    elif colType not in (gdal.GFT_Integer,gdal.GFT_Real,gdal.GFT_String):
        msg = "coltype must be a valid gdal column type"
        raise rioserrors.AttributeTableTypeError(msg)

    # create the RAT - if the colunm already exists it gets over-written
    # because of the way the HFA driver works
    # not sure if we should check or not...
    attrTbl = gdal.RasterAttributeTable()
    attrTbl.CreateColumn(colName, colType, gdal.GFU_Generic)
    colNum = attrTbl.GetColumnCount() - 1

    rowsToAdd = len(sequence)
    # Imagine has trouble if not 256 items for byte
    if gdalBand.DataType == gdal.GDT_Byte:
        rowsToAdd = 256

    defaultValues = {gdal.GFT_Integer:0, gdal.GFT_Real:0.0, gdal.GFT_String:''}

    # go thru and set each value into the RAT
    for rowNum in range(rowsToAdd):
        if rowNum >= len(sequence):
            # they haven't given us enough values - fill in with default
            val = defaultValues[colType]
        else:
            val = sequence[rowNum]

        if colType == gdal.GFT_Integer:
            attrTbl.SetValueAsInt(rowNum, colNum, val)
        elif colType == gdal.GFT_Real:
            attrTbl.SetValueAsDouble(rowNum, colNum, val)
        else:
            attrTbl.SetValueAsString(rowNum, colNum, val)

    gdalBand.SetDefaultRAT(attrTbl)

def writeColumn(imgFile, colName, sequence, colType=None, bandNumber=1):
    """
    Given either an open gdal dataset, or a filename,
    writes the data specified in sequence (can be list, tuple or array etc)
    to the named column in the attribute table assocated with the
    gdalBand. colType must be one of gdal.GFT_Integer,gdal.GFT_Real,gdal.GFT_String.
    """
    if isinstance(imgFile, basestring):
        ds = gdal.Open(str(imgFile), gdal.GA_Update)
    elif isinstance(imgFile, gdal.Dataset):
        ds = imgFile

    gdalBand = ds.GetRasterBand(bandNumber) 

    writeColumnToBand(gdalBand, colName, sequence, colType) 
