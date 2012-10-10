
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

# use turborat if available
try:
    from turbogdal import turborat
    HAVE_TURBORAT = True
except ImportError:
    HAVE_TURBORAT = False

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
            if HAVE_TURBORAT:
                # if turborat is available use that
                colArray = turborat.readColumn(rat, col)
            else:
                # do it the slow way
                dtype = rat.GetTypeOfCol(col)
                if dtype == gdal.GFT_Integer:
                    colArray = numpy.zeros(numRows,int)
                elif dtype == gdal.GFT_Real:
                    colArray = numpy.zeros(numRows,float)
                elif dtype == gdal.GFT_String:
                    # for string attributes, create a list - convert later
                    colArray = []
                else:
                    msg = "Can't interpret data type of attribute"
                    raise rioserrors.AttributeTableTypeError(msg)
            
        
                # do it checking the type outside the loop for maximum speed
                if dtype == gdal.GFT_Integer:
                    for row in range(numRows):
                        val = rat.GetValueAsInt(row,col)
                        colArray[row] = val
                elif dtype == gdal.GFT_Real:
                    for row in range(numRows):
                        val = rat.GetValueAsDouble(row,col)
                        colArray[row] = val
                else:
                    for row in range(numRows):
                        val = rat.GetValueAsString(row,col)
                        colArray.append(val)

                if isinstance(colArray, list):
                    # convert to array - numpy can handle this now it can work out the lengths
                    colArray = numpy.array(colArray)
                
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
    associated with the file as a list.
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

    if HAVE_TURBORAT:
        # use turborat to write values to RAT if available
        if not isinstance(sequence, numpy.ndarray):
            # turborat.writeColumn needs an array
            sequence = numpy.array(sequence)
        turborat.writeColumn(attrTbl, colNum, sequence, rowsToAdd)
    else:
        defaultValues = {gdal.GFT_Integer:0, gdal.GFT_Real:0.0, gdal.GFT_String:''}

        # go thru and set each value into the RAT
        for rowNum in range(rowsToAdd):
            if rowNum >= len(sequence):
                # they haven't given us enough values - fill in with default
                val = defaultValues[colType]
            else:
                val = sequence[rowNum]

            if colType == gdal.GFT_Integer:
                # appears that swig cannot convert numpy.int64
                # to the int type required by SetValueAsInt
                # so we need to cast. 
                # This is a problem as readColumn returns numpy.int64 
                # for integer columns. 
                # Seems fine converting numpy.float64 to 
                # float however for SetValueAsDouble.
                attrTbl.SetValueAsInt(rowNum, colNum, int(val))
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
    file. colType must be one of gdal.GFT_Integer,gdal.GFT_Real,gdal.GFT_String.
    """
    if isinstance(imgFile, basestring):
        ds = gdal.Open(str(imgFile), gdal.GA_Update)
    elif isinstance(imgFile, gdal.Dataset):
        ds = imgFile

    gdalBand = ds.GetRasterBand(bandNumber) 

    writeColumnToBand(gdalBand, colName, sequence, colType) 

def getColorTable(imgFile, bandNumber=1):
    """
    Given either an open gdal dataset, or a filename,
    reads the color table as an array that can be passed
    to ImageWriter.setColorTable() or rat.setColorTable()
    
    The returned colour table is a numpy array, described in detail
    in the docstring for rat.setColorTable(). 
    
    """
    if isinstance(imgFile, basestring):
        ds = gdal.Open(str(imgFile))
    elif isinstance(imgFile, gdal.Dataset):
        ds = imgFile

    gdalBand = ds.GetRasterBand(bandNumber)
    colorTable = gdalBand.GetColorTable()
    if colorTable is None:
        raise rioserrors.AttributeTableColumnError("Image has no color table")

    count = colorTable.GetCount()
    colorArray = numpy.zeros((count, 5), dtype=numpy.uint8)
    for index in range(count):
        colorEntry = colorTable.GetColorEntry(index)
        arrayEntry = [index] + list(colorEntry)
        colorArray[index] = numpy.array(arrayEntry)

    return colorArray


def setColorTable(imgfile, colorTblArray, layernum=1):
    """
    Set the color table for the specified band. You can specify either 
    the imgfile as either a filename string or a gdal.Dataset object. The
    layer number defaults to 1, i.e. the first layer in the file. 
    
    The color table is given as a numpy array of 5 columns. There is one row 
    (i.e. first array index) for every value to be set, and the columns
    are:
        pixelValue
        Red
        Green
        Blue
        Opacity
    The Red/Green/Blue values are on the range 0-255, with 255 meaning full 
    color, and the opacity is in the range 0-255, with 255 meaning fully 
    opaque. 
    
    The pixels values in the first column must be in ascending order, but do 
    not need to be a complete set (i.e. you don't need to supply a color for 
    every possible pixel value - any not given will default to transparent black).
    It does not even need to be contiguous. 
    
    For reasons of backwards compatability, a 4-column array will also be accepted, 
    and will be treated as though the row index corresponds to the pixelValue (i.e. 
    starting at zero). 
    
    """
    arrayShape = colorTblArray.shape
    if len(arrayShape) != 2:
        raise rioserrors.ArrayShapeError("ColorTableArray must be 2D. Found shape %s instead"%arrayShape)
        
    (numRows, numCols) = arrayShape
    # Handle the backwards-compatible case of a 4-column array
    if numCols == 4:
        numCols = 5
        arrayShape = (numRows, numCols)
        colorTbl5cols = numpy.zeros(arrayShape, dtype=numpy.uint8)
        colorTbl5cols[:, 0] = numpy.arange(numRows)
        colorTbl5cols[:, 1:] = colorTblArray
        colorTblArray = colorTbl5cols
        
    if numCols != 5:
        raise rioserrors.ArrayShapeError("Color table array has %d columns, expecting 5"%numCols)
    
    # Open the image file and get the band object
    if isinstance(imgfile, gdal.Dataset):
        ds = imgfile
    elif isinstance(imgfile, basestring):
        ds = gdal.Open(imgfile, gdal.GA_Update)
    
    bandobj = ds.GetRasterBand(layernum)
    
    clrTbl = gdal.ColorTable()
    maxPixVal = colorTblArray[:, 0].max()
    i = 0
    # This loop sets an entry for every pixel value up to the largest given. Imagine
    # bitches if we don't do this. 
    tblMaxVal = maxPixVal
    if bandobj.DataType == gdal.GDT_Byte:
        # For Byte files, we always add rows for entries up to 255. Imagine gets 
        # confused if we don't
        tblMaxVal = 255
        
    for pixVal in range(tblMaxVal+1):
        while  i < numRows and colorTblArray[i, 0] < pixVal:
            i += 1
        if i < numRows:
            tblPixVal = colorTblArray[i, 0]
            if tblPixVal == pixVal:
                colEntry = tuple(colorTblArray[i, 1:])
            else:
                colEntry = (0, 0, 0, 0)
        else:
            colEntry = (0, 0, 0, 0)
        clrTbl.SetColorEntry(pixVal, colEntry)
    
    bandobj.SetRasterColorTable(clrTbl)

