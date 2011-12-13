"""
A simple test of the vector inputs. 

Creates a simple raster, and a simple vector, using straight gdal/ogr.
Then reads the raster, masked by the vector, and calculates the mean of the masked
area. Does the same thing with straight numpy, and checks the results. 

"""
import os

import numpy

from rios import imagereader
from rios import vectorreader

import riostestutils

TESTNAME = "TESTVECTOR"

def run():
    """
    Run the simple vector test
    """
    riostestutils.reportStart(TESTNAME)
    
    imgfile = 'ramp1.img'
    riostestutils.genRampImageFile(imgfile)
    
    vecfile = 'square.shp'
    riostestutils.genVectorSquare(vecfile)
    
    meanVal = calcMeanWithRios(imgfile, vecfile)
    
    meanVal2 = calcMeanWithNumpy()
    
    if meanVal == meanVal2:
        riostestutils.report(TESTNAME, "Passed")
    else:
        riostestutils.report(TESTNAME, "Failed. Mean values unequal (%s != %s)"%(meanVal, meanVal2))
    
    # Cleanup
    os.remove(imgfile)
    for ext in ['shp', 'shx', 'dbf', 'prj']:
        os.remove(vecfile.replace('shp', ext))


def calcMeanWithRios(imgfile, vecfile):
    """
    Use RIOS's vector facilities to calculate the mean of the 
    image within the vector
    
    Uses the low-level calls, because the applier does not yet support 
    the vector stuff. 
    
    """
    reader = imagereader.ImageReader([imgfile])
    vec = vectorreader.Vector(vecfile, burnvalue=-1, datatype=numpy.int16)
    vreader = vectorreader.VectorReader([vec])
    
    total = 0
    count = 0
    for (info, blocklist) in reader:
        vecblockList = vreader.rasterize(info)
        vecblock = vecblockList[0]
        
        block = blocklist[0]
        
        # Mask a boolean mask from the vector
        mask = (vecblock < 0)
        vals = block[mask]
        count += len(vals)
        total += vals.sum()
    
    
    if count > 0:
        meanVal = total / count
    else:
        meanVal = -1
    return meanVal


def calcMeanWithNumpy():
    """
    Calculate the mean using numpy. This kind of relies on just "knowing" how the 
    vector was generated. It would be better if the part that generated the vector
    had returned a bit more information that I could just use here, but didn't get
    too carried away. Should tidy it up, though. 
    
    """
    rampArr = riostestutils.genRampArray()
    squareSize = 20
    minRow = 11
    maxRow = minRow + squareSize - 1
    minCol = 11
    maxCol = minCol + squareSize - 1
    
    subArr = rampArr[minRow:maxRow+1, minCol:maxCol+1]
    meanVal = subArr.mean()
    return meanVal
