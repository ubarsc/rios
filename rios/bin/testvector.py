"""
A simple test of the vector inputs. 

Creates a simple raster, and a simple vector, using straight gdal/ogr.
Then reads the raster, masked by the vector, and calculates the mean of the masked
area. Does the same thing with straight numpy, and checks the results. 

"""
import os

import numpy

from rios import applier
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
    
    meanVal3 = calcMeanWithRiosApplier(imgfile, vecfile)
    
    if meanVal == meanVal2 and meanVal == meanVal3:
        riostestutils.report(TESTNAME, "Passed")
    elif meanVal != meanVal3:
        riostestutils.report(TESTNAME, "Failed. Applier and low-level disagree (%s != %s)"%(meanVal, meanVal3))
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


def calcMeanWithRiosApplier(imgfile, vecfile):
    """
    Use RIOS's vector facilities, through the applier, to calculate the
    mean of the image within the vector
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    otherargs = applier.OtherInputs()
    infiles.img = imgfile
    infiles.vec = vecfile
    controls.setBurnValue(-1)
    controls.setVectorDatatype(numpy.int16)
    otherargs.total = 0
    otherargs.count = 0
    
    applier.apply(meanWithinVec, infiles, outfiles, otherargs, controls=controls)

    mean = otherargs.total / otherargs.count
    return mean
    

def meanWithinVec(info, inputs, outputs, otherargs):
    """
    Called from RIOS applier to accumulate sum and count of values
    within a vector
    """
    mask = (inputs.vec < 0)
    vals = inputs.img[mask]
    otherargs.count += len(vals)
    otherargs.total += vals.sum()

    
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
