"""
A simple test of the vector inputs. 

Creates a simple raster, and a simple vector, using straight gdal/ogr.
Then reads the raster, masked by the vector, and calculates the mean of the masked
area. Does the same thing with straight numpy, and checks the results. 

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

from rios import applier

from . import riostestutils

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
    
    meanVal_numpy = calcMeanWithNumpy()
    
    meanVal_rios = calcMeanWithRiosApplier(imgfile, vecfile)
    
    ok = True
    if meanVal_numpy == meanVal_rios:
        riostestutils.report(TESTNAME, "Passed")
    else:
        riostestutils.report(TESTNAME,
            "Failed. Mean values unequal (%s != %s)"%(meanVal_numpy, meanVal_rios))
        ok = False
    
    # Cleanup
    riostestutils.removeRasterFile(imgfile)
    riostestutils.removeVectorFile(vecfile)
    
    return ok


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
    otherargs.total += vals.astype(numpy.float32).sum()

    
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
    
    subArr = rampArr[minRow:maxRow + 1, minCol:maxCol + 1]
    meanVal = subArr.mean()
    return meanVal
