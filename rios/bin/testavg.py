"""
Does a broad, general test of the applier functionality. 

Generates a pair of images, and then applies a function to calculate
the average of them. Checks the resulting output against a known 
correct answer. 

Prints a message stderr if something wrong.  

"""
import os

import numpy
from osgeo import gdal
from rios import applier

import riostestutils

TESTNAME = "TESTAVG"

def run():
    """
    Run the test
    """
    ramp1 = 'ramp1.img'
    ramp2 = 'ramp2.img'
    riostestutils.genRampImageFile(ramp1)
    riostestutils.genRampImageFile(ramp2, reverse=True)
    outfile = 'rampavg.img'
    
    calcAverage(ramp1, ramp2, outfile)
    
    checkResult(outfile)
    
    # Clean up
    for filename in [ramp1, ramp2, outfile]:
        os.remove(filename)


def calcAverage(file1, file2, avgfile):
    """
    Use RIOS to calculate the average of two files.
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    infiles.img = [file1, file2]
    outfiles.avg = avgfile
    
    applier.apply(doAvg, infiles, outfiles)


def doAvg(info, inputs, outputs):
    """
    Called from RIOS.
    
    Calculate the average of the input files. 
    
    """
    tot = inputs.img[0].astype(numpy.float32)
    for img in inputs.img[1:]:
        tot += img
    outputs.avg = (tot / len(inputs.img)).astype(numpy.uint8)


def checkResult(avgfile):
    """
    Read in from the given file, and check that it matches what we 
    think it should be
    """
    # Work out the correct answer
    ramp1 = riostestutils.genRampArray()
    ramp2 = riostestutils.genRampArray()[:, ::-1]
    tot = (ramp1.astype(numpy.float32) + ramp2)
    avg = (tot / 2.0).astype(numpy.uint8)
    
    # Read what RIOS wrote
    ds = gdal.Open(avgfile)
    band = ds.GetRasterBand(1)
    riosavg = band.ReadAsArray()
    del ds
    
    # Check that they are the same
    if avg.shape != riosavg.shape:
        riostestutils.report(TESTNAME, "Shape mis-match: %s != %s"%(avg.shape, riosavg.shape))
    elif (riosavg-avg).any():
        riostestutils.report(TESTNAME, "Incorrect result. Average difference = %s"%(riosavg-avg).mean())
    else:
        riostestutils.report(TESTNAME, "Passed")
