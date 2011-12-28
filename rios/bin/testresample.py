"""
A simple test with resampling required. Two input images
are generated on a slightly different grid, and then rios averages
the two, allowing a resample. 
"""
import os

import numpy
from osgeo import gdal
from rios import applier
from rios import cuiprogress

import riostestutils

TESTNAME = "TESTRESAMPLE"

def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)

    ramp1 = 'ramp1.img'
    ramp2 = 'ramp2.img'
    riostestutils.genRampImageFile(ramp1)
    # Now generate a similar file, but with a small offset. This corresponds 
    # to shifting the image 1 pixel across and 2 pixels down, although not precisely
    xLeft = riostestutils.DEFAULT_XLEFT + 9
    yTop = riostestutils.DEFAULT_YTOP - 19
    riostestutils.genRampImageFile(ramp2, xLeft=xLeft, yTop=yTop)
    outfile = 'rampavg.img'
    
    calcAverage(ramp1, ramp2, outfile)
    
    checkResult(outfile)
    
    # Clean up
    for filename in [ramp1, ramp2, outfile]:
        os.remove(filename)


def calcAverage(file1, file2, avgfile):
    """
    Use RIOS to calculate the average of two files. Allows
    nearest-neighbour resampling of the second file. 
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    infiles.img = [file1, file2]
    outfiles.avg = avgfile
    controls = applier.ApplierControls()
    controls.setReferenceImage(file1)
    controls.setResampleMethod('near')
    controls.setProgress(cuiprogress.CUIProgressBar())
    
    applier.apply(doAvg, infiles, outfiles, controls=controls)


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
    # Do a nearest-neighbour resample of the second array
    ramp2 = ramp1[:-2, :-1]
    # Get the corresponding part of the first array
    ramp1 = ramp1[2:, 1:]
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
