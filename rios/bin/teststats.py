"""
Test the calculation of statistics by rios.calcstats. 
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
import os

import numpy
from osgeo import gdal
import scipy.stats
from rios import calcstats, cuiprogress

import riostestutils

TESTNAME = 'TESTSTATS'

def run():
    """
    Run a test of statistics calculation
    """
    riostestutils.reportStart(TESTNAME)
    
    # Create a sample image, and calculate statistics on it
    nullVal = 0
    imgfile = 'ramp.img'
    riostestutils.genRampImageFile(imgfile)
    ds = gdal.Open(imgfile, gdal.GA_Update)
    calcstats.calcStats(ds, progress=cuiprogress.SilentProgress(), 
        ignore=nullVal)
    del ds
    
    # Read back the data as a numpy array
    ds = gdal.Open(imgfile)
    band = ds.GetRasterBand(1)
    rampArr = band.ReadAsArray()
    # Read the stats from the file
    fileMean = float(band.GetMetadataItem('STATISTICS_MEAN'))
    fileStddev = float(band.GetMetadataItem('STATISTICS_STDDEV'))
    fileMin = float(band.GetMetadataItem('STATISTICS_MINIMUM'))
    fileMax = float(band.GetMetadataItem('STATISTICS_MAXIMUM'))
    fileMedian = float(band.GetMetadataItem('STATISTICS_MEDIAN'))
    fileMode = float(band.GetMetadataItem('STATISTICS_MODE'))
    del ds
    
    # Work out the statistics directly from the data array, using numpy
    nonNullMask = (rampArr != nullVal)
    nonNullArr = rampArr[nonNullMask].astype(numpy.float64)
    
    # Work out what the correct answers should be
    mean = nonNullArr.mean()
    stddev = nonNullArr.std()
    minVal = nonNullArr.min()
    maxVal = nonNullArr.max()
    median = numpy.median(nonNullArr)
    # Get the first mode, since (in theory) there can be more than one
    mode = scipy.stats.mode(nonNullArr)[0]
    
    ok = True
    msgList = []
    tolerance = 0.000000001
    if not equalTol(mean, fileMean, tolerance):
        msgList.append("Error in mean: %s != %s" % (fileMean, mean))
    if not equalTol(stddev, fileStddev, tolerance):
        msgList.append("Error in stddev: %s != %s" % (fileStddev, stddev))
    if not equalTol(minVal, fileMin, tolerance):
        msgList.append("Error in min: %s != %s" % (fileMin, minVal))
    if not equalTol(maxVal, fileMax, tolerance):
        msgList.append("Error in max: %s != %s" % (fileMax, maxVal))
    if not equalTol(median, fileMedian, tolerance):
        msgList.append("Error in median: %s != %s" % (fileMedian, median))
    if not equalTol(mode, fileMode, tolerance):
        msgList.append("Error in mode: %s != %s" % (fileMode, mode))
    
    if len(msgList) > 0:
        ok = False
        riostestutils.report(TESTNAME, '\n'.join(msgList))
    else:
        riostestutils.report(TESTNAME, "Passed")
    
    if os.path.exists(imgfile):
        os.remove(imgfile)
    
    return ok

def equalTol(a, b, tol):
    """
    Compare two values to within a tolerance. If the difference
    between the two values is smaller than the tolerance, 
    then return True
    """
    diff = abs(a - b)
    return (diff < tol)

if __name__ == "__main__":
    run()
