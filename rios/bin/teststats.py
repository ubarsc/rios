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
    
    stats1 = getStatsFromBand(band)
    stats2 = getStatsFromArray(rampArr, nullVal)
    ok = compareStats(stats1, stats2)
    
    del ds
    
    if os.path.exists(imgfile):
        os.remove(imgfile)
    
    return ok


def getStatsFromBand(band):
    """
    Get statistics from given band object, return Stats instance
    """
    mean = float(band.GetMetadataItem('STATISTICS_MEAN'))
    stddev = float(band.GetMetadataItem('STATISTICS_STDDEV'))
    minval = float(band.GetMetadataItem('STATISTICS_MINIMUM'))
    maxval = float(band.GetMetadataItem('STATISTICS_MAXIMUM'))
    median = float(band.GetMetadataItem('STATISTICS_MEDIAN'))
    mode = float(band.GetMetadataItem('STATISTICS_MODE'))
    statsObj = Stats(mean, stddev, minval, maxval, median, mode)
    return statsObj


def getStatsFromArray(arr, nullVal):
    """
    Work out the statistics directly from the image array. 
    Return a Stats instance
    """
    nonNullMask = (arr != nullVal)
    nonNullArr = arr[nonNullMask].astype(numpy.float64)
    
    # Work out what the correct answers should be
    mean = nonNullArr.mean()
    stddev = nonNullArr.std()
    minval = nonNullArr.min()
    maxval = nonNullArr.max()
    median = numpy.median(nonNullArr)
    mode = scipy.stats.mode(nonNullArr, axis=None)[0][0]
    return Stats(mean, stddev, minval, maxval, median, mode)


def equalTol(a, b, tol):
    """
    Compare two values to within a tolerance. If the difference
    between the two values is smaller than the tolerance, 
    then return True
    """
    diff = abs(a - b)
    return (diff < tol)

class Stats(object):
    def __init__(self, mean, stddev, minval, maxval, median, mode):
        self.mean = mean
        self.stddev = stddev
        self.minval = minval
        self.maxval = maxval
        self.median = median
        self.mode = mode


def compareStats(stats1, stats2):
    """
    Compare two Stats instances, and report differences. Also
    return True if all OK. 
    """
    ok = True
    msgList = []
    tolerance = 0.000000001
    for statsName in ['mean', 'median', 'minval', 'maxval', 'median', 'mode']:
        value1 = getattr(stats1, statsName)
        value2 = getattr(stats2, statsName)
        if not equalTol(value1, value2, tolerance):
            msgList.append("Error in %s: %s != %s" % (statsName, value1, value2))
    
    if len(msgList) > 0:
        ok = False
        riostestutils.report(TESTNAME, '\n'.join(msgList))
    else:
        riostestutils.report(TESTNAME, "Passed")
    return ok

if __name__ == "__main__":
    run()
