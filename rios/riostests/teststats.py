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
import traceback

import numpy
from osgeo import gdal
from osgeo.gdal_array import GDALTypeCodeToNumericTypeCode

from rios import applier, VersionObj

from rios.riostests import riostestutils

TESTNAME = 'TESTSTATS'

# Work-around, see https://github.com/OSGeo/gdal/issues/13543
if gdal.__version__ == "3.12.0":
    os.environ['GDAL_STATS_USE_FLOAT64_OPTIM'] = 'NO'


def run():
    """
    Run a test of statistics calculation
    """
    riostestutils.reportStart(TESTNAME)

    allOK = True
    
    # We repeat the basic test for a number of different GDAL datatypes, with different
    # ranges of data. Each element of the following list is a tuple of
    #    (gdalDataType, scalefactor)
    # for which the test is run. The original data being scaled is in 
    # the range 25-100 (after clobbering half the array as nulls, to ensure that
    # the nulls are enough to make a difference). 
    dataTypesList = [
        (gdal.GDT_Byte, 1),
        (gdal.GDT_UInt16, 1),
        (gdal.GDT_Int16, 300),
        (gdal.GDT_UInt16, 300),
        (gdal.GDT_Int32, 30000),
        (gdal.GDT_UInt32, 30000),
        (gdal.GDT_Float32, 1),
        (gdal.GDT_Float32, 100),
        (gdal.GDT_Float32, 0.01),
        (gdal.GDT_Float64, 1),
        (gdal.GDT_Float64, 100),
        (gdal.GDT_Float64, 0.01)
    ]
    # Include 64-bit int types, if supported. Int64/UInt64 were not fully
    # supported until 3.5.2 - see https://github.com/OSGeo/gdal/pull/6059
    if VersionObj(gdal.__version__) >= VersionObj('3.5.2'):
        dataTypesList.append((gdal.GDT_Int64, 30000))
        dataTypesList.append((gdal.GDT_UInt64, 30000))
    if hasattr(gdal, 'GDT_Int8'):
        dataTypesList.append((gdal.GDT_Int8, 1))
    
    # We repeat these tests on a number of different drivers, if they are
    # available, as some stats-related things may work fine on some drivers
    # but not on others. 
    driverTestList = [
        ('HFA', ['COMPRESS=YES']),
        ('GTiff', ['COMPRESS=DEFLATE', 'TILED=YES', 'INTERLEAVE=BAND']),
        ('KEA', [])
    ]
    # Remove any drivers not supported by current GDAL
    driverTestList = [(drvrName, options) for (drvrName, options) in driverTestList
        if gdal.GetDriverByName(drvrName) is not None]

    # Create a test input file
    rampInfile = 'ramp.img'
    riostestutils.genRampImageFile(rampInfile)
    offset = 0

    # Loop over all drivers
    for (driverName, creationOptions) in driverTestList:
        drvr = gdal.GetDriverByName(driverName)
        # File extension for this driver
        ext = drvr.GetMetadataItem('DMD_EXTENSION')

        # Restrict to datatypes supported by this driver
        supportedTypeNames = drvr.GetMetadataItem('DMD_CREATIONDATATYPES').split()
        supportedTypes = set([gdal.GetDataTypeByName(tn) for tn in supportedTypeNames])
        dataTypesForDriver = [(gdt, scale) for (gdt, scale) in dataTypesList
            if gdt in supportedTypes]

        # Loop over all datatype tuples in the list
        for (fileDtype, scalefactor) in dataTypesForDriver:
            ok = testForDriverAndType(driverName, creationOptions,
                fileDtype, scalefactor, offset, rampInfile, ext)
            allOK = allOK and ok

    # A simple test of omitting pyramids/stats/histogram
    gtiffOptions = [options for (drvrName, options) in driverTestList
        if drvrName == "GTiff"][0]
    ok = runOneTest('GTiff', gtiffOptions, gdal.GDT_Byte, 1, 0, rampInfile, 'tif',
        True, None, False)
    allOK = allOK and ok
    # A test with negative pixel values
    ok = runOneTest('GTiff', gtiffOptions, gdal.GDT_Int16, 300, -20, rampInfile,
        'tif', False, None, False)
    allOK = allOK and ok
    # A test with no null value
    ok = runOneTest('GTiff', gtiffOptions, gdal.GDT_Byte, 1, 0, rampInfile,
        'tif', False, None, False, noNull=True)
    allOK = allOK and ok

    # Run a test with the output being all null values
    ok = testAllNull()
    allOK = allOK and ok
    
    if os.path.exists(rampInfile):
        riostestutils.removeRasterFile(rampInfile)

    if allOK:
        riostestutils.report(TESTNAME, "Passed")

    return allOK


hugeIntGDALTypes = (gdal.GDT_Int32, gdal.GDT_UInt32)
floatGDALTypes = (gdal.GDT_Float32, gdal.GDT_Float64)
if VersionObj(gdal.__version__) >= VersionObj('3.5.2'):
    hugeIntGDALTypes += (gdal.GDT_Int64, gdal.GDT_UInt64)


def testForDriverAndType(driverName, creationOptions, fileDtype, scalefactor,
        offset, rampInfile, ext):
    """
    Run a set of stats tests for the given drive and datatype.
    """
    # The default behaviour
    ok = runOneTest(driverName, creationOptions, fileDtype, scalefactor,
        offset, rampInfile, ext, False, None, False)

    # With thematic output
    if fileDtype not in (hugeIntGDALTypes + floatGDALTypes):
        ok = runOneTest(driverName, creationOptions, fileDtype, scalefactor,
            offset, rampInfile, ext, False, None, True)

    # Force single-pass, with thematic output
    if fileDtype not in (hugeIntGDALTypes + floatGDALTypes):
        ok = ok and runOneTest(driverName, creationOptions, fileDtype,
            scalefactor, offset, rampInfile, ext, False, True, True)

    # Force GDAL pyramids/stats/histogram
    ok = ok and runOneTest(driverName, creationOptions, fileDtype, scalefactor,
        offset, rampInfile, ext, False, False, False)

    # With GDAL, and thematic output
    if fileDtype not in (hugeIntGDALTypes + floatGDALTypes):
        ok = ok and runOneTest(driverName, creationOptions, fileDtype, scalefactor,
            offset, rampInfile, ext, False, False, True)

    return ok    


def runOneTest(driverName, creationOptions, fileDtype, scalefactor, offset,
        rampInfile, ext, omit, singlePass, thematic, noNull=False):
    """
    Run a full test of stats and histogram for the given configuration
    """
    ok = True

    # A random null value, so we don't rely on it being zero.
    nullVal = 52 * scalefactor
    if noNull:
        nullVal = None

    iterationName = "{} {} scale={} omit={} singlePass={} thematic={}".format(
        driverName, gdal.GetDataTypeName(fileDtype), scalefactor,
        omit, singlePass, thematic)

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    otherargs = applier.OtherInputs()

    infiles.inimg = rampInfile
    
    arrDtype = GDALTypeCodeToNumericTypeCode(fileDtype)
    outfiles.outimg = 'test.' + ext

    otherargs.scale = scalefactor
    otherargs.offset = offset
    otherargs.nullval = nullVal
    otherargs.dtype = arrDtype
    controls.setOutputDriverName(driverName)
    controls.setThematic(thematic)
    controls.setStatsIgnore(nullVal)
    controls.setOmitPyramids(omit)
    controls.setOmitBasicStats(omit)
    controls.setOmitHistogram(omit)
    controls.setSinglePassPyramids(singlePass)
    controls.setSinglePassBasicStats(singlePass)
    controls.setSinglePassHistogram(singlePass)

    try:
        rtn = applier.apply(doit, infiles, outfiles, otherargs,
            controls=controls)
    except Exception as e:
        tbStr = ''.join(traceback.format_exception(e))
        msg = f"Exception raised\n{iterationName}\n{tbStr}"
        riostestutils.report(TESTNAME, msg)
        rtn = None
        ok = False

    if rtn is not None:
        # Check whether we actually did single-pass when supposed to
        singlePassMgr = rtn.singlePassMgr
        symbolicName = 'outimg'
        if (singlePassMgr.directPyramidsSupported[symbolicName] and
                (singlePass is True) and
                not singlePassMgr.doSinglePassPyramids(symbolicName)):
            ok = False
            msg = f"Iteration={iterationName}\nSingle-pass requested, but not done for pyramids"
            riostestutils.report(TESTNAME, msg)

        if (singlePass is True and
                not singlePassMgr.doSinglePassStatistics(symbolicName)):
            ok = False
            msg = f"Iteration={iterationName}\nSingle-pass requested, but not done for basic stats"
            riostestutils.report(TESTNAME, msg)

        if (singlePass is True and
                not singlePassMgr.doSinglePassHistogram(symbolicName)):
            ok = False
            msg = f"Iteration={iterationName}\nSingle-pass requested, but not done for histogram"
            riostestutils.report(TESTNAME, msg)

        # Read back the written data as a numpy array
        ds = gdal.Open(outfiles.outimg)
        band = ds.GetRasterBand(1)
        outarr = band.ReadAsArray()

        # Get stats from file, and from array, and compare
        stats1 = getStatsFromBand(band)
        if stats1 is not None:
            stats2 = getStatsFromArray(outarr, nullVal)

            # This relative tolerance is used for comparing the median and mode,
            # because those are approximate only, and the likely error depends on the
            # size of the numbers in question (thus it depends on the scalefactor).
            # Please do not make it any larger unless you have a really solid reason.
            relativeTolerance = 0.3 * scalefactor
            statsOK = compareStats(stats1, stats2, iterationName, relativeTolerance)
            ok = ok and statsOK
        elif not omit:
            msg = "Stats missing, even though not omitting them"
            riostestutils.report(TESTNAME,
                'Iteration={}\n{}'.format(iterationName, msg))
            ok = False

        if omit and stats1 is not None:
            msg = "Stats present, even though directed to omit"
            riostestutils.report(TESTNAME,
                'Iteration={}\n{}'.format(iterationName, msg))
            ok = False

        if not omit:
            histOK = checkHistogram(band, outarr, nullVal, iterationName)
            ok = ok and histOK

        del ds

    if os.path.exists(outfiles.outimg):
        riostestutils.removeRasterFile(outfiles.outimg)

    return ok


def doit(info, inputs, outputs, otherargs):
    """
    Called from RIOS.

    Re-write the input, with scaling and change of datatype
    """
    dtype = otherargs.dtype
    if otherargs.nullval is not None:
        nullmask = (inputs.inimg == otherargs.nullval)
    outimg = inputs.inimg.astype(dtype) * otherargs.scale + otherargs.offset
    if otherargs.nullval is not None:
        outimg[nullmask] = otherargs.nullval
    outputs.outimg = outimg.astype(dtype)


def getStatsFromBand(band):
    """
    Get statistics from given band object, return Stats instance
    """
    mean = getStatsFloatVal(band, 'STATISTICS_MEAN')
    stddev = getStatsFloatVal(band, 'STATISTICS_STDDEV')
    minval = getStatsFloatVal(band, 'STATISTICS_MINIMUM')
    maxval = getStatsFloatVal(band, 'STATISTICS_MAXIMUM')
    median = getStatsFloatVal(band, 'STATISTICS_MEDIAN')
    mode = getStatsFloatVal(band, 'STATISTICS_MODE')
    if None not in (mean, stddev, minval, maxval, median, mode):
        statsObj = Stats(mean, stddev, minval, maxval, median, mode)
    else:
        statsObj = None
    return statsObj


def getStatsFloatVal(band, metadataName):
    """
    Get a single float value from the band metadata, or None if
    it is not present
    """
    valStr = band.GetMetadataItem(metadataName)
    if valStr is not None:
        value = float(valStr)
    else:
        value = None
    return value


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
    mode = calcMode(nonNullArr, axis=None)[0][0]
    return Stats(mean, stddev, minval, maxval, median, mode)


def equalTol(a, b, tol):
    """
    Compare two values to within a tolerance. If the difference
    between the two values is smaller than the tolerance, 
    then return True
    """
    diff = abs(a - b)
    return (diff < tol)


def calcMode(a, axis=0):
    """
    Copied directly from scipy.stats.mode(), so as not to have a dependency on scipy. 
    """
    def _chk_asarray(a, axis):
        "Also copied from scipy.stats, and inserted into this function. "
        if axis is None:
            a = numpy.ravel(a)
            outaxis = 0
        else:
            a = numpy.asarray(a)
            outaxis = axis

        if a.ndim == 0:
            a = numpy.atleast_1d(a)

        return a, outaxis
        
    a, axis = _chk_asarray(a, axis)
    if a.size == 0:
        return numpy.array([]), numpy.array([])

    scores = numpy.unique(numpy.ravel(a))       # get ALL unique values
    testshape = list(a.shape)
    testshape[axis] = 1
    oldmostfreq = numpy.zeros(testshape, dtype=a.dtype)
    oldcounts = numpy.zeros(testshape, dtype=int)
    for score in scores:
        template = (a == score)
        counts = numpy.expand_dims(numpy.sum(template, axis), axis)
        mostfrequent = numpy.where(counts > oldcounts, score, oldmostfreq)
        oldcounts = numpy.maximum(counts, oldcounts)
        oldmostfreq = mostfrequent

    return (mostfrequent, oldcounts)


class Stats(object):
    def __init__(self, mean, stddev, minval, maxval, median, mode):
        self.mean = mean
        self.stddev = stddev
        self.minval = minval
        self.maxval = maxval
        self.median = median
        self.mode = mode
    
    def __str__(self):
        return ' '.join(['%s:%s'%(n, repr(getattr(self, n)))
            for n in ['mean', 'stddev', 'minval', 'maxval', 'median', 'mode']])


def compareStats(stats1, stats2, iterationName, relativeTolerance):
    """
    Compare two Stats instances, and report differences. Also
    return True if all OK. 
    """
    ok = True
    msgList = []
    absoluteTolerance = 0.000001
    for statsName in ['mean', 'stddev', 'minval', 'maxval', 'median', 'mode']:
        value1 = getattr(stats1, statsName)
        value2 = getattr(stats2, statsName)
        
        tol = absoluteTolerance
        if statsName in ['median', 'mode']:
            tol = relativeTolerance
        if not equalTol(value1, value2, tol):
            msgList.append("Error in %s: %s (from file) != %s (from array)" % 
                (statsName, repr(value1), repr(value2)))
    
    if len(msgList) > 0:
        ok = False
        riostestutils.report(TESTNAME, 
            'Iteration=%s\n%s'%(iterationName, '\n'.join(msgList)))
    return ok


def checkHistogram(band, imgArr, nullVal, iterationName):
    """
    Do simple check(s) on the histogram
    """
    histValsStr = None
    metadataDict = band.GetMetadata()
    if "STATISTICS_HISTOBINVALUES" in metadataDict:
        histValsStr = metadataDict["STATISTICS_HISTOBINVALUES"]
    if histValsStr is not None:
        if histValsStr[-1] == '|':
            # Remove trailing '|'
            histValsStr = histValsStr[:-1]
        histVals = numpy.array([int(v) for v in histValsStr.split('|')])
    else:
        # Must be KEA, so we have to use the RAT to read the histogram.
        tbl = band.GetDefaultRAT()
        (histVals, histColNdx) = (None, None)
        for i in range(tbl.GetColumnCount()):
            if tbl.GetNameOfCol(i) == "Histogram":
                histColNdx = i
        if histColNdx is not None:
            histVals = tbl.ReadAsArray(histColNdx)

    ok = True
    msgList = []
    if histVals is not None:
        totalCount = histVals.sum()
        trueTotalCount = numpy.count_nonzero(imgArr != nullVal)
        if totalCount != trueTotalCount:
            ok = False
            msgList.append("Histogram total count error: {} != {}".format(totalCount, trueTotalCount))

        # Test the individual counts
        imgArrNonNull = imgArr[imgArr != nullVal]
        histMin = float(band.GetMetadataItem("STATISTICS_HISTOMIN"))
        histMax = float(band.GetMetadataItem("STATISTICS_HISTOMAX"))
        (trueHist, bin_edges) = numpy.histogram(imgArrNonNull,
            bins=len(histVals), range=(histMin, histMax))
        # For the test cases, it appears that we always get exactly the same
        # histogram counts. This feels unexpectedly lucky, but it
        # makes the following test possible
        mismatch = (histVals != trueHist)
        if mismatch.any():
            ok = False
            numMismatch = numpy.count_nonzero(mismatch)
            msg = "Histogram mis-match for {} values".format(numMismatch)
            msgList.append(msg)
    else:
        ok = False
        msgList.append("Histogram not found, so could not be checked")

    if not ok:
        msg = 'Iteration={}\n{}'.format(iterationName, '\n'.join(msgList))
        riostestutils.report(TESTNAME, msg)

    return ok


def testAllNull():
    """
    Write an output file which is all nulls, and check that the stats and
    histogram behave appropriately.
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()
    otherargs = applier.OtherInputs()

    infiles.inimg = "empty.img"
    outfiles.outimg = "empty2.tif"
    nullval = 27
    otherargs.nullval = nullval
    controls.setOutputDriverName("GTiff")
    controls.setStatsIgnore(nullval)

    ds = riostestutils.createTestFile(infiles.inimg)
    nRows = ds.RasterYSize
    nCols = ds.RasterXSize
    arr = numpy.full((nRows, nCols), nullval, dtype=numpy.uint8)
    ds.GetRasterBand(1).WriteArray(arr)
    del ds

    ok = True
    # These keys should all not be present in the resulting metadata
    keysToCheck = [
        'STATISTICS_MEAN',
        'STATISTICS_STDDEV',
        'STATISTICS_MINIMUM',
        'STATISTICS_MAXIMUM',
        'STATISTICS_MEDIAN',
        'STATISTICS_MODE',
        'STATISTICS_HISTOMIN',
        'STATISTICS_HISTOMAX',
        'STATISTICS_HISTOBINVALUES',
        'STATISTICS_HISTONUMBINS'
    ]

    for singlePass in [True, False]:
        controls.setSinglePassBasicStats(singlePass)
        controls.setSinglePassHistogram(singlePass)
        applier.apply(doAllNull, infiles, outfiles, otherargs,
            controls=controls)

        ds = gdal.Open(outfiles.outimg)
        band = ds.GetRasterBand(1)
        md = band.GetMetadata()
        for k in keysToCheck:
            if k in md:
                msg = ("Found statistics item '{}', even though output is " +
                       "all nulls").format(k)
                riostestutils.report(TESTNAME, msg)
                ok = False
        del ds

    for fn in [infiles.inimg, outfiles.outimg]:
        if os.path.exists(fn):
            riostestutils.removeRasterFile(fn)

    return ok


def doAllNull(info, inputs, outputs, otherargs):
    """
    Called from RIOS. Write an output which is all nulls
    """
    shape = inputs.inimg.shape
    dtype = inputs.inimg.dtype
    nullval = otherargs.nullval
    outputs.outimg = numpy.full(shape, nullval, dtype=dtype)


if __name__ == "__main__":
    run()
