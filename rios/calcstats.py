"""
This module creates pyramid layers and calculates statistics for image
files. Much of it was originally for ERDAS Imagine files but should work 
with any other format that supports pyramid layers and statistics

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
try:
    from numba import njit
    haveNumba = True
except ImportError:
    # Define a dummy njit decorator
    def njit(func, *args, **kwargs):
        return func
    haveNumba = False
from . import cuiprogress
from .rioserrors import ProcessCancelledError, SinglePassActionsError

# When calculating overviews (i.e. pyramid layers), default behaviour
# is controlled by these
dfltOverviewLvls = os.getenv('RIOS_DFLT_OVERVIEWLEVELS')
if dfltOverviewLvls is None:
    DEFAULT_OVERVIEWLEVELS = [4, 8, 16, 32, 64, 128, 256, 512]
else:
    DEFAULT_OVERVIEWLEVELS = [int(i) for i in dfltOverviewLvls.split(',')]
DEFAULT_MINOVERVIEWDIM = int(os.getenv('RIOS_DFLT_MINOVERLEVELDIM', default=128))
DEFAULT_OVERVIEWAGGREGRATIONTYPE = os.getenv('RIOS_DFLT_OVERVIEWAGGTYPE', 
    default="NEAREST")


def progressFunc(value, string, userdata):
    """
    Progress callback for BuildOverviews
    """
    percent = (userdata.curroffset + (value / userdata.nbands) * 100)
    userdata.progress.setProgress(percent)
    if value == 1.0:
        userdata.curroffset = userdata.curroffset + 100.0 / userdata.nbands
    return not userdata.progress.wasCancelled()


# make userdata object with progress and num bands
class ProgressUserData(object):
    pass


def addPyramid(ds, progress, 
        minoverviewdim=DEFAULT_MINOVERVIEWDIM, 
        levels=DEFAULT_OVERVIEWLEVELS,
        aggregationType=None):
    """
    Adds Pyramid layers to the dataset. Adds levels until
    the raster dimension of the overview layer is < minoverviewdim,
    up to a maximum level controlled by the levels parameter. 
    
    Uses gdal.Dataset.BuildOverviews() to do the work. 
    
    """
    progress.setLabelText("Computing Pyramid Layers...")
    progress.setProgress(0)
    
    # ensure everything is written to disc first
    ds.FlushCache()

    # first we work out how many overviews to build based on the size
    mindim = min(ds.RasterXSize, ds.RasterYSize)
    
    nOverviews = 0
    for i in levels:
        if (mindim // i) > minoverviewdim:
            nOverviews = nOverviews + 1

    # Need to find out if we are thematic or continuous. 
    tmpmeta = ds.GetRasterBand(1).GetMetadata()
    if aggregationType is None:
        if 'LAYER_TYPE' in tmpmeta:
            if tmpmeta['LAYER_TYPE'] == 'athematic':
                aggregationType = "AVERAGE"
            else:
                aggregationType = "NEAREST"
        else:
            aggregationType = DEFAULT_OVERVIEWAGGREGRATIONTYPE
    
    userdata = ProgressUserData()
    userdata.progress = progress
    userdata.nbands = ds.RasterCount
    userdata.curroffset = 0

    ds.BuildOverviews(aggregationType, levels[:nOverviews], progressFunc, userdata)
  
    if progress.wasCancelled():
        raise ProcessCancelledError()

    # make sure it goes to 100%
    progress.setProgress(100)


def findOrCreateColumn(ratObj, usage, name, dtype):
    """
    Returns the index of an existing column matched
    on usage. Creates it if not already existing using 
    the supplied name and dtype
    Returns a tupe with index and a boolean specifying if 
    it is a new column or not
    """
    ncols = ratObj.GetColumnCount()
    for col in range(ncols):
        if ratObj.GetUsageOfCol(col) == usage:
            return col, False

    # got here so can't exist
    ratObj.CreateColumn(name, dtype, usage)
    # new one will be last col
    return ncols, True


gdalLargeIntTypes = set([gdal.GDT_Int16, gdal.GDT_UInt16, gdal.GDT_Int32, gdal.GDT_UInt32])
# hack for GDAL 3.5 and later which suppport 64 bit ints
if hasattr(gdal, 'GDT_Int64'):
    gdalLargeIntTypes.add(gdal.GDT_Int64)
    gdalLargeIntTypes.add(gdal.GDT_UInt64)

gdalFloatTypes = set([gdal.GDT_Float32, gdal.GDT_Float64])


def addStatistics(ds, progress, ignore=None, approx_ok=False):
    """
    Calculates statistics and adds them to the image
    
    Uses gdal.Band.ComputeStatistics() for mean, stddev, min and max,
    and gdal.Band.GetHistogram() to do histogram calculation. 
    The median and mode are estimated using the histogram, and so 
    for larger datatypes, they will be approximate only. 
    
    For thematic layers, the histogram is calculated with as many bins 
    as required, for athematic integer and float types, a maximum
    of 256 bins is used.

    Note that this routine will use the given ignore value to set the
    no-data value (i.e. null value) on the dataset, using the same value
    for every band.

    """
    minMaxList = addBasicStatsGDAL(ds, approx_ok)
    addHistogramsGDAL(ds, minMaxList, approx_ok)


def addBasicStatsGDAL(ds, approx_ok):
    """
    Add basic statistics to all bands of the given Dataset, using
    GDAL's function. Passes approx_ok through to that.

    Return a list of the minimum and maximum values for each band, in case
    this is required later for the histogram.

    """
    minMaxList = []
    for bandndx in range(ds.RasterCount):
        band = ds.GetRasterBand(bandndx + 1)

        (minval, maxval, meanval, stddev) = computeStatsGDAL(band, approx_ok)
        if None not in (minval, maxval, meanval, stddev):
            writeBasicStats(band, minval, maxval, meanval, stddev, approx_ok)
        minMaxList.append((minval, maxval))
    return minMaxList


def addHistogramsGDAL(ds, minMaxList, approx_ok):
    """
    Add histograms to all bands of the given Dataset, using GDAL's own
    function. Passes approx_ok through to that. The minMaxList is as returned
    by addBasicStatsGDAL.

    """
    for bandndx in range(ds.RasterCount):
        band = ds.GetRasterBand(bandndx + 1)
        (minval, maxval) = minMaxList[bandndx]
        histParams = HistogramParams(band, minval, maxval)

        # Get histogram and force GDAL to recalculate it. Note that we use include_out_of_range=True,
        # which is safe because we have calculated the histCalcMin/Max from the data. 
        includeOutOfRange = True
        hist = band.GetHistogram(histParams.calcMin,
                    histParams.calcMax, histParams.nbins,
                    includeOutOfRange, approx_ok)
        # comes back as a list for some reason
        hist = numpy.array(hist)

        # Check if GDAL's histogram code overflowed. This is not a fool-proof
        # test, as some overflows will not result in negative counts. Since
        # GDAL 3.x, it is no longer required, as counts are int64.
        histogramOverflow = (hist.min() < 0)
        
        if not histogramOverflow:
            writeHistogram(ds, band, hist, histParams)


def computeStatsGDAL(band, approx_ok):
    """
    Compute basic statistics of a single band, using GDAL's function.
    Returns the values as a tuple (does NOT write anything into the file).

    If there are no non-null pixels, then all stats are returned as None.

    Returns (minval, maxval, mean, stddev)

    """
    # get GDAL to calculate statistics - force recalculation. Trap errors
    usingExceptions = gdal.GetUseExceptions()
    gdal.UseExceptions()
    try:
        (minval, maxval, meanval, stddev) = band.ComputeStatistics(approx_ok)
    except RuntimeError as e:
        if str(e).endswith('Failed to compute statistics, no valid pixels found in sampling.'):
            minval = maxval = meanval = stddev = None
        else:
            raise e
    finally:
        if not usingExceptions:
            gdal.DontUseExceptions()

    return (minval, maxval, meanval, stddev)


def writeBasicStats(band, minval, maxval, meanval, stddev, approx_ok):
    """
    Write the given basic statistics into the given band.

    It is assumed that by this point, we have set the null value on the band
    (this is normally done when the file is opened).
    """
    band.SetStatistics(float(minval), float(maxval), meanval, stddev)

    nullval = band.GetNoDataValue()
    if nullval is not None:
        # Not sure, but I think this is only used by QGIS
        band.SetMetadataItem("STATISTICS_EXCLUDEDVALUES", repr(nullval))

    # I think that mainly the HFA format makes use of these (not sure).
    if approx_ok:
        band.SetMetadataItem("STATISTICS_APPROXIMATE", "YES")
    else:
        band.SetMetadataItem("STATISTICS_SKIPFACTORX", "1")
        band.SetMetadataItem("STATISTICS_SKIPFACTORY", "1")


class HistogramParams:
    """
    Work out the various parameters needed by GDAL to compute a histogram.
    The inferences are based on the pixel datatype.
    """
    def __init__(self, band, minval, maxval):
        self.min = None
        self.max = None
        self.step = None
        self.calcMin = None
        self.calcMax = None
        self.nbins = None
        self.binFunction = None

        layerType = band.GetMetadataItem('LAYER_TYPE')
        thematic = (layerType == "thematic")

        # Note that we explicitly set histstep in each datatype case.
        # In principle, this can be calculated, as it is done in the
        # float and large-int cases, but for some of the others we need
        # it to be exactly equal to 1, so we set it explicitly here, to
        # avoid rounding error problems.

        if band.DataType == gdal.GDT_Byte:
            # if byte data use 256 bins and the whole range
            self.min = 0
            self.max = 255
            self.step = 1.0
            self.calcMin = -0.5
            self.calcMax = 255.5
            self.nbins = 256
            self.binFunction = 'direct'
        elif thematic:
            # all other thematic types a bin per value
            self.min = int(numpy.floor(minval))
            self.max = int(numpy.ceil(maxval))
            self.step = 1.0
            self.calcMin = -0.5
            self.calcMax = maxval + 0.5
            self.nbins = (self.max - self.min + 1)
            self.binFunction = 'direct'
        elif band.DataType in gdalLargeIntTypes:
            histrange = int(numpy.ceil(maxval) - numpy.floor(minval)) + 1
            (self.min, self.max) = (minval, maxval)
            if histrange <= 256:
                self.nbins = histrange
                self.step = 1.0
                self.binFunction = 'direct'
                self.calcMin = self.min - 0.5
                self.calcMax = self.max + 0.5
            else:
                self.nbins = 256
                self.binFunction = 'linear'
                self.calcMin = self.min
                self.calcMax = self.max
                self.step = float(self.calcMax - self.calcMin) / self.nbins
        elif band.DataType in gdalFloatTypes:
            self.nbins = 256
            (self.min, self.max) = (minval, maxval)
            self.binFunction = 'linear'
            self.calcMin = minval
            self.calcMax = maxval
            if self.calcMin == self.calcMax:
                self.calcMax = self.calcMax + 0.5
                self.nbins = 1
            self.step = float(self.calcMax - self.calcMin) / self.nbins


def calcStats(ds, progress=None, ignore=None,
        minoverviewdim=DEFAULT_MINOVERVIEWDIM, 
        levels=DEFAULT_OVERVIEWLEVELS,
        aggregationType=None, approx_ok=False):
    """
    Does both the stats and pyramid layers. Calls addPyramid()
    and addStatistics() functions. See their docstrings for details. 
    
    """
    if progress is None:
        progress = cuiprogress.SilentProgress()
    
    if ignore is not None:
        setNullValue(ds, ignore)

    addPyramid(ds, progress, minoverviewdim=minoverviewdim, levels=levels, 
        aggregationType=aggregationType)

    addStatistics(ds, progress, ignore, approx_ok=approx_ok)


def setNullValue(ds, nullValue):
    """
    Set the given null value on all bands of the given Dataset
    """
    for i in range(ds.RasterCount):
        band = ds.GetRasterBand(i + 1)
        band.SetNoDataValue(nullValue)


class SinglePassManager:
    """
    The required info for dealing with single-pass pyramids/statistics/histogram.
    There is some complexity here, because the decisions about what to do are
    a result of a number of different factors. We attempt to make these decisions
    as early as possible, and store the decisions on this object, so they can
    just be checked later.

    The general intention is that wherever possible, the pyramids, basic
    statistics, and histogram, will all be done with the single-pass methods.
    When this is not possible, or has been explicitly disabled, then it will
    fall back to using GDAL's methods, after the whole raster has been written.
    """
    def __init__(self, outfiles, controls, workinggrid, tmpfileMgr):
        """
        Check whether single-pass is appropriate and/or supported for
        all output files.
        """
        self.PYRAMIDS = 0
        self.STATISTICS = 1
        self.HISTOGRAM = 2
        self.histSupportedDtypes = (numpy.uint8, numpy.int16, numpy.uint16)
        self.supportedAggtypes = ("NEAREST", )

        self.omit = {}
        self.singlePassRequested = {}
        self.approxOK = {}
        self.overviewLevels = {}
        self.oviewAggtype = {}
        self.arrDtype = {}
        self.accumulators = {}
        self.pyramidsSupported = {}

        (nrows, ncols) = workinggrid.getDimensions()
        mindim = min(nrows, ncols)
        driverSupportsPyramids = self.checkDriverPyramidSupport(outfiles,
            controls, tmpfileMgr)

        for (symbolicName, seqNum, filename) in outfiles:
            # Store all the relevant settings from the controls object,
            # in a form which is a bit easier to query.
            # (These are all the same for all seqNum values, and unnecessarily
            # reset each time for the same symbolicName. Sorry.)
            self.omit[symbolicName, self.PYRAMIDS] = (
                controls.getOptionForImagename('omitPyramids', symbolicName))
            self.singlePassRequested[symbolicName, self.PYRAMIDS] = (
                controls.getOptionForImagename('singlePassPyramids', symbolicName))
            self.omit[symbolicName, self.STATISTICS] = (
                controls.getOptionForImagename('omitBasicStats', symbolicName))
            self.singlePassRequested[symbolicName, self.STATISTICS] = (
                controls.getOptionForImagename('singlePassBasicStats', symbolicName))
            self.omit[symbolicName, self.HISTOGRAM] = (
                controls.getOptionForImagename('omitHistogram', symbolicName))
            self.singlePassRequested[symbolicName, self.HISTOGRAM] = (
                controls.getOptionForImagename('singlePassHistogram', symbolicName))

            driverName = controls.getOptionForImagename('drivername',
                symbolicName)
            self.pyramidsSupported[symbolicName] = driverSupportsPyramids[driverName]

            self.approxOK[symbolicName] = controls.getOptionForImagename(
                'approxStats', symbolicName)
            oviewLvls = controls.getOptionForImagename('overviewLevels',
                symbolicName)
            aggType = controls.getOptionForImagename(
                'overviewAggType', symbolicName)
            if aggType is None:
                aggType = "NEAREST"
            self.oviewAggtype[symbolicName] = aggType
            minOverviewDim = controls.getOptionForImagename(
                'overviewMinDim', symbolicName)
            nOverviews = 0
            for lvl in oviewLvls:
                if (mindim // lvl) > minOverviewDim:
                    nOverviews += 1
            self.overviewLevels[symbolicName] = oviewLvls[:nOverviews]

    def checkDriverPyramidSupport(self, outfiles, controls, tmpfileMgr):
        """
        For all the format drivers being used for output, check whether they
        support direct writing of pyramid layers. Return a dictionary keyed
        by driver name, with boolean values.
        """
        driverSupportsPyramids = {}
        nrows = ncols = 64
        fillVal = 20
        for (symbolicName, seqNum, filename) in outfiles:
            driverName = controls.getOptionForImagename(
                'drivername', symbolicName)
            if driverName not in driverSupportsPyramids:
                drvr = gdal.GetDriverByName(driverName)
                suffix = ".{}".format(drvr.GetMetadataItem('DMD_EXTENSION'))

                # Create a small test image with a single overview level,
                # written directly.
                imgfile = tmpfileMgr.mktempfile(prefix='pyrcheck_',
                    suffix=suffix)
                arr = numpy.full((nrows, ncols), fillVal, dtype=numpy.uint8)
                ds = drvr.Create(imgfile, ncols, nrows, 1, gdal.GDT_Byte)
                band = ds.GetRasterBand(1)
                ds.BuildOverviews(overviewlist=[2])
                band.WriteArray(arr)
                band_ov = band.GetOverview(0)
                arr_ov = arr[::2, ::2]
                band_ov.WriteArray(arr_ov)
                del band_ov, band, ds

                # Now read back the overview array
                ds = gdal.Open(imgfile)
                band = ds.GetRasterBand(1)
                band_ov = band.GetOverview(0)
                arr_sub2 = band_ov.ReadAsArray()
                del band_ov, band, ds
                drvr.Delete(imgfile)

                # If the overview array is full of the fill value, then it works
                supported = (arr_sub2 == fillVal).all()
                driverSupportsPyramids[driverName] = supported
        return driverSupportsPyramids

    def initFor(self, ds, symbolicName, seqNum, arr):
        """
        Initialise for the given output file
        """
        includeStats = self.doSinglePassStatistics(symbolicName)
        self.arrDtype[symbolicName] = arr.dtype
        includeHist = self.doSinglePassHistogram(symbolicName)
        if includeStats or includeHist:
            nullval = ds.GetRasterBand(1).GetNoDataValue()
            key = (symbolicName, seqNum)
            numBands = arr.shape[0]
            self.accumulators[key] = [
                SinglePassAccumulator(includeStats, includeHist,
                        arr.dtype, nullval)
                for i in range(numBands)
            ]
        if self.doSinglePassPyramids(symbolicName):
            aggType = self.oviewAggtype[symbolicName]
            ds.BuildOverviews(aggType, self.overviewLevels[symbolicName])

    def doSinglePassPyramids(self, symbolicName):
        """
        Return True if we should do single-pass pyramids layers, False
        otherwise. Decision depends on choices for omitPyramids,
        singlePassPyramids, and overviewAggType.

        """
        key = (symbolicName, self.PYRAMIDS)
        omit = self.omit[key]
        supported = self.pyramidsSupported[symbolicName]
        spReq = self.singlePassRequested[key]
        aggType = self.oviewAggtype[symbolicName]
        if spReq is True and aggType not in self.supportedAggtypes:
            msg = ("Single-pass pyramids explicitly requested, but " +
               "not supported for aggregationType '{}'").format(
                   aggType)
            raise SinglePassActionsError(msg)

        spPyr = ((spReq is True or spReq is None) and (not omit) and
            supported and (aggType in self.supportedAggtypes))
        return spPyr

    def doSinglePassStatistics(self, symbolicName):
        """
        Return True if we should do single-pass basic statistics, False
        otherwise.
        """
        key = (symbolicName, self.STATISTICS)
        omit = self.omit[key]
        spReq = self.singlePassRequested[key]
        approxOK = self.approxOK[symbolicName]
        spStats = ((spReq is True or spReq is None) and
                not (omit or approxOK))
        return spStats

    def doSinglePassHistogram(self, symbolicName):
        """
        Return True if we should do single-pass histogram, False
        otherwise, based on what has been requested, the datatype of
        the raster, and the availability of numba.
        """
        key = (symbolicName, self.HISTOGRAM)
        omit = self.omit[key]
        spReq = self.singlePassRequested[key]
        approxOK = self.approxOK[symbolicName]
        if symbolicName not in self.arrDtype:
            msg = ("doSinglePassHistogram({name}) has been called " +
                   "before initFor({name}, ...)").format(name=symbolicName)
            raise SinglePassActionsError(msg)
        dtype = self.arrDtype[symbolicName]
        dtypeSupported = (dtype in self.histSupportedDtypes)

        # Here we distinguish between spReq being True or None. If it
        # is None, then we will settle on some suitable default behaviour,
        # depending on other conditions, but if it is explicitly True,
        # then we must have the required conditions, or raise an
        # exception to explain why it will not be done.
        if spReq is True and not dtypeSupported:
            msg = ("Explicitly requested single-pass histogram, but " +
                   "this is not supported for datatype {}".format(dtype))
            raise SinglePassActionsError(msg)
        if spReq is True and not haveNumba:
            msg = ("Explicitly requested single-pass histogram, but " +
                   "the numba package is not available")
            raise SinglePassActionsError(msg)

        spHist = ((spReq is True or spReq is None) and
                  dtypeSupported and haveNumba and
                  not (omit or approxOK))
        return spHist


class SinglePassAccumulator:
    """
    Accumulator for statistics and histogram for a single band. Used when
    doing single-pass stats and/or histogram.
    """
    def __init__(self, includeStats, includeHist, dtype, nullval):
        self.nullval = nullval
        self.histNullval = nullval
        if nullval is None and includeHist:
            # We can't use None in the njit-ed histogram code, so make a
            # value that would be impossible, given the supported datatypes
            self.histNullval = numpy.uint32(2**32 - 1)
        self.includeStats = includeStats
        self.includeHist = includeHist
        if includeStats:
            self.minval = None
            self.maxval = None
            self.sum = 0
            self.ssq = 0
            self.count = 0
        if includeHist:
            # We only do single-pass histograms if we are also
            # doing direct binning histograms.
            self.binFunc = "direct"

            # Set the outer possible limits of the histogram. These
            # are used only internally, during accumulation, and are
            # superceded later by the histLimits() method.
            if dtype == numpy.uint8:
                self.histmin = 0
                self.nbins = 256
            elif dtype in (numpy.int16, numpy.uint16):
                if dtype == numpy.uint16:
                    self.histmin = 0
                else:
                    self.histmin = -(2**15)
                self.nbins = 2**16
            self.hist = numpy.zeros(self.nbins, dtype=numpy.uint64)

    def doStatsAccum(self, arr):
        """
        Accumulate basic stats for the given array
        """
        if self.nullval is None:
            values = arr.flatten()
        else:
            values = arr[arr != self.nullval]
        if len(values) > 0:
            self.sum += values.sum()
            self.ssq += (values.astype(numpy.float32)**2).sum()
            self.count += values.size
            minval = values.min()
            if self.minval is None or minval < self.minval:
                self.minval = minval
            maxval = values.max()
            if self.maxval is None or maxval > self.maxval:
                self.maxval = maxval

    def finalStats(self):
        """
        Return the final values of the four basic statistics
        (minval, maxval, mean, stddev)
        """
        meanval = None
        stddev = None
        if self.count > 0:
            meanval = self.sum / self.count
            variance = self.ssq / self.count - meanval ** 2
            stddev = 0.0
            # In case some rounding error made variance negative
            if variance >= 0:
                stddev = numpy.sqrt(variance)

        return (self.minval, self.maxval, meanval, stddev)

    def histLimits(self):
        """
        Return the values which describe the limits of the histogram, 
        i.e. the lowest and highest values with non-zero counts
        """
        nonzeroNdx = numpy.where(self.hist > 0)[0]
        if len(nonzeroNdx) > 0:
            first = nonzeroNdx[0]
            last = nonzeroNdx[-1]
            minval = self.histmin + first
            maxval = self.histmin + last
            nbins = last - first + 1
        else:
            minval = maxval = first = last = nbins = None
        return (minval, maxval, first, last, nbins)


def handleSinglePassActions(ds, arr, singlePassMgr, symbolicName, seqNum,
        xOff, yOff, timings):
    """
    Called from writeBlock, to handle the single-pass actions which may
    or may not be required.
    """
    numBands = arr.shape[0]
    if singlePassMgr.doSinglePassPyramids(symbolicName):
        with timings.interval('pyramids'):
            writeBlockPyramids(ds, arr, singlePassMgr, symbolicName, xOff, yOff)
    if singlePassMgr.doSinglePassStatistics(symbolicName):
        with timings.interval('basicstats'):
            accumList = singlePassMgr.accumulators[symbolicName, seqNum]
            for i in range(numBands):
                accumList[i].doStatsAccum(arr[i])
    if singlePassMgr.doSinglePassHistogram(symbolicName):
        with timings.interval('histogram'):
            accumList = singlePassMgr.accumulators[symbolicName, seqNum]
            for i in range(numBands):
                accum = accumList[i]
                singlePassHistAccum(arr[i], accum.hist, accum.histNullval,
                    accum.histmin, accum.nbins)


def writeBlockPyramids(ds, arr, singlePassMgr, symbolicName, xOff, yOff):
    """
    Calculate and write out the pyramid layers for all bands of the block
    given as arr. Called when doing single-pass pyramid layers.

    """
    overviewLevels = singlePassMgr.overviewLevels[symbolicName]
    nOverviews = len(overviewLevels)

    numBands = arr.shape[0]
    for i in range(numBands):
        band = ds.GetRasterBand(i + 1)
        for j in range(nOverviews):
            band_ov = band.GetOverview(i)
            lvl = overviewLevels[i]
            # Offset from top-left edge
            o = lvl // 2
            # Sub-sample by taking every lvl-th pixel in each direction
            arr_sub = arr[i, o::lvl, o::lvl]
            # The xOff/yOff of the block within the sub-sampled raster
            xOff_sub = xOff // lvl
            yOff_sub = yOff // lvl
            # The actual number of rows and cols to write, ensuring we
            # do not go off the edges
            nc = band_ov.XSize - xOff_sub
            nr = band_ov.YSize - yOff_sub
            arr_sub = arr_sub[:nr, :nc]
            band_ov.WriteArray(arr_sub, xOff_sub, yOff_sub)


@njit
def singlePassHistAccum(arr, histCounts, nullval, minval, nbins):
    """
    When doing single-pass histogram, accumulate counts for the
    given arr. This function is compiled using numba.njit, so should
    not be passed any unexpected Python objects, just scalars and
    numpy arrays.

    In principle, this would be neater as a method on the
    SinglePassAccumulator class, but with numba involved, it is more
    straightforward to use a separate function.

    """
    for val in arr.flatten():
        if val != nullval:
            ndx = val - minval
            if ndx >= 0 and ndx < nbins:
                histCounts[ndx] += 1


def finishSinglePassStats(ds, singlePassMgr, symbolicName, seqNum):
    """
    Finish the single-pass basic statistics for all bands of the given
    file, and write them into the file.
    """
    accumList = singlePassMgr.accumulators[symbolicName, seqNum]
    numBands = len(accumList)
    for i in range(numBands):
        (minval, maxval, meanval, stddev) = accumList[i].finalStats()
        if None not in (minval, maxval, meanval, stddev):
            band = ds.GetRasterBand(i + 1)
            approx_ok = singlePassMgr.approxOK[symbolicName]
            writeBasicStats(band, minval, maxval, meanval, stddev, approx_ok)


def finishSinglePassHistogram(ds, singlePassMgr, symbolicName, seqNum):
    """
    Finish the histogram
    """
    accumList = singlePassMgr.accumulators[symbolicName, seqNum]
    numBands = len(accumList)
    for i in range(numBands):
        accum = accumList[i]
        (minval, maxval, first, last, nbins) = accum.histLimits()
        if minval is not None:
            band = ds.GetRasterBand(i + 1)
            histParams = HistogramParams(band, minval, maxval)
            hist = accum.hist[first:last + 1]
            writeHistogram(ds, band, hist, histParams)


def writeHistogram(ds, band, hist, histParams):
    """
    Write the given histogram into the band object. Also use the histogram
    to calculate median and mode, and write them as well.
    """
    ratObj = band.GetDefaultRAT()
    # The GDAL HFA driver has a bug in its SetLinearBinning function,
    # which was introduced as part of the RFC40 changes. Until
    # this is fixed and widely distributed, we should disable the use
    # of RFC40-style techniques for HFA files.
    driverName = ds.GetDriver().ShortName
    disableRFC40 = (driverName == 'HFA')

    if ratObj is not None and not disableRFC40:
        histIndx, histNew = findOrCreateColumn(ratObj, gdal.GFU_PixelCount,
                                "Histogram", gdal.GFT_Real)
        # write the hist in a single go
        ratObj.SetRowCount(histParams.nbins)
        ratObj.WriteArray(hist, histIndx)

        ratObj.SetLinearBinning(histParams.min,
            (histParams.calcMax - histParams.calcMin) / histParams.nbins)
    else:
        # Use GDAL's original metadata interface, for drivers which
        # don't support the more modern approach
        band.SetMetadataItem("STATISTICS_HISTOBINVALUES",
            '|'.join(map(str, hist)) + '|')

        band.SetMetadataItem("STATISTICS_HISTOMIN", repr(histParams.min))
        band.SetMetadataItem("STATISTICS_HISTOMAX", repr(histParams.max))
        band.SetMetadataItem("STATISTICS_HISTONUMBINS",
            repr(int(histParams.nbins)))
        band.SetMetadataItem("STATISTICS_HISTOBINFUNCTION",
            histParams.binFunction)

    # estimate the median - bin with the middle number
    middlenum = hist.sum() / 2
    gtmiddle = hist.cumsum() >= middlenum
    medianbin = gtmiddle.nonzero()[0][0]
    medianval = medianbin * histParams.step + histParams.min
    if band.DataType in (gdal.GDT_Float32, gdal.GDT_Float64):
        band.SetMetadataItem("STATISTICS_MEDIAN", repr(float(medianval)))
    else:
        band.SetMetadataItem("STATISTICS_MEDIAN", repr(int(round(medianval))))

    # do the mode - bin with the highest count
    modebin = numpy.argmax(hist)
    modeval = modebin * histParams.step + histParams.min
    if band.DataType == gdal.GDT_Float32 or band.DataType == gdal.GDT_Float64:
        band.SetMetadataItem("STATISTICS_MODE", repr(float(modeval)))
    else:
        band.SetMetadataItem("STATISTICS_MODE", repr(int(round(modeval))))

    if ratObj is not None and not ratObj.ChangesAreWrittenToFile():
        # For drivers that require the in memory thing
        band.SetDefaultRAT(ratObj)
