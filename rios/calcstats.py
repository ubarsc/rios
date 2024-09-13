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
import warnings
import numpy
from osgeo import gdal
try:
    from numba import njit
    haveNumba = True
except ImportError:
    # Define a dummy njit decorator
    # https://stackoverflow.com/questions/57774497/how-do-i-make-a-dummy-do-nothing-jit-decorator
    def njit(f=None, *args, **kwargs):
        def decorator(func):
            return func

        if callable(f):
            return f
        else:
            return decorator
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
DEFAULT_MINOVERVIEWDIM = int(os.getenv('RIOS_DFLT_MINOVERLEVELDIM', default=33))
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
    if ds.RasterXSize < ds.RasterYSize:
        mindim = ds.RasterXSize
    else:
        mindim = ds.RasterYSize
    
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
    progress.setLabelText("Computing Statistics...")
    progress.setProgress(0)
    percent = 0
    percentstep = 100.0 / (ds.RasterCount * 2)  # 2 steps for each layer

    # flush the cache. The ensures that any unwritten data is 
    # written to file so we get the right stats. It also 
    # makes sure any metdata is written on HFA. This means
    # the LAYER_TYPE setting will be picked up by rat.SetLinearBinning()
    ds.FlushCache()

    # The GDAL HFA driver has a bug in its SetLinearBinning function,
    # which was introduced as part of the RFC40 changes. Until
    # this is fixed and widely distributed, we should disable the use
    # of RFC40-style techniques for HFA files.
    driverName = ds.GetDriver().ShortName
    disableRFC40 = (driverName == 'HFA')
  
    for bandnum in range(ds.RasterCount):
        band = ds.GetRasterBand(bandnum + 1)

        # fill in the metadata
        tmpmeta = band.GetMetadata()
    
        if ignore is not None:
            # tell QGIS that the ignore value was ignored
            band.SetNoDataValue(ignore)
            tmpmeta["STATISTICS_EXCLUDEDVALUES"] = repr(ignore)  # doesn't seem to do anything
      
        # get GDAL to calculate statistics - force recalculation. Trap errors 
        usingExceptions = gdal.GetUseExceptions()
        gdal.UseExceptions()
        try:
            if approx_ok and "LAYER_TYPE" in tmpmeta and tmpmeta["LAYER_TYPE"] == "thematic": 
                warnings.warn('WARNING: approx_ok specified for stats but image is thematic (this could be a bad idea)')

            (minval, maxval, meanval, stddevval) = band.ComputeStatistics(approx_ok)
        except RuntimeError as e:
            if str(e).endswith('Failed to compute statistics, no valid pixels found in sampling.'):
                minval = ignore
                maxval = ignore
                meanval = ignore
                stddevval = 0
            else:
                raise e
        finally:
            if not usingExceptions:
                gdal.DontUseExceptions()

        percent = percent + percentstep
        progress.setProgress(percent)
    
        tmpmeta["STATISTICS_MINIMUM"] = repr(minval)
        tmpmeta["STATISTICS_MAXIMUM"] = repr(maxval)
        tmpmeta["STATISTICS_MEAN"] = repr(meanval)
        tmpmeta["STATISTICS_STDDEV"] = repr(stddevval)
        # because we did at full res - these are the default anyway

        if approx_ok:
            tmpmeta["STATISTICS_APPROXIMATE"] = "YES"
        else:
            tmpmeta["STATISTICS_SKIPFACTORX"] = "1"
            tmpmeta["STATISTICS_SKIPFACTORY"] = "1"

        # create a histogram so we can do the mode and median
        if band.DataType == gdal.GDT_Byte:
            # if byte data use 256 bins and the whole range
            histmin = 0
            histmax = 255
            histstep = 1.0
            histCalcMin = -0.5
            histCalcMax = 255.5
            histnbins = 256
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
        elif "LAYER_TYPE" in tmpmeta and tmpmeta["LAYER_TYPE"] == 'thematic':
            # all other thematic types a bin per value
            histmin = 0
            histmax = int(numpy.ceil(maxval))
            histstep = 1.0
            histCalcMin = -0.5
            histCalcMax = maxval + 0.5
            histnbins = histmax + 1
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
        elif band.DataType in gdalLargeIntTypes:
            histrange = int(numpy.ceil(maxval) - numpy.floor(minval)) + 1
            (histmin, histmax) = (minval, maxval)
            if histrange <= 256:
                histnbins = histrange
                histstep = 1.0
                tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
                histCalcMin = histmin - 0.5
                histCalcMax = histmax + 0.5
            else:
                histnbins = 256
                tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'linear'
                histCalcMin = histmin
                histCalcMax = histmax
                histstep = float(histCalcMax - histCalcMin) / histnbins
        elif band.DataType in gdalFloatTypes:
            histnbins = 256
            (histmin, histmax) = (minval, maxval)
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'linear'
            histCalcMin = minval
            histCalcMax = maxval
            if histCalcMin == histCalcMax:
                histCalcMax = histCalcMax + 0.5
                histnbins = 1
            histstep = float(histCalcMax - histCalcMin) / histnbins
        # Note that the complex number data types are not handled, as I am not sure
        # what a histogram or a median would mean for such types. 
      
        userdata = ProgressUserData()
        userdata.progress = progress
        userdata.nbands = ds.RasterCount * 2
        userdata.curroffset = percent
      
        # Get histogram and force GDAL to recalculate it. Note that we use include_out_of_range=True,
        # which is safe because we have calculated the histCalcMin/Max from the data. 
        hist = band.GetHistogram(histCalcMin, histCalcMax, histnbins, True,
                        approx_ok, progressFunc, userdata)
        
        # Check if GDAL's histogram code overflowed. This is not a fool-proof test,
        # as some overflows will not result in negative counts. 
        histogramOverflow = (min(hist) < 0)
        
        # we may use this ratObj reference for the colours below also
        # may be None if format does not support RATs
        ratObj = band.GetDefaultRAT()

        if not histogramOverflow:
            # comes back as a list for some reason
            hist = numpy.array(hist)

            # Note that we have explicitly set histstep in each datatype case 
            # above. In principle, this can be calculated, as it is done in the 
            # float case, but for some of the others we need it to be exactly
            # equal to 1, so we set it explicitly there, to avoid rounding
            # error problems. 

            # do the mode - bin with the highest count
            modebin = numpy.argmax(hist)
            modeval = modebin * histstep + histmin
            if band.DataType == gdal.GDT_Float32 or band.DataType == gdal.GDT_Float64:
                tmpmeta["STATISTICS_MODE"] = repr(float(modeval))
            else:
                tmpmeta["STATISTICS_MODE"] = repr(int(round(modeval)))

            if ratObj is not None and not disableRFC40:
                histIndx, histNew = findOrCreateColumn(ratObj, gdal.GFU_PixelCount, 
                                        "Histogram", gdal.GFT_Real)
                # write the hist in a single go
                ratObj.SetRowCount(histnbins)
                ratObj.WriteArray(hist, histIndx)

                ratObj.SetLinearBinning(histmin, (histCalcMax - histCalcMin) / histnbins)

                # The HFA driver still honours the STATISTICS_HISTOBINVALUES
                # metadata item. If we are recalculating the histogram the old
                # values will be copied across with the metadata so clobber it
                if "STATISTICS_HISTOBINVALUES" in tmpmeta:
                    del tmpmeta["STATISTICS_HISTOBINVALUES"]
            else:
                # Use GDAL's original metadata interface, for drivers which
                # don't support the more modern approach
                tmpmeta["STATISTICS_HISTOBINVALUES"] = '|'.join(map(str, hist)) + '|'

                tmpmeta["STATISTICS_HISTOMIN"] = repr(histmin)
                tmpmeta["STATISTICS_HISTOMAX"] = repr(histmax)
                tmpmeta["STATISTICS_HISTONUMBINS"] = int(histnbins)

            # estimate the median - bin with the middle number
            middlenum = hist.sum() / 2
            gtmiddle = hist.cumsum() >= middlenum
            medianbin = gtmiddle.nonzero()[0][0]
            medianval = medianbin * histstep + histmin
            if band.DataType == gdal.GDT_Float32 or band.DataType == gdal.GDT_Float64:
                tmpmeta["STATISTICS_MEDIAN"] = repr(float(medianval))
            else:
                tmpmeta["STATISTICS_MEDIAN"] = repr(int(round(medianval)))
    
        # set the data
        band.SetMetadata(tmpmeta)

        if ratObj is not None and not ratObj.ChangesAreWrittenToFile():
            # For drivers that require the in memory thing
            band.SetDefaultRAT(ratObj)

        percent = percent + percentstep
        progress.setProgress(percent)

        if progress.wasCancelled():
            raise ProcessCancelledError()
    
    progress.setProgress(100)
    
    
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


class SinglePassInfo:
    """
    The required info for dealing with single-pass pyramids/statistics/histogram.
    There is some complexity here, because the decisions about what to do are
    a result of a number of different factors. We attempt to make these decisions
    as early as possible, and store the decisions on this object, so they can
    just be checked later.
    """
    def __init__(self, outfiles, controls, workinggrid):
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

        (nrows, ncols) = workinggrid.getDimensions()
        mindim = min(nrows, ncols)

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
        spReq = self.singlePassRequested[key]
        aggType = self.oviewAggtype[symbolicName]
        if spReq is True and aggType not in self.supportedAggtypes:
            msg = ("Single-pass pyramids explicitly requested, but " +
               "not supported for aggregationType '{}'").format(
                   aggType)
            raise SinglePassActionsError(msg)

        spPyr = ((spReq is True or spReq is None) and (not omit) and
            (aggType in self.supportedAggtypes))
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
        return (minval, maxval, first, last, nbins)


def handleSinglePassActions(ds, arr, singlePassInfo, symbolicName, seqNum,
        xOff, yOff):
    """
    Called from writeBlock, to handle the single-pass actions which may
    or may not be required.
    """
    numBands = arr.shape[0]
    if singlePassInfo.doSinglePassPyramids(symbolicName):
        writeBlockPyramids(ds, arr, singlePassInfo, symbolicName, xOff, yOff)
    if singlePassInfo.doSinglePassStatistics(symbolicName):
        accumList = singlePassInfo.accumulators[symbolicName, seqNum]
        for i in range(numBands):
            accumList[i].doStatsAccum(arr[i])
    if singlePassInfo.doSinglePassHistogram(symbolicName):
        accumList = singlePassInfo.accumulators[symbolicName, seqNum]
        for i in range(numBands):
            accum = accumList[i]
            singlePassHistAccum(arr[i], accum.hist, accum.histNullval,
                accum.histmin, accum.nbins)


def writeBlockPyramids(ds, arr, singlePassInfo, symbolicName, xOff, yOff):
    """
    Calculate and write out the pyramid layers for all bands of the block
    given as arr. Called when doing single-pass pyramid layers.

    """
    overviewLevels = singlePassInfo.overviewLevels[symbolicName]
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


def finishSinglePassStats(ds, singlePassInfo, symbolicName, seqNum):
    """
    Finish the single-pass basic statistics for all bands of the given
    file, and write them into the file.
    """
    accumList = singlePassInfo.accumulators[symbolicName, seqNum]
    numBands = len(accumList)
    for i in range(numBands):
        (minval, maxval, meanval, stddev) = accumList[i].finalStats()
        band = ds.GetRasterBand(i + 1)
        # In the old way, we used to write these using the named metadata
        # items, but that now seems to be obsolete.
        band.SetStatistics(float(minval), float(maxval), meanval, stddev)


def finishSinglePassHistogram(ds, singlePassInfo, symbolicName, seqNum):
    """
    Finish the histogram
    """
    accumList = singlePassInfo.accumulators[symbolicName, seqNum]
    numBands = len(accumList)
    for i in range(numBands):
        accum = accumList[i]
        (minval, maxval, first, last, nbins) = accum.histLimits()
        band = ds.GetRasterBand(i + 1)
        hist = accum.hist[first:last + 1]
        writeHistogram(band, hist, minval, maxval, nbins, accum.binFunc)
        # Do mode and median.....


def writeHistogram(outBand, hist, histmin, histmax, histnbins, histBinFunc):
    """
    Write the given values into the band object
    """
    # Should be deciding whether to use RFC40, or old metadata API.

    outBand.SetMetadataItem("STATISTICS_HISTOBINVALUES",
                            '|'.join(map(str, hist)) + '|')

    outBand.SetMetadataItem("STATISTICS_HISTOMIN", repr(histmin))
    outBand.SetMetadataItem("STATISTICS_HISTOMAX", repr(histmax))
    outBand.SetMetadataItem("STATISTICS_HISTONUMBINS", repr(int(histnbins)))
    outBand.SetMetadataItem("STATISTICS_HISTOBINFUNCTION", histBinFunc)
