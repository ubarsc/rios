"""
Contains functions used to write output files from applier.apply.

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

from __future__ import print_function, division

import sys
import os

import numpy
from osgeo import gdal
from osgeo import gdal_array

from . import rioserrors
from . import rat
from . import calcstats
from . import fileinfo


def setDefaultDriver():
    """
    Sets some default values into global variables, defining
    what defaults we should use for GDAL driver. On any given
    output file these can be over-ridden, and can be over-ridden globally
    using the environment variables

        * $RIOS_DFLT_DRIVER
        * $RIOS_DFLT_DRIVEROPTIONS
        * $RIOS_DFLT_CREOPT_<drivername>
    
    If RIOS_DFLT_DRIVER is set, then it should be a gdal short driver name. 
    If RIOS_DFLT_DRIVEROPTIONS is set, it should be a space-separated list
    of driver creation options, e.g. "COMPRESS=LZW TILED=YES", and should
    be appropriate for the selected GDAL driver. This can also be 'None'
    in which case an empty list of creation options is passed to the driver.
    
    The same rules apply to the driver-specific creation options given
    using $RIOS_DFLT_CREOPT_<driver>. These options are a later paradigm, and 
    are intended to supercede the previous generic driver defaults. 
    
    If not otherwise supplied, the default is to use the HFA driver, with compression. 
    
    The code here is more complex than desirable, because it copes with legacy behaviour
    in the absence of the environment variables, and in the absence of the driver-specific
    option variables. 
        
    """
    global DEFAULTDRIVERNAME, DEFAULTCREATIONOPTIONS
    DEFAULTDRIVERNAME = os.getenv('RIOS_DFLT_DRIVER', default='HFA')
    creationOptionsStr = os.getenv('RIOS_DFLT_DRIVEROPTIONS')
    if creationOptionsStr is not None:
        DEFAULTCREATIONOPTIONS = creationOptionsStr.split()
    else:
        # To cope with the old behaviour, set something sensible for HFA, but not
        # otherwise.
        # The IGNOREUTM=YES is there to switch off a minor kludge in GDAL's
        # HFA driver. By default, it will check any Transverse Mercator
        # projection, and if its parameters match a standard UTM zone, it
        # re-states the projection as literal UTM. This was originally because
        # Imagine was not good at matching equivalent projections. This is no
        # longer true, and we choose to disable that behaviour by default.
        if DEFAULTDRIVERNAME == "HFA":
            DEFAULTCREATIONOPTIONS = ['COMPRESSED=YES', 'IGNOREUTM=YES']
        else:
            DEFAULTCREATIONOPTIONS = []
    
    # In the new paradigm, default creation options are specific to each driver, and
    # are loaded into a dictionary
    global dfltDriverOptions
    dfltDriverOptions = {}
    # Start with the old generic default options, applied to the default driver
    dfltDriverOptions[DEFAULTDRIVERNAME] = DEFAULTCREATIONOPTIONS
    # Load some others which we wish to have as defaults, even if not set by the environment
    dfltDriverOptions['GTiff'] = ['TILED=YES', 'COMPRESS=DEFLATE',
        'INTERLEAVE=BAND', 'BIGTIFF=IF_SAFER']
    # Now load any which are specified by environment variables, of the
    # form RIOS_DFLT_CREOPT_<drivername>
    driverOptVarPrefix = 'RIOS_DFLT_CREOPT_'
    for varname in os.environ:
        if varname.startswith(driverOptVarPrefix):
            drvrName = varname[len(driverOptVarPrefix):]
            optionsStr = os.getenv(varname)
            dfltDriverOptions[drvrName] = optionsStr.split()


setDefaultDriver()


def writeBlock(gdalOutObjCache, blockDefn, outfiles, outputs, controls,
        workinggrid, singlePassMgr, timings):
    """
    Write the given block to the files given in outfiles
    """
    for (symbolicName, seqNum, filename) in outfiles:
        arr = outputs[symbolicName, seqNum]
        # Trim the margin
        m = controls.getOptionForImagename('overlap', symbolicName)
        if m > 0:
            arr = arr[:, m:-m, m:-m]

        key = (symbolicName, seqNum)
        if key not in gdalOutObjCache:
            ds = openOutfile(symbolicName, filename, controls, arr,
                    workinggrid)
            gdalOutObjCache[symbolicName, seqNum] = ds
            singlePassMgr.initFor(ds, symbolicName, seqNum, arr)

        ds = gdalOutObjCache[symbolicName, seqNum]

        with timings.interval('writing'):
            checkForNanOrInf(arr, filename)

            # Write the base raster data
            ds.WriteArray(arr, blockDefn.left, blockDefn.top)

        # If appropriate, do single-pass actions for this block
        calcstats.handleSinglePassActions(ds, arr, singlePassMgr,
            symbolicName, seqNum, blockDefn.left, blockDefn.top, timings)


def openOutfile(symbolicName, filename, controls, arr, workinggrid):
    """
    Open the requested output file
    """
    # RIOS only works with 3-d image arrays, where the first dimension is
    # the number of bands. Check that this is what the user gave us to write.
    if len(arr.shape) != 3:
        msg = ("Shape of array to write must be 3-d. " +
            "Shape is actually {}").format(repr(arr.shape))
        raise rioserrors.ArrayShapeError(msg)

    deleteIfExisting(filename)

    driverName = controls.getOptionForImagename('drivername', symbolicName)
    creationoptions = controls.getOptionForImagename('creationoptions',
        symbolicName)
    if creationoptions is None:
        creationoptions = dfltDriverOptions.get(driverName, [])
    doubleCheckCreationOptions(driverName, creationoptions, controls,
        workinggrid)

    numBands = arr.shape[0]
    gdalDatatype = gdal_array.NumericTypeCodeToGDALTypeCode(arr.dtype)
    if gdalDatatype is None:
        msg = f"Array type {arr.dtype} has no corresponding GDAL data type"
        raise rioserrors.ImageOpenError(msg)
    (nrows, ncols) = workinggrid.getDimensions()
    geotransform = workinggrid.makeGeoTransform()
    projWKT = workinggrid.projection
    thematic = controls.getOptionForImagename('thematic', symbolicName)
    nullVal = controls.getOptionForImagename('statsIgnore', symbolicName)
    layernames = controls.getOptionForImagename('layernames', symbolicName)

    drvr = gdal.GetDriverByName(driverName)
    ds = drvr.Create(filename, ncols, nrows, numBands, gdalDatatype,
        creationoptions)
    if ds is None:
        msg = 'Unable to create output file {}'.format(filename)
        raise rioserrors.ImageOpenError(msg)
    ds.SetGeoTransform(geotransform)
    ds.SetProjection(projWKT)

    for i in range(numBands):
        band = ds.GetRasterBand(i + 1)
        if thematic:
            band.SetMetadataItem('LAYER_TYPE', 'thematic')
        if nullVal is not None:
            band.SetNoDataValue(nullVal)
        if layernames is not None:
            band.SetDescription(layernames[i])

    return ds


def closeOutfiles(gdalOutObjCache, outfiles, controls, singlePassMgr, timings):
    """
    Close all the output files
    """
    # getOpt is just a little local shortcut
    getOpt = controls.getOptionForImagename

    for (symbolicName, seqNum, filename) in outfiles:
        omitPyramids = getOpt('omitPyramids', symbolicName)
        omitBasicStats = getOpt('omitBasicStats', symbolicName)
        omitHistogram = getOpt('omitHistogram', symbolicName)
        overviewLevels = getOpt('overviewLevels', symbolicName)
        overviewMinDim = getOpt('overviewMinDim', symbolicName)
        overviewAggType = getOpt('overviewAggType', symbolicName)
        approxStats = getOpt('approxStats', symbolicName)
        autoColorTableType = getOpt('autoColorTableType', symbolicName)
        callBeforeClose = getOpt('callBeforeClose', symbolicName)
        progress = getOpt('progress', symbolicName)
        if progress is None:
            from .cuiprogress import SilentProgress
            progress = SilentProgress()

        ds = gdalOutObjCache[symbolicName, seqNum]
        with timings.interval('writing'):
            # Ensure that all data has been written
            ds.FlushCache()

        if (not singlePassMgr.doSinglePassPyramids(symbolicName) and
                not omitPyramids):
            # Pyramids have not been done single-pass, and are not being
            # omitted, so do them on closing (i.e. the old way)
            with timings.interval('pyramids'):
                calcstats.addPyramid(ds, progress, levels=overviewLevels,
                    minoverviewdim=overviewMinDim,
                    aggregationType=overviewAggType)

        if singlePassMgr.doSinglePassStatistics(symbolicName):
            with timings.interval('basicstats'):
                calcstats.finishSinglePassStats(ds, singlePassMgr,
                    symbolicName, seqNum)
            # Make the minMaxList from values already on singlePassMgr
            minMaxList = makeMinMaxList(singlePassMgr, symbolicName, seqNum)
        elif not omitBasicStats:
            with timings.interval('basicstats'):
                minMaxList = calcstats.addBasicStatsGDAL(ds, approxStats)

        if singlePassMgr.doSinglePassHistogram(symbolicName):
            with timings.interval('histogram'):
                calcstats.finishSinglePassHistogram(ds, singlePassMgr,
                    symbolicName, seqNum)
        elif not omitHistogram:
            with timings.interval('histogram'):
                calcstats.addHistogramsGDAL(ds, minMaxList, approxStats)

        warnNanOrInf(filename)

        if callBeforeClose is not None and len(callBeforeClose) == 2:
            (beforeCloseFunc, beforeCloseArgs) = callBeforeClose
            with timings.interval('beforeclose'):
                beforeCloseFunc(ds, *beforeCloseArgs)

        # This is doing everything I can to ensure the file gets fully closed
        # at this point.
        ds.FlushCache()
        gdalOutObjCache.pop((symbolicName, seqNum))
        del ds

        # Check whether we will need to add an auto color table
        if autoColorTableType is not None:
            # Does nothing if layers are not thematic
            addAutoColorTable(filename, autoColorTableType)


def makeMinMaxList(singlePassMgr, symbolicName, seqNum):
    """
    Make a list of min/max values per band, for the nominated output file,
    from values already present on singlePassMgr.
    Mimicing the list returned by addBasicStatsGDAL, for use with
    addHistogramsGDAL.

    """
    accumList = singlePassMgr.accumulators[symbolicName, seqNum]
    minMaxList = []
    for i in range(len(accumList)):
        accum = accumList[i]
        (minval, maxval) = (accum.minval, accum.maxval)
        minMaxList.append((minval, maxval))
    return minMaxList


def deleteIfExisting(filename):
    """
    Delete the filename if it already exists. If possible, use the
    appropriate GDAL driver to do so, to ensure that any associated
    files will also be deleted.

    """
    if os.path.exists(filename):
        drvr = gdal.IdentifyDriver(filename)
        if drvr is not None:
            drvr.Delete(filename)
        else:
            # Apparently not a valid GDAL file, for whatever reason,
            # so just remove the file directly.
            os.remove(filename)


def doubleCheckCreationOptions(drivername, creationoptions, controls,
        workinggrid):
    """
    Try to ensure that the given creation options are compatible with
    RIOS operations. Does not attempt to ensure they are totally valid, as
    that is GDAL's job.

    If it finds any incompatibility, an exception is raised.

    """
    if drivername == 'GTiff':
        # The GDAL GTiff driver is incapable of reclaiming space within the
        # file. This means that if a block is re-written, then the space
        # already used is left dangling, and the total file size gets larger
        # accordingly. If the RIOS block size is not a multiple of the TIFF
        # block size, then each RIOS block will require the re-writing of at
        # least one TIFF block (usually several). This turns out to be a
        # disaster for file sizes. So, here, we do our best to check these
        # things, and prevent such a result. The recommended configuration
        # is that the $RIOS_DFLT_BLOCKXSIZE and $RIOS_DFLT_BLOCKYSIZE be
        # set to a power of 2, and everything else will follow.

        # Work out what block size values the GTiff driver will use
        tiffBlockX = None
        tiffBlockY = None
        tiled = False
        for optStr in creationoptions:
            optTokens = optStr.split('=')
            if optTokens[0] == 'BLOCKXSIZE':
                tiffBlockX = int(optStr[11:])
            elif optTokens[0] == 'BLOCKYSIZE':
                tiffBlockY = int(optStr[11:])
            elif optTokens[0] == 'TILED':
                tiled = True

        # Apply default TIFF block sizes if not explicitly requested. These are
        # as defined by GDAL at
        #   https://gdal.org/drivers/raster/gtiff.html#creation-options
        # assuming I have read it correctly.
        (nRows, nCols) = workinggrid.getDimensions()
        if tiffBlockX is None:
            if tiled:
                tiffBlockX = 256
            else:
                # If not TILED=YES then GTiff uses blocks which are full width
                tiffBlockX = nCols
        if tiffBlockY is None:
            if tiled:
                tiffBlockY = 256
            else:
                # If not tiled, then default strip height is such that one
                # strip is 8K (which I assume is a count of pixels)
                tiffBlockY = int(8 * 1024 / tiffBlockX)

        # Require that tiff block sizes be a factor of the RIOS block size, so
        # that whole TIFF blocks are always written exactly once, with no
        # re-writing.
        riosBlockX = controls.windowxsize
        riosBlockY = controls.windowysize
        if ((riosBlockX < tiffBlockX) or ((riosBlockX % tiffBlockX) != 0) or
                (riosBlockY < tiffBlockY) or ((riosBlockY % tiffBlockY) != 0)):
            msg = ("RIOS block dimensions {} should be multiples of GTiff " +
                "block dimensions {}, otherwise vast amounts of space are " +
                "wasted rewriting blocks which are not reclaimed.").format(
                (riosBlockX, riosBlockY), (tiffBlockX, tiffBlockY))
            raise rioserrors.ImageOpenError(msg)


def addAutoColorTable(filename, autoColorTableType):
    """
    If autoColorTable has been set up for this output, then generate
    a color table of the requested type, and add it to the current
    file. This is called AFTER the Dataset has been closed, so is performed on
    the filename. This only applies to thematic layers, so when we open the file
    and find that the layers are athematic, we do nothing. 

    """
    imgInfo = fileinfo.ImageInfo(filename)
    if imgInfo.layerType == "thematic":
        imgStats = fileinfo.ImageFileStats(filename)
        ds = gdal.Open(filename, gdal.GA_Update)

        for i in range(imgInfo.rasterCount):
            numEntries = int(imgStats[i].max + 1)
            clrTbl = rat.genColorTable(numEntries, autoColorTableType)
            band = ds.GetRasterBand(i + 1)
            ratObj = band.GetDefaultRAT()
            redIdx, redNew = calcstats.findOrCreateColumn(ratObj, gdal.GFU_Red, "Red", gdal.GFT_Integer)
            greenIdx, greenNew = calcstats.findOrCreateColumn(ratObj, gdal.GFU_Green, "Green", gdal.GFT_Integer)
            blueIdx, blueNew = calcstats.findOrCreateColumn(ratObj, gdal.GFU_Blue, "Blue", gdal.GFT_Integer)
            alphaIdx, alphaNew = calcstats.findOrCreateColumn(ratObj, gdal.GFU_Alpha, "Alpha", gdal.GFT_Integer)
            # were any of these not already existing?
            if redNew or greenNew or blueNew or alphaNew:
                ratObj.WriteArray(clrTbl[:, 0], redIdx)
                ratObj.WriteArray(clrTbl[:, 1], greenIdx)
                ratObj.WriteArray(clrTbl[:, 2], blueIdx)
                ratObj.WriteArray(clrTbl[:, 3], alphaIdx)
            if not ratObj.ChangesAreWrittenToFile():
                band.SetDefaultRAT(ratObj)


# Set of files which require a warning about NaN or Inf pixels
nanWarningFiles = set()


def checkForNanOrInf(arr, filename):
    """
    Check if the given array is float and contains any NaN or Inf elements.
    If any non-finite is found, then the filename is added to the global
    nanWarningFiles set.

    If the array is not a float (or complex) dtype, then no check is done.
    If it is float (or complex), then numpy.isfinite is used to check
    all elements.

    If the filename is already in nanWarningFiles, no further check is done.
    """
    if filename in nanWarningFiles:
        return

    isFloat = (arr.dtype.kind in ('f', 'c'))
    ok = True
    if isFloat:
        ok = numpy.isfinite(arr).all()
    if not ok:
        nanWarningFiles.add(filename)


def warnNanOrInf(filename):
    """
    Print a warning message if the given filename is a member
    of nanWarningFiles set. Clears the filename once the warning is printed.
    """
    if filename in nanWarningFiles:
        msg = f"WARNING: Output file {filename} contains NaN, Inf or -Inf"
        print(msg, file=sys.stderr)
        nanWarningFiles.remove(filename)
