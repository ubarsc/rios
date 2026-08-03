"""
Contains the functions needed for opening and reading input files, and the
ReadWorkerMgr class used to manage concurrent read workers.

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
import sys
from concurrent import futures
import queue
import threading

import numpy
from osgeo import gdal, gdal_array, osr

from . import const
from . import rioserrors
from . import VersionObj
from .structures import BlockAssociations, WorkerErrorRecord
from .fileinfo import ImageInfo, VectorFileInfo, preventGdal3axisSwap
from .pixelgrid import PixelGridDefn, findCommonRegion

if sys.version_info[0] > 2:
    # hack for Python 3 which uses str instead of basestring
    # we just use basestring
    basestring = str

DEFAULTFOOTPRINT = int(os.getenv('RIOS_DFLT_FOOTPRINT', 
                            default=const.INTERSECTION))
DEFAULTWINDOWXSIZE = int(os.getenv('RIOS_DFLT_BLOCKXSIZE', default=256))
DEFAULTWINDOWYSIZE = int(os.getenv('RIOS_DFLT_BLOCKYSIZE', default=256))
DEFAULTOVERLAP = int(os.getenv('RIOS_DFLT_OVERLAP', default=0))
DEFAULTLOGGINGSTREAM = sys.stdout


def readBlockAllFiles(infiles, workinggrid, blockDefn, allInfo, gdalObjCache,
        controls, tmpfileMgr, rasterizeMgr):
    """
    Read all input files for a single block.
    Return the complete BlockAssociations object (i.e. 'inputs').
    """
    inputs = BlockAssociations(infiles)
    for (symbolicName, seqNum, filename) in infiles:
        arr = readBlockOneFile(blockDefn, symbolicName, seqNum, filename,
            gdalObjCache, controls, tmpfileMgr, rasterizeMgr, workinggrid,
            allInfo)
        inputs[symbolicName, seqNum] = arr
    return inputs


def readBlockOneFile(blockDefn, symbolicName, seqNum, filename, gdalObjCache,
        controls, tmpfileMgr, rasterizeMgr, workinggrid, allInfo):
    """
    Read the requested block, as per blockDefn, of the requested file,
    as per (symbolicName, seqNum, filename). If the file has already been
    opened, its GDAL objects will be in the gdalObjCache, otherwise
    it will be opened and those objects placed in the cache.

    Return a numpy array for the block, of shape (numBands, numRows, numCols).

    """
    if (symbolicName, seqNum) not in gdalObjCache:
        # The file has not yet been opened, so open it, and cache the
        # GDAL Dataset & Band objects
        fileInfo = allInfo[symbolicName, seqNum]
        (ds, bandObjList) = openForWorkingGrid(filename, workinggrid,
            fileInfo, controls, tmpfileMgr, rasterizeMgr, symbolicName)
        gdalObjCache[symbolicName, seqNum] = (ds, bandObjList)

    (ds, bandObjList) = gdalObjCache[symbolicName, seqNum]

    (left, top, xsize, ysize) = (blockDefn.left, blockDefn.top,
            blockDefn.ncols, blockDefn.nrows)

    # We construct the final output array. It begins as an array full of
    # nulls, then we read in the array for each band. Since the block
    # may be incomplete (i.e. off the edge of the extent), we then slot
    # it in to the right portion of the full array.

    margin = controls.overlap
    nBands = len(bandObjList)
    shape = (nBands, ysize + 2 * margin, xsize + 2 * margin)
    gdalType = bandObjList[0].DataType
    dtype = gdal_array.GDALTypeCodeToNumericTypeCode(gdalType)

    # We need a null value to initialize the array. Figure out
    # what to use, depending on what is available.
    nullvalList = controls.getOptionForImagename('inputnodata',
                symbolicName)
    if nullvalList is not None and not isinstance(nullvalList, list):
        nullvalList = [nullvalList] * len(bandObjList)
    if nullvalList is None:
        nullvalList = [bandObjList[i].GetNoDataValue()
                   for i in range(len(bandObjList))]

    # Now fill each layer with its corresponding null value. We start with
    # zeros, so if any of the null values is None, it will fallback to zero
    outArray = numpy.zeros(shape, dtype=dtype)
    for i in range(len(nullvalList)):
        if nullvalList[i] is not None:
            outArray[i].fill(nullvalList[i])

    for i in range(nBands):
        readIntoArray(outArray[i], ds, bandObjList[i], top, left,
            xsize, ysize, workinggrid, margin)

    return outArray


def readIntoArray(outArray, ds, bandObj, top_wg, left_wg,
            xsize, ysize, workinggrid, margin):
    """
    Read the requested block from the given band/dataset, and place it
    into the given output array. If the block falls off the edge of the
    file extent, the request is trimmed back, and the resulting smaller
    block is placed into the correct part of the array, leaving the
    surrounding array elements unchanged.

    The request coordinates (top, left, xsize, ysize) do not include the
    margin (i.e. overlap), so that is accounted for explicitly here.
    If margin > 0, the array is thus larger by (2*margin) in each direction.

    NOTE: While it may seem that this could be done using a VRT, our tests
    of that approach found that it imposes a substantial overhead, and doing
    it ourselves is much faster. 

    """
    # The row/col shift between working grid and file grid. The shift
    # should be SUBTRACTED from working grid row/col to get file row/col
    fileTransform = ds.GetGeoTransform()
    file_xMin = fileTransform[0]
    file_yMax = fileTransform[3]
    (xRes, yRes) = (workinggrid.xRes, workinggrid.yRes)
    colShift = int(round((file_xMin - workinggrid.xMin) / xRes))
    rowShift = int(round((workinggrid.yMax - file_yMax) / yRes))

    # The file coordinates of the outer-most pixels, including the margin
    fileLeft = left_wg - margin - colShift
    fileRight = left_wg + (xsize - 1) + margin - colShift
    fileTop = top_wg - margin - rowShift
    fileBottom = top_wg + (ysize - 1) + margin - rowShift

    # The number of rows/cols outside file extent in each direction, which
    # thus need to be trimmed off the array to actually read
    trimLeft = max(0, -fileLeft)
    trimRight = max(0, (fileRight + 1 - ds.RasterXSize))
    trimTop = max(0, -fileTop)
    trimBottom = max(0, (fileBottom + 1 - ds.RasterYSize))

    # Specify what to actually read
    left_read = fileLeft + trimLeft
    top_read = fileTop + trimTop
    xsize_read = fileRight + 1 - left_read - trimRight
    ysize_read = fileBottom + 1 - top_read - trimBottom

    if left_read >= 0 and top_read >= 0 and xsize_read > 0 and ysize_read > 0:
        subArr = bandObj.ReadAsArray(left_read, top_read, xsize_read, ysize_read)
        (subRows, subCols) = subArr.shape
        i1 = trimTop
        i2 = trimTop + subRows
        j1 = trimLeft
        j2 = trimLeft + subCols
        outArray[i1:i2, j1:j2] = subArr


def openForWorkingGrid(filename, workinggrid, fileInfo, controls,
        tmpfileMgr, rasterizeMgr, symbolicName):
    """
    If the fileInfo for the given filename is a raster, aligned with
    the working grid, just open it. If it is a raster, but not aligned,
    do a warp VRT that makes it aligned, and open that instead.
    If it is a vector, then first rasterize into a temp file and use that.

    Either way, return a GDAL Dataset object and a list of band objects
    corresponding to the selected bands.

    """
    (xRes, yRes) = (workinggrid.xRes, abs(workinggrid.yRes))
    gdalVersion = VersionObj(gdal.__version__)

    # If the file is actually a vector, then first rasterize it
    # onto the right pixel size. If it is the wrong projection, it will
    # be reprojected in raster form later on
    if isinstance(fileInfo, VectorFileInfo):
        vectorlayer = controls.getOptionForImagename('vectorlayer',
                symbolicName)

        if isinstance(vectorlayer, int):
            vecNdx = vectorlayer
        else:
            vecNdx = None
            for i in range(fileInfo.layerCount):
                if fileInfo[i].name == vectorlayer:
                    vecNdx = i
            if vecNdx is None:
                raise ValueError("Named vector layer '{}' not found".format(
                    vectorlayer))
        vecName = fileInfo[vecNdx].name

        vecLyrInfo = fileInfo[vecNdx]
        projection = vecLyrInfo.spatialRef.ExportToWkt()
        wgXmin = workinggrid.xMin
        wgYmin = workinggrid.yMin

        # Work out a resolution for the rasterized vector. Try to keep it
        # the same (or similar) to the working grid resolution
        wgSpatialRef = osr.SpatialReference()
        wgSpatialRef.ImportFromWkt(workinggrid.projection)
        preventGdal3axisSwap(wgSpatialRef)
        (nrows, ncols) = workinggrid.getDimensions()
        wgCtrX = wgXmin + xRes * (ncols // 2)   # Rough centre of grid
        wgCtrY = wgYmin + yRes * (nrows // 2)
        (xRes_vec, yRes_vec) = reprojResolution(xRes, yRes,
            wgCtrX, wgCtrY, wgSpatialRef, vecLyrInfo.spatialRef)

        xMin = PixelGridDefn.snapToGrid(vecLyrInfo.xMin, wgXmin, xRes_vec) - xRes_vec
        xMax = PixelGridDefn.snapToGrid(vecLyrInfo.xMax, wgXmin, xRes_vec) + xRes_vec
        yMin = PixelGridDefn.snapToGrid(vecLyrInfo.yMin, wgYmin, yRes_vec) - yRes_vec
        yMax = PixelGridDefn.snapToGrid(vecLyrInfo.yMax, wgYmin, yRes_vec) + yRes_vec
        vectorPixgrid = PixelGridDefn(projection=projection,
            xMin=xMin, xMax=xMax, yMin=yMin, yMax=yMax,
            xRes=xRes_vec, yRes=yRes_vec)
        gridList = [workinggrid, vectorPixgrid]
        try:
            commonRegion = findCommonRegion(gridList, vectorPixgrid,
                combine=const.INTERSECTION)
        except rioserrors.IntersectionError:
            commonRegion = None
        dtype = controls.getOptionForImagename('vectordatatype', symbolicName)
        gdalDtype = gdal_array.NumericTypeCodeToGDALTypeCode(dtype)
        gtiffOptions = ['TILED=YES', 'COMPRESS=DEFLATE', 'BIGTIFF=IF_SAFER']
        if commonRegion is not None:
            outBounds = (commonRegion.xMin, commonRegion.yMin,
                commonRegion.xMax, commonRegion.yMax)
        else:
            outBounds = (wgCtrX, wgCtrY, wgCtrX + xRes_vec, wgCtrY + yRes_vec)
        vecNull = controls.getOptionForImagename('vectornull', symbolicName)
        burnattribute = controls.getOptionForImagename('burnattribute',
                symbolicName)
        burnvalue = None
        if burnattribute is None:
            burnvalue = controls.getOptionForImagename('burnvalue',
                    symbolicName)
        alltouched = controls.getOptionForImagename('alltouched',
                    symbolicName)
        filtersql = controls.getOptionForImagename('filtersql', symbolicName)
        rasterizeOptions = gdal.RasterizeOptions(format='GTiff',
            outputType=gdalDtype, creationOptions=gtiffOptions,
            outputBounds=outBounds, xRes=xRes_vec, yRes=yRes_vec, noData=vecNull,
            initValues=vecNull, burnValues=burnvalue, attribute=burnattribute,
            allTouched=alltouched, SQLStatement=filtersql, layers=vecName)

        tmprast = rasterizeMgr.rasterize(filename, rasterizeOptions,
                tmpfileMgr)
        filename = tmprast
        fileInfo = ImageInfo(filename)

    fileToOpen = filename

    if reprojectionRequired(fileInfo, workinggrid):
        vrtfile = tmpfileMgr.mktempfile(prefix='rios_', suffix='.vrt')

        srcProj = specialProjFixes(fileInfo.projection)
        dstProj = specialProjFixes(workinggrid.projection)
        outBounds = (workinggrid.xMin, workinggrid.yMin,
            workinggrid.xMax, workinggrid.yMax)

        # Work out what null value(s) to use, honouring anything set
        # with controls.setInputNoDataValue().
        nullvalList = controls.getOptionForImagename('inputnodata',
                symbolicName)
        if nullvalList is not None and not isinstance(nullvalList, list):
            # Turn a scalar into a list, one for each band in the file
            nullvalList = [nullvalList] * fileInfo.rasterCount
        # If we have None from controls, then use whatever is
        # specified on fileInfo
        if nullvalList is None:
            nullvalList = fileInfo.nodataval

        # The WarpOptions constructor has weird expectations about the
        # null values, so construct what it requires. It accepts other
        # forms, but they result in performance penalties, sometimes
        # quite severe. Not sure if this is the best form, but it is the
        # best I could find.
        if all([n is None for n in nullvalList]):
            nullval = None
        else:
            nullval = ' '.join([repr(n) for n in nullvalList])

            # Cope with a small bug in gdal 3.9.0 & 3.9.1. For these versions,
            # if the null value for the first layer is negative, then having a
            # leading minus causes trouble with how the string is parsed. By
            # prepending a leading space we avoid the problem.
            gdalHasNegNullBug = (gdalVersion == VersionObj('3.9.0') or
                                 gdalVersion == VersionObj('3.9.1'))
            if (gdalHasNegNullBug and len(nullvalList) > 1 and
                    nullvalList[0] < 0):
                nullval = ' ' + nullval

        overviewLevel = 'NONE'
        if controls.getOptionForImagename('allowOverviewsGdalwarp',
                symbolicName):
            overviewLevel = 'AUTO'
        resampleMethod = controls.getOptionForImagename('resampleMethod',
            symbolicName)
        warpOptions = gdal.WarpOptions(format="VRT", outputBounds=outBounds,
            xRes=xRes, yRes=yRes, srcNodata=nullval, srcSRS=srcProj,
            dstSRS=dstProj, dstNodata=nullval, overviewLevel=overviewLevel,
            resampleAlg=resampleMethod)
        # Have to remove the vrtfile, because gdal.Warp won't over-write.
        os.remove(vrtfile)
        gdal.Warp(vrtfile, filename, options=warpOptions)
        fileToOpen = vrtfile

    ds = gdal.Open(fileToOpen)
    layerselection = controls.getOptionForImagename('layerselection',
            symbolicName)
    if layerselection is None:
        # Default to all bands
        layerselection = [(i + 1) for i in range(ds.RasterCount)]

    bandObjList = [ds.GetRasterBand(i) for i in layerselection]

    return (ds, bandObjList)


def reprojectionRequired(imgInfo, workinggrid):
    """
    Compare the details of the given imgInfo and the workinggrid,
    to work out if a reprojection is required. Return True if so.
    """
    proj = specialProjFixes(imgInfo.projection)
    pixGrid = PixelGridDefn(projection=proj,
        xMin=imgInfo.xMin, xMax=imgInfo.xMax, xRes=imgInfo.xRes,
        yMin=imgInfo.yMin, yMax=imgInfo.yMax, yRes=imgInfo.yRes)

    allEqual = (workinggrid.equalPixSize(pixGrid) and
                workinggrid.equalProjection(pixGrid) and
                workinggrid.alignedWith(pixGrid))
    reprojReqd = not allEqual
    return reprojReqd


def specialProjFixes(projwkt):
    """
    Does any special fixes required for the projection. Returns the fixed
    projection WKT string.

    Specifically this does two things, both of which are to cope with rubbish
    that Imagine has put into the projection. Firstly, it removes the
    crappy TOWGS84 parameters which Imagine uses for GDA94, and secondly
    removes the crappy name which Imagine gives to the correct GDA94.

    If neither of these things is found, returns the string unchanged.

    """
    dodgyTOWGSstring = "TOWGS84[-16.237,3.51,9.939,1.4157e-06,2.1477e-06,1.3429e-06,1.91e-07]"
    properTOWGSstring = "TOWGS84[0,0,0,0,0,0,0]"
    if projwkt.find('"GDA94"') > 0 or projwkt.find('"Geocentric_Datum_of_Australia_1994"') > 0:
        newWkt = projwkt.replace(dodgyTOWGSstring, properTOWGSstring)
    else:
        newWkt = projwkt

    # Imagine's name for the correct GDA94 also causes problems, so
    # replace it with something more standard.
    newWkt = newWkt.replace('GDA94-ICSM', 'GDA94')

    return newWkt


def reprojResolution(xRes, yRes, x, y, srcSRS, tgtSRS):
    """
    Return a reprojected version of the given resolution. The (xRes yRes)
    values are given in the srcSRS project, and are translated to something
    as similar as possible in the tgtSRS projection. The rough location
    is given by (x, y) (in the src projection), so the transformation is
    at its best around that point, and would be progressively worse the
    further one gets from there (due to the increased distortion from the
    different projections).

    """
    t = osr.CoordinateTransformation(srcSRS, tgtSRS)
    (tl_x, tl_y, z) = t.TransformPoint(x, y)
    (tr_x, tr_y, z) = t.TransformPoint(x + xRes, y)
    (bl_x, bl_y, z) = t.TransformPoint(x, y - yRes)
    tgtXres = tr_x - tl_x
    tgtYres = tl_y - bl_y
    return (tgtXres, tgtYres)


class ReadWorkerMgr:
    """
    Simple class to hold all the things we need to sustain for
    the read worker threads
    """
    def __init__(self):
        self.threadPool = None
        self.workerList = None
        self.readTaskQue = None
        self.forceExit = None
        self.isActive = False

    def startReadWorkers(self, blockList, infiles, allInfo, controls,
            tmpfileMgr, rasterizeMgr, workinggrid, inBlockBuffer, timings,
            exceptionQue):
        """
        Start the requested number of read worker threads, within the current
        process. All threads will read single blocks from individual files
        and place them into the inBlockBuffer.

        Return value is an instance of ReadWorkerMgr, which must remain
        active until all reading is complete.

        """
        numWorkers = controls.concurrency.numReadWorkers
        threadPool = futures.ThreadPoolExecutor(max_workers=numWorkers)
        readTaskQue = queue.Queue()

        # Put all read tasks into the queue. A single task is one block of
        # input for one input file.
        for blockDefn in blockList:
            for (symName, seqNum, filename) in infiles:
                task = (blockDefn, symName, seqNum, filename)
                readTaskQue.put(task)

        workerList = []
        forceExit = threading.Event()
        for i in range(numWorkers):
            worker = threadPool.submit(self.readWorkerFunc, readTaskQue,
                inBlockBuffer, controls, tmpfileMgr, rasterizeMgr, workinggrid,
                allInfo, timings, forceExit, exceptionQue)
            workerList.append(worker)

        self.threadPool = threadPool
        self.workerList = workerList
        self.readTaskQue = readTaskQue
        self.forceExit = forceExit
        self.isActive = True

    @staticmethod
    def readWorkerFunc(readTaskQue, blockBuffer, controls, tmpfileMgr,
            rasterizeMgr, workinggrid, allInfo, timings, forceExit,
            exceptionQue):
        """
        This function runs in each read worker thread. The readTaskQue gives
        it tasks to perform (i.e. single blocks of data to read), and it loops
        until there are no more to do. Each block is sent back through
        the blockBuffer.

        """
        # Each instance of this readWorkerFunc has its own set of GDAL objects,
        # as these cannot be shared between threads.
        gdalObjCache = {}

        try:
            try:
                readTask = readTaskQue.get(block=False)
            except queue.Empty:
                readTask = None
            while readTask is not None and not forceExit.is_set():
                (blockDefn, symName, seqNum, filename) = readTask
                with timings.interval('reading'):
                    arr = readBlockOneFile(blockDefn, symName, seqNum,
                        filename, gdalObjCache, controls, tmpfileMgr,
                        rasterizeMgr, workinggrid, allInfo)

                with timings.interval('insert_readbuffer'):
                    blockBuffer.addBlockData(blockDefn, symName, seqNum, arr)

                try:
                    readTask = readTaskQue.get(block=False)
                except queue.Empty:
                    readTask = None
        except Exception as e:
            exceptionRecord = WorkerErrorRecord(e, 'read')
            exceptionQue.put(exceptionRecord)

    def shutdown(self):
        """
        Shut down the read worker manager
        """
        self.forceExit.set()
        self.threadPool.shutdown()
        self.isActive = False

    def __del__(self):
        "Destructor"
        if self.isActive:
            # If we have not already done shutdown, then do it
            self.shutdown()
