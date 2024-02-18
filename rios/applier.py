#!/usr/bin/env python
"""
Basic tools for setting up a function to be applied over 
a raster processing chain. The :func:`rios.applier.apply` function is the main
point of entry in this module. 

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

import numpy

from . import rioserrors
from .imagereader import DEFAULTFOOTPRINT, DEFAULTWINDOWXSIZE
from .imagereader import DEFAULTWINDOWYSIZE, DEFAULTOVERLAP, DEFAULTLOGGINGSTREAM
from .imagereader import readBlockAllFiles, readBlockOneFile
from .imagewriter import DEFAULTDRIVERNAME, DEFAULTCREATIONOPTIONS
from .imagewriter import dfltDriverOptions, writeBlock, closeOutfiles
from .imageio import INTERSECTION, UNION, BOUNDS_FROM_REFERENCE
from .calcstats import DEFAULT_OVERVIEWLEVELS, DEFAULT_MINOVERVIEWDIM
from .calcstats import DEFAULT_OVERVIEWAGGREGRATIONTYPE
from .rat import DEFAULT_AUTOCOLORTABLETYPE
from .structures import BlockAssociations, OtherInputs
from .structures import BlockCache, Timers, TempfileManager, ApplierReturn
from .structures import ReadWorkerMgr, ApplierBlockDefn
from .structures import CW_NONE
from .fileinfo import ImageInfo, VectorFileInfo
from .pixelgrid import PixelGridDefn, findCommonRegion
from .readerinfo import makeReaderInfo
from .computemanager import getComputeWorkerManager


DEFAULT_RESAMPLEMETHOD = "near"


class ApplierControls(object):
    """
    Controls for the operation of rios, for use with 
    the :func:`rios.applier.apply` function. 
    
    This object starts with default values for all controls, and 
    has methods for setting each of them to something else. 
    
    Attributes are:
        * **windowxsize**     X size of rios block (pixels)
        * **windowysize**     Y size of rios block (pixels)
        * **overlap**         Number of pixels in margin for block overlaps
        * **footprint**       :data:`rios.applier.INTERSECTION` or :data:`rios.applier.UNION` or :data:`rios.applier.BOUNDS_FROM_REFERENCE`
        * **drivername**      GDAL driver short name for output
        * **creationoptions** GDAL creation options for output
        * **thematic**        True/False for thematic outputs
        * **layernames**      List of layer names for outputs
        * **referenceImage**  Image for reference projection and grid that inputs will be resampled to.
        * **referencePixgrid** pixelGrid for reference projection and grid
        * **loggingstream**   file-like for logging of messages
        * **progress**        progress object
        * **statsIgnore**     global stats ignore value for output (i.e. null value)
        * **calcStats**       True/False to signal calculate statistics and pyramids
        * **omitPyramids**    True/False to omit pyramids when doing stats
        * **overviewLevels**  List of level factors used when calculating output image overviews
        * **overviewMinDim**  Minimum dimension of highest overview level
        * **overviewAggType** Aggregation type for calculating overviews
        * **tempdir**         Name of directory for temp files (resampling, etc.)
        * **resampleMethod**  String for resample method, when required (as per GDAL)
        * **numThreads**      Number of parallel threads used for processing each image block
        * **jobManagerType**  Which :class:`rios.parallel.jobmanager.JobManager` sub-class to use for parallel processing (by name)
        * **autoColorTableType** Type of color table to be automatically added to thematic output rasters
        * **allowOverviewsGdalwarp** Allow use of overviews in input resample (dangerous, do not use)
        * **approxStats**       Allow approx stats (much faster)
    
    Options relating to vector input files
        * **burnvalue**       Value to burn into raster from vector
        * **filtersql**       SQL where clause used to filter vector features
        * **alltouched**      Boolean. If True, all pixels touched are included in vector. 
        * **burnattribute**   Name of vector attribute used to supply burnvalue
        * **vectorlayer**     Number (or name) of vector layer
        * **vectordatatype**  Numpy datatype to use for raster created from vector
        * **vectornull**      Rasterised vector is initialised to this value, before burning
        
    
    Default values are provided for all attributes, and can then be over-ridden
    with the 'set' methods given. 
    
    Some 'set' methods take an optional imagename argument. If given, this should be 
    the same internal name used for a given image as in the :class:`rios.applier.FilenameAssociations`
    objects. This is the internal name for that image, and the method will set 
    the parameter in question for that specific image, which will over-ride the
    global value set when no imagename is given. 
    
    """
    def __init__(self):
        self.loggingstream = sys.stdout
        self.drivername = DEFAULTDRIVERNAME
        self.overlap = DEFAULTOVERLAP
        self.windowxsize = DEFAULTWINDOWXSIZE
        self.windowysize = DEFAULTWINDOWYSIZE
        self.footprint = DEFAULTFOOTPRINT
        self.referenceImage = None
        self.referencePixgrid = None
        self.progress = None
        self.creationoptions = None
        self.statsIgnore = 0
        self.calcStats = True
        self.omitPyramids = False
        self.overviewLevels = DEFAULT_OVERVIEWLEVELS
        self.overviewMinDim = DEFAULT_MINOVERVIEWDIM
        self.overviewAggType = None
        self.thematic = False
        self.layernames = None
        self.tempdir = '.'
        self.resampleMethod = DEFAULT_RESAMPLEMETHOD
        self.numThreads = 1
        self.jobManagerType = os.getenv('RIOS_DFLT_JOBMGRTYPE', default=None)
        self.autoColorTableType = DEFAULT_AUTOCOLORTABLETYPE
        self.allowOverviewsGdalwarp = False
        self.approxStats = False

        # Vector fields
        self.burnvalue = 1
        self.vectornull = 0
        self.burnattribute = None
        self.filtersql = None
        self.alltouched = False
        self.vectordatatype = numpy.uint8
        self.vectorlayer = 0
        self.layerselection = None

        # Options specific to a named image. This was added on later, and is 
        # only valid for some of the attributes, so it looks a bit out-of-place.
        # Instead of the options being attributes of self, they are keys in a
        # dictionary. This dictionary is managed by the two methods
        # setOptionForImagename() and getOptionForImagename(). 
        self.optionsByImage = {}
    
    def setOptionForImagename(self, option, imagename, value):
        """
        Set the given option specifically for the given imagename. This 
        method is for internal use only. If you wish to set a particular 
        attribute, use the corresponding 'set' method. 
        """
        if imagename is None:
            setattr(self, option, value)
        else:
            if option not in self.optionsByImage:
                self.optionsByImage[option] = {}
            self.optionsByImage[option][imagename] = value
            
    def getOptionForImagename(self, option, imagename):
        """
        Returns the value of a particular option for the 
        given imagename. If only the global option has been set,
        then that is returned, but if a specific value has been set for 
        the given imagename, then use that. 
        
        The imagename is the same internal name as used for the image
        in the :class:`rios.applier.FilenameAssociations` objects. 
        
        """
        value = getattr(self, option)
        if option in self.optionsByImage:
            if imagename in self.optionsByImage[option]:
                value = self.optionsByImage[option][imagename]
        return value
        
    def setLoggingStream(self, loggingstream):
        """
        Set the rios logging stream to the given file-like object. 
        """
        self.loggingstream = loggingstream
        
    def setOverlap(self, overlap):
        """
        Set the overlap to the given value.

        Overlap is a number of pixels, and is somewhat mis-named. It refers
        to the amount of margin added to each block of input, so that the
        blocks will overlap, hence the actual amount of overlap is really
        more like double this value (allowing for odd and even numbers, etc).

        The margin can result in pixels which are outside the extent of
        the given input images. These pixels will be filled with the null
        value for that input file, or zero if no null value is set on
        that file.

        """
        self.overlap = overlap
        
    def setOutputDriverName(self, drivername, imagename=None):
        """
        Set the output driver name to the given GDAL shortname.
        
        Note that the GDAL creation options have defaults suitable only 
        for the default driver, so if one sets the output driver, then 
        the creation options should be reviewed too. 
        
        In more recent versions of RIOS, the addition of driver-specific
        default creation options ($RIOS_DFLT_CREOPT_<driver>) allows for
        multiple default creation options to be set up.
        
        """
        self.setOptionForImagename('drivername', imagename, drivername)
        
    def setWindowXsize(self, windowxsize):
        """
        Set the X size of the blocks used. Images are processed in 
        blocks (windows) of 'windowxsize' columns, and 'windowysize' rows. 
        """
        self.windowxsize = windowxsize
        
    def setWindowYsize(self, windowysize):
        """
        Set the Y size of the blocks used. Images are processed in 
        blocks (windows) of 'windowxsize' columns, and 'windowysize' rows. 
        """
        self.windowysize = windowysize
        
    def setWindowSize(self, windowxsize, windowysize):
        """
        Sets the X and Y size of the blocks used in one call.
        Images are processed in blocks (windows) of 'windowxsize' 
        columns, and 'windowysize' rows. 
        """
        self.windowxsize = windowxsize
        self.windowysize = windowysize
        
    def setFootprintType(self, footprint):
        """
        Set type of footprint, one of INTERSECTION, UNION or 
        BOUNDS_FROM_REFERENCE from this module

        The footprint type controls the extent of the pixel grid
        used for calculation within the user function, and of the
        output files.

        Using INTERSECTION will result in the maximum extent which
        is wholly included in all of the input images. Using UNION results
        in the minimum extent which wholly includes all of the input
        images. If BOUNDS_FROM_REFERENCE is used, then the extent will
        be the same as that of the reference image or pixgrid, regardless
        of the extents of the various other inputs.

        For both UNION and BOUNDS_FROM_REFERENCE, it is possible to
        have pixels which are within the extent, but outside one or
        more of the input files. The input data for such pixels are filled
        with the null value for that file. If no null value is set for that
        file, then zero is used.

        """
        self.footprint = footprint
        
    def setReferenceImage(self, referenceImage):
        """
        Set the name of the image to use for the reference pixel grid and 
        projection. If neither referenceImage nor referencePixgrid are set, 
        then no resampling will be allowed. Only set one of referenceImage or
        referencePixgrid. 
        
        Note that this is the external filename, not the internal name (which 
        unfortunately is a bit inconsistent with everything else). 
        
        """
        self.referenceImage = referenceImage
        
    def setReferencePixgrid(self, referencePixgrid):
        """
        Set the reference pixel grid. If neither referenceImage nor 
        referencePixgrid are set, then no resampling will be allowed. 
        Only set one of referenceImage or referencePixgrid. The referencePixgrid
        argument is of type :class:`rios.pixelgrid.PixelGridDefn`. 

        """
        self.referencePixgrid = referencePixgrid

    def setProgress(self, progress):
        """
        Set the progress display object. Default is no progress
        object. 
        """
        self.progress = progress
        
    def setCreationOptions(self, creationoptions, imagename=None):
        """
        Set a list of GDAL creation options (should match with the driver). 
        Each list element is a string of the form "NAME=VALUE". 
        
        Defaults are suitable for the default driver, and need to be changed
        if that is changed. However, if an appropriate driver-specific default 
        environment variable ($RIOS_DFLT_CREOPT_<driver>) is given, this 
        will be used. 
        
        """
        self.setOptionForImagename('creationoptions', imagename, creationoptions)
        
    def setStatsIgnore(self, statsIgnore, imagename=None):
        """
        Set the global default value to use as the 
        null value when calculating stats.
        Setting this to None means there will be no null value in the 
        stats calculations.
        """
        self.setOptionForImagename('statsIgnore', imagename, statsIgnore)
        
    def setCalcStats(self, calcStats, imagename=None):
        """
        Set True to calc stats, False otherwise. If True, then statistics and 
        pyramid layers are calculated (if supported by the driver
        """
        self.setOptionForImagename('calcStats', imagename, calcStats)
        
    def setOmitPyramids(self, omitPyramids, imagename=None):
        """
        Set True to omit pyramid layers, False otherwise. If True, then when
        statistics are being calculated, pyramid layers will be omitted, 
        otherwise they will be created at the same time. 
        Usual default is False. 
        """
        self.setOptionForImagename('omitPyramids', imagename, omitPyramids)
    
    def setOverviewLevels(self, overviewLevels, imagename=None):
        """
        Set the overview levels to be used on output images (i.e. pyramid layers). 
        Levels are specified as a list of integer factors, with the same meanings 
        as given to the gdaladdo command. 
        
        """
        self.setOptionForImagename('overviewLevels', imagename, overviewLevels)
    
    def setOverviewMinDim(self, overviewMinDim, imagename=None):
        """
        Set minimum dimension allowed for output overview. Overview levels (i.e. pyramid
        layers) will be calculated as per the overviewLevels list of factors, but 
        only until the minimum dimension falls below the value of overviewMinDim
        
        """
        self.setOptionForImagename('overviewMinDim', imagename, overviewMinDim)
    
    def setOverviewAggregationType(self, overviewAggType, imagename=None):
        """
        Set the type of aggregation used when computing overview images (i.e. pyramid 
        layers). Normally a thematic image should be aggregated using "NEAREST", while a 
        continuous image should be aggregated using "AVERAGE". When the setting is 
        given as None, then a default is used. If using an output format which 
        supports LAYER_TYPE, the default is based on this, but if not, it comes from 
        the value of the environment variable $RIOS_DEFAULT_OVERVIEWAGGREGATIONTYPE.
        
        This method should usually be used to set when writing an output to a format
        which does not support LAYER_TYPE, and which is not appropriate for the
        setting given by the environment default. 
        
        """
        self.setOptionForImagename('overviewAggType', imagename, overviewAggType)
        
    def setThematic(self, thematicFlag, imagename=None):
        "Set boolean value of thematic flag (may not be supported by the GDAL driver)"
        self.setOptionForImagename('thematic', imagename, thematicFlag)

    def setLayerNames(self, layerNames, imagename=None):
        """
        Set list of layernames to be given to the output file(s)
        """
        self.setOptionForImagename('layernames', imagename, layerNames)
        
    def setTempdir(self, tempdir):
        "Set directory to use for temporary files for resampling, etc. "
        self.tempdir = tempdir
        
    def setResampleMethod(self, resampleMethod, imagename=None):
        """
        Set resample method to be used for all resampling. Possible 
        options are those defined by gdalwarp, i.e. 'near', 'bilinear', 
        'cubic', 'cubicspline', 'lanczos'. 
        """
        self.setOptionForImagename('resampleMethod', imagename, resampleMethod)
    
    def setBurnValue(self, burnvalue, vectorname=None):
        """
        Set the burn value to be used when rasterizing the input vector(s).
        If vectorname given, set only for that vector. Default is 1. 
        """
        self.setOptionForImagename('burnvalue', vectorname, burnvalue)
    
    def setBurnAttribute(self, burnattribute, vectorname=None):
        """
        Set the vector attribute name from which to get the burn value
        for each vector feature. If vectorname is given, set only for that
        vector input. Default is to use burnvalue instead of burnattribute. 
        """
        self.setOptionForImagename('burnattribute', vectorname, burnattribute)
    
    def setVectorNull(self, vectornull, vectorname=None):
        """
        Set the vector null value. This is used to initialise the
        rasterised vector, before burning in the burn value. This is of most
        importance when burning values from a vector attribute column, as 
        this should be a distinct value from any of the values in the column. 
        If this is not so, then polygons can end up blending with the background,
        resulting in incorrect answers. 
        """
        self.setOptionForImagename('vectornull', vectorname, vectornull)
    
    def setFilterSQL(self, filtersql, vectorname=None):
        """
        Set an SQL WHERE clause which will be used to filter vector features.
        If vectorname is given, then set only for that vector
        """
        self.setOptionForImagename('filtersql', vectorname, filtersql)
    
    def setAlltouched(self, alltouched, vectorname=None):
        """
        Set boolean value of alltouched attribute. If alltouched is True, then
        pixels will count as "inside" a vector polygon if they touch the polygon,
        rather than only if their centre is inside. 
        If vectornmame given, then set only for that vector. 
        """
        self.setOptionForImagename('alltouched', vectorname, alltouched)
    
    def setVectorDatatype(self, vectordatatype, vectorname=None):
        """
        Set numpy datatype to use for rasterized vectors
        If vectorname given, set only for that vector
        """
        self.setOptionForImagename('vectordatatype', vectorname, vectordatatype)
    
    def setVectorlayer(self, vectorlayer, vectorname=None):
        """
        Set number/name of vector layer, for vector formats which have 
        multiple layers. Not required for plain shapefiles. 
        Can be either a layer number (start at zero) or 
        a layer name. If vectorname given, set only for that vector.
        """
        self.setOptionForImagename('vectorlayer', vectorname, vectorlayer)
    
    def makeResampleDict(self, imageDict):
        """
        Make a dictionary of resample methods, one for every image
        name in the given dictionary. This method is for internal use only. 
        """
        d = {}
        imagenamelist = imageDict.keys()
        for name in imagenamelist:
            method = self.getOptionForImagename('resampleMethod', name)
            if isinstance(imageDict[name], list):
                # We have a list of images for this name, so make a list of 
                # resample methods
                d[name] = [method] * len(imageDict[name])
            else:
                # We have just one image, so the corresponding entry is just one 
                # resample method
                d[name] = method
        return d
    
    def selectInputImageLayers(self, layerselection, imagename=None):
        """
        Set which layers are to be read from the input image(s). Default
        will read all layers. If imagename is given, selection will be for 
        that image only. The layerselection parameter should be a list
        of layer numbers. Layer numbers follow GDAL conventions, i.e. 
        a layer number of 1 refers to the first layer in the file. 
        Can  be much more efficient when only using a small subset of 
        layers from the inputs. 
        """
        self.setOptionForImagename('layerselection', imagename, layerselection)
    
    def setNumThreads(self, numThreads):
        """
        Set the number of 'threads' to be used when processing each block 
        of imagery. Note that these are not threads in the technical sense, 
        but are handled by the JobManager class, and are some form of 
        cooperating parallel processes, depending on the type of job 
        manager sub-class selected. See :mod:`rios.parallel.jobmanager` 
        for full details. Note that this is only worth using on very 
        computationally-intensive tasks. Default is 1, i.e. no parallel 
        processing. 
        
        """
        self.numThreads = numThreads
    
    def setJobManagerType(self, jobMgrType):
        """
        Set which type of JobManager is to be used for parallel processing.
        See :mod:`rios.parallel.jobmanager` for details. Default is taken from
        $RIOS_DFLT_JOBMGRTYPE. 
        
        """
        self.jobManagerType = jobMgrType
    
    def setAutoColorTableType(self, autoColorTableType, imagename=None):
        """
        If this option is set, then thematic raster outputs will have a
        color table automatically generated and attached to them. The type is
        passed to :func:`rios.rat.genColorTable` to determine what type of automatic
        color table is generated. 
        
        The default type will be taken from $RIOS_DFLT_AUTOCOLORTABLETYPE if it
        is set. If that is not set, then the default is not to automatically attached
        any color table to thematic output rasters.
        
        In practise, it is probably simpler to explicitly set the color table using 
        the :func:`rios.rat.setColorTable` function, after creating the file, but this
        is an alternative. 
        
        Note that the imagename parameter can be given, in which case the autoColorTableType 
        will only be applied to that raster. 
        
        None of this has any impact on athematic outputs. 
        
        """
        self.setOptionForImagename('autoColorTableType', imagename, autoColorTableType)
    
    def setAllowOverviewsGdalwarp(self, allowOverviewsGdalwarp):
        """
        This option is provided purely for testing purposes, and it is recommended 
        that this never be used operationally. 
        
        In GDAL >= 2.0, the default behaviour of gdalwarp was modified so that it
        will use overviews during a resample to a lower resolution. By default, 
        RIOS now switches this off again (by giving gdalwarp the '-ovr NONE' 
        switch), as this is very unreliable behaviour. Overviews can be 
        calculated by many different methods, and the user of the 
        file cannot tell how they were done. 
        
        In order to allow users to assess the damage done by this, we provide
        this option to allow resampling to use overviews. This also allows
        compatibility with versions of RIOS which did not switch it off, before 
        we discovered that it was happening. To allow this, set this parameter
        to True, otherwise it defaults to False. 
        
        We strongly recommend against allowing gdalwarp to use overviews. 
        
        """
        self.allowOverviewsGdalwarp = allowOverviewsGdalwarp
    
    def setApproxStats(self, approxStats):
        """
        Set boolean value of approxStats attribute. This modifies the behaviour of
        calcStats by forcing it to use the pyramid layers during stats generation
        (much faster but only provides approximate values, not recommended for
        thematic rasters)
        """
        self.approxStats = approxStats


def apply(userFunction, infiles, outfiles, otherArgs=None, controls=None):
    """
    Apply the given 'userFunction' to the given
    input and output files. 

    infiles and outfiles are :class:`rios.applier.FilenameAssociations` objects to 
    define associations between internal variable names and
    external filenames, for the raster file inputs and outputs. 

    otherArgs is an object of extra arguments to be passed to the 
    userFunction, each with a sensible name on the object. These 
    can be either input or output arguments, entirely at the discretion
    of userFunction(). otherArgs should be in instance of :class:`rios.applier.OtherInputs`

    The userFunction has the following call sequence::

        userFunction(info, inputs, outputs)

    or::

        userFunction(info, inputs, outputs, otherArgs)

    if otherArgs is not None.

    inputs and outputs are objects in which there are named attributes 
    with the same names as those given in the infiles and outfiles 
    objects. In the inputs and outputs objects, available inside 
    userFunction, these attributes contain numpy arrays of data read 
    from/written to the corresponding image file. 

    If the attributes given in the infiles or outfiles objects are 
    lists of filenames, the the corresponding attributes of the 
    inputs and outputs objects inside the applied function will be 
    lists of image data blocks instead of single blocks. 

    The numpy arrays are always 3-d arrays, with shape::

        (numBands, numRows, numCols)

    The datatype of the output image(s) is determined directly
    from the datatype of the numpy arrays in the outputs object. 

    The info object contains many useful details about the processing, 
    and will always be passed to the userFunction. It can, of course, 
    be ignored. It is an instance of the :class:`rios.readerinfo.ReaderInfo` class. 

    The controls argument, if given, is an instance of the 
    :class:`rios.applier.ApplierControls` class, which allows control of various 
    aspects of the reading and writing of images. See the class 
    documentation for further details. 

    There is a page dedicated to :doc:`applierexamples`.

    """
    if controls is None:
        controls = ApplierControls()

    # Includes ImageInfo and VectorFileInfo, keyed by (logicalname, seqNum)
    allInfo = readAllImgInfo(infiles)
    # Make the working grid
    workinggrid = makeWorkingGrid(infiles, allInfo, controls)
    # Divide the working grid into blocks for processing
    blockList = makeBlockList(workinggrid, controls)

    # The various cases for different concurrency styles
    concurrency = controls.concurrency
    if (concurrency.computeWorkerKind == CW_NONE):
        rtn = apply_singleCompute(userFunction, infiles, outfiles,
            otherArgs, controls, allInfo, workinggrid, blockList,
            None, None)
    else:
        rtn = apply_multipleCompute(userFunction, infiles, outfiles,
            otherArgs, controls, allInfo, workinggrid, blockList)

    return rtn


def apply_singleCompute(userFunction, infiles, outfiles, otherArgs,
        controls, allInfo, workinggrid, blockList, outBlockCache,
        inBlockCache):
    """
    Apply function for simplest configuration, with no compute concurrency.
    Does have possible read concurrency.

    This is also called for each compute worker in the batch-oriented
    compute worker styles, where each worker is an instance of a
    single-compute case.

    """
    timings = Timers()

    concurrency = controls.concurrency
    tmpfileMgr = TempfileManager(controls.tempdir)
    readWorkerMgr = None
    gdalObjCache = None
    if inBlockCache is None:
        if concurrency.numReadWorkers > 0:
            inBlockCache = BlockCache(infiles, concurrency.numReadWorkers)
            readWorkerMgr = startReadWorkers(blockList, infiles, allInfo,
                controls, tmpfileMgr, workinggrid, inBlockCache)
        else:
            gdalObjCache = {}
    if outBlockCache is None:
        gdalOutObjCache = {}

    for blockDefn in blockList:
        readerInfo = makeReaderInfo(workinggrid, blockDefn, controls)
        if inBlockCache is None:
            with timings.interval('reading'):
                inputs = readBlockAllFiles(infiles, workinggrid, blockDefn,
                    allInfo, gdalObjCache, controls, tmpfileMgr)
        else:
            with timings.interval('waitaddincache'):
                inputs = inBlockCache.popCompleteBlock(blockDefn)

        outputs = BlockAssociations()
        userArgs = (readerInfo, inputs, outputs)
        if otherArgs is not None:
            userArgs += otherArgs

        with timings.interval('userfunction'):
            userFunction(*userArgs)

        if outBlockCache is None:
            with timings.interval('writing'):
                writeBlock(gdalOutObjCache, blockDefn, outfiles, outputs,
                    controls, workinggrid)
        else:
            with timings.interval('waitaddoutcache'):
                outBlockCache.insertCompleteBlock(blockDefn, outputs)

    if outBlockCache is None:
        closeOutfiles(gdalOutObjCache, outfiles, controls)
    if readWorkerMgr is not None:
        readWorkerMgr.shutdown()

    # Set up returns object
    rtn = ApplierReturn()
    rtn.timings = timings
    rtn.cachemonitors = None
    rtn.otherArgsList = None

    return rtn


def apply_multipleCompute(userFunction, infiles, outfiles, otherArgs,
        controls, allInfo, workinggrid, blockList):
    """
    Multiple compute workers
    """
    concurrency = controls.concurrency
    tmpfileMgr = TempfileManager(controls.tempdir)
    computeMgr = getComputeWorkerManager(concurrency.computeWorkerKind)
    timings = Timers()

    numComputeWorkers = concurrency.numComputeWorkers
    outBlockCache = BlockCache(outfiles, numComputeWorkers)
    gdalOutObjCache = {}

    inBlockCache = None
    readWorkerMgr = None
    if not concurrency.computeWorkersRead and concurrency.numReadWorkers > 0:
        inBlockCache = BlockCache(infiles, concurrency.numReadWorkers)
        if concurrency.numReadWorkers > 0:
            readWorkerMgr = startReadWorkers(blockList, infiles, allInfo,
                controls, tmpfileMgr, workinggrid, inBlockCache, timings)

    gdalObjCache = None
    if (concurrency.numReadWorkers == 0 and
            not concurrency.computeWorkersRead):
        gdalObjCache = {}

    computeMgr.startWorkers(numWorkers=concurrency.numComputeWorkers,
        userFunction=userFunction, infiles=infiles, outfiles=outfiles,
        otherArgs=otherArgs, controls=controls, blockList=blockList,
        inBlockCache=inBlockCache, outBlockCache=outBlockCache,
        workinggrid=workinggrid, allInfo=allInfo,
        computeWorkersRead=concurrency.computeWorkersRead,
        singleBlockComputeWorkers=concurrency.singleBlockComputeWorkers,
        tmpfileMgr=tmpfileMgr, haveSharedTemp=concurrency.haveSharedTemp)

    for blockDefn in blockList:
        if (concurrency.numReadWorkers == 0 and
                not concurrency.computeWorkersRead):
            with timings.interval('reading'):
                inputs = readBlockAllFiles(infiles, workinggrid, blockDefn,
                    allInfo, gdalObjCache, controls, tmpfileMgr)
            with timings.interval('waitaddincache'):
                inBlockCache.insertCompleteBlock(blockDefn, inputs)

        with timings.interval('waitpopoutcache'):
            outputs = outBlockCache.popCompleteBlock(blockDefn)
        with timings.interval('writing'):
            writeBlock(gdalOutObjCache, blockDefn, outfiles, outputs,
                    controls, workinggrid)

    closeOutfiles(gdalOutObjCache, outfiles, controls)
    computeMgr.shutdown()
    if readWorkerMgr is not None:
        readWorkerMgr.shutdown()

    # Assemble the return object
    rtn = ApplierReturn()
    outObjList = computeMgr.outObjList
    timingsList = [obj for obj in outObjList if isinstance(obj, Timers)]
    rtn.timings = timings
    for t in timingsList:
        rtn.timings.merge(t)
    rtn.otherArgsList = [obj for obj in computeMgr.outObjList
        if isinstance(obj, OtherInputs)]

    return rtn


def startReadWorkers(blockList, infiles, allInfo, controls, tmpfileMgr,
        workinggrid, inBlockCache, timings):
    """
    Start the requested number of read worker threads, within the current
    process. All threads will read single blocks from individual files
    and place them into the inBlockCache.

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
    for i in range(numWorkers):
        worker = threadPool.submit(readWorkerFunc, readTaskQue,
            inBlockCache, controls, tmpfileMgr, workinggrid, allInfo,
            timings)
        workerList.append(worker)

    return ReadWorkerMgr(threadPool, workerList, readTaskQue)


def readWorkerFunc(readTaskQue, blockCache, gdalObjCache, controls, tmpfileMgr,
        workinggrid, allInfo, timings):
    """
    This function runs in each read worker thread. The readTaskQue gives
    it tasks to perform (i.e. single blocks of data to read), and it loops
    until there are no more to do. Each block is sent back through
    the blockCache.

    """
    # Each instance of this readWorkerFunc has its own set of GDAL objects,
    # as these cannot be shared between threads.
    gdalObjCache = {}

    readTask = readTaskQue.get(block=False)
    while readTask is not None:
        (blockDefn, symName, seqNum, filename) = readTask
        with timings.interval('reading'):
            arr = readBlockOneFile(symName, seqNum, filename,
                gdalObjCache, controls, tmpfileMgr, workinggrid, allInfo)

        with timings.interval('waitaddincache'):
            blockCache.addBlockData(blockDefn, symName, seqNum, arr)

        readTask = readTaskQue.get(block=False)


def readAllImgInfo(infiles):
    """
    Open all input files and create an ImageInfo (or VectorFileInfo)
    object for each. Return a dictionary of them, keyed by their
    position within infiles, i.e. (symbolicName, SeqNum).

    """
    allInfo = {}
    for (symbolicName, seqNum, filename) in infiles:
        try:
            infoObj = ImageInfo(filename)
        except (RuntimeError, rioserrors.FileOpenError):
            infoObj = None

        # Try as a vector
        if infoObj is None:
            try:
                infoObj = VectorFileInfo(filename)
            except (RuntimeError, rioserrors.FileOpenError):
                infoObj = None

        if infoObj is None:
            msg = "Unable to open '{}'".format(filename)
            raise rioserrors.FileOpenError(msg)

        allInfo[symbolicName, seqNum] = infoObj

    return allInfo


def makeWorkingGrid(infiles, allInfo, controls):
    """
    Work out the projection and extent of the working grid.

    Return a PixelGridDefn object representing it.
    """
    # Make a list of all the pixel grids
    pixgridList = []
    for info in allInfo.values():
        if isinstance(info, ImageInfo):
            pixgrid = PixelGridDefn(projection=info.projection,
                        geotransform=info.transform,
                        nrows=info.nrows, ncols=info.ncols)
            pixgridList.append(pixgrid)

    # Work out the reference pixel grid
    refPixGrid = controls.referencePixgrid
    if refPixGrid is None and controls.referenceImage is not None:
        refImage = controls.referenceImage

        refNdx = None
        for (symbolicName, seqNum, filename) in infiles:
            # refImage can be either a symbolic name or a real filename,
            # so check both.
            if refImage in (symbolicName, filename):
                refNdx = (symbolicName, seqNum)
        refInfo = allInfo[refNdx]
        refPixGrid = PixelGridDefn(projection=refInfo.projection,
                        geotransform=refInfo.transform,
                        nrows=refInfo.nrows, ncols=refInfo.ncols)

    if refPixGrid is None:
        # We have not been given a reference. This means that we should not
        # be doing any reprojecting, so check that all pixel grids match
        # the first one
        refPixGrid = pixgridList[0]
        match = checkAllMatch(pixgridList, refPixGrid)
        if not match:
            msg = ('Input grids do not match. Must supply a reference'
                'image or pixelgrid')
            raise rioserrors.ResampleNeededError(msg)

    workinggrid = findCommonRegion(pixgridList, refPixGrid,
        controls.footprint)
    return workinggrid


def checkAllMatch(pixgridList, refPixGrid):
    """
    Returns whether any resampling necessary to match
    reference dataset.

    Use as a check if no resampling is done that we
    can proceed ok.

    """

    match = True
    for pixGrid in pixgridList:
        if not refPixGrid.isComparable(pixGrid):
            match = False
            break
        elif not refPixGrid.alignedWith(pixGrid):
            match = False
            break

    return match


def makeBlockList(workinggrid, controls):
    """
    Divide the working grid area into blocks. Return a list of
    ApplierBlockDefn objects
    """
    blockList = []
    (nrows, ncols) = (workinggrid.nrows, workinggrid.ncols)
    top = 0
    while top < nrows:
        ysize = min(controls.windowysize, (nrows - top))
        left = 0
        while left < ncols:
            xsize = min(controls.windowxsize, (ncols - left))

            blockDefn = ApplierBlockDefn(top, left, xsize, ysize)
            blockList.append(blockDefn)
            left += xsize
        top += ysize
    return blockList


def updateProgress(controls, info, lastpercent):
    """
    Called by :func:`rios.applier.apply` to update progress
    """
    if controls.progress is not None:
        percent = info.getPercent()
        if percent != lastpercent:
            controls.progress.setProgress(percent)
            lastpercent = percent
    return lastpercent
