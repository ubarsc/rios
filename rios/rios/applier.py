#!/usr/bin/env python
"""
Basic tools for setting up a function to be applied over 
a raster processing chain. 

"""
# Subversion id keywords
# $HeadURL$
# $Id$

import sys

from . import imagereader
from . import imagewriter
from . import rioserrors

# All default values, copied in from their appropriate rios modules. 
DEFAULT_RESAMPLEMETHOD = "near"
DEFAULTFOOTPRINT = imagereader.DEFAULTFOOTPRINT
DEFAULTWINDOWXSIZE = imagereader.DEFAULTWINDOWXSIZE
DEFAULTWINDOWYSIZE = imagereader.DEFAULTWINDOWYSIZE
DEFAULTOVERLAP = imagereader.DEFAULTOVERLAP
DEFAULTLOGGINGSTREAM = imagereader.DEFAULTLOGGINGSTREAM
DEFAULTDRIVERNAME = imagewriter.DEFAULTDRIVERNAME
DEFAULTCREATIONOPTIONS = imagewriter.DEFAULTCREATIONOPTIONS


class FilenameAssociations(object): 
    """
    Class for associating external image filenames with internal
    names, which are then the same names used inside a function given
    to the applier.apply() function. 
    
    Each attribute created on this object should be a filename, or a 
    list of filenames. The corresponding attribute names will appear 
    on the 'inputs' or 'outputs' objects inside the applied function. 
    Each such attribute will be an image data block or a list of image 
    data blocks, accordingly. 
    
    """
    pass

class BlockAssociations(object): 
    """
    Generic object to store the image blocks used within
    the applied function. The attributes are named the same way 
    as in the corresponding FilenameAssociations object, but are
    blocks of image data, instead of filenames. Where lists of 
    filenames were used, lists of image blocks are used here. 
    """
    pass
    
class OtherInputs(object): 
    """
    Generic object to store any extra inputs and outputs used 
    inside the function being applied. This class was originally
    named for inputs, but in fact works just as well for outputs, 
    too. Any items stored on this will be persistent between 
    iterations of the block loop. 
    """
    pass

class ApplierControls(object):
    """
    Controls for the operation of rios, for use with 
    the applier.apply() function. 
    
    This object starts with default values for all controls, and 
    has methods for setting each of them to something else. 
    
    Attributes are:
        windowxsize     X size of rios block (pixels)
        windowysize     Y size of rios block (pixels)
        overlap         Number of pixels in margin for block overlaps
        footprint       applier.INTERSECTION or applier.UNION
        drivername      global GDAL driver short name for output
        creationoptions global GDAL creation options for output
        thematic        global True/False for thematic outputs
        referenceImage  Image for reference projection and grid
        loggingstream   file-like for logging of messages
        progress        progress object
        statsIgnore     global stats ignore value for output
        statscache      stats cache if pre-calculated
        calcStats       True/False to signal calculate statistics and pyramids
        tempdir         Name of directory for temp files (resampling, etc.)
        thematic        One of 'thematic' or 'athematic'
        resampleMethod  String for resample method, when required (as per GDAL)
    
    Default values are provided for all attributes, and can then be over-ridden
    with the 'set' methods given. 
    
    Some 'set' methods take an optional imagename argument. If given, this should be 
    the same internal name used for a given image as in the FilenameAssociations
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
        self.progress = None
        self.creationoptions = DEFAULTCREATIONOPTIONS
        self.statscache = None
        self.statsIgnore = 0
        self.calcStats = True
        self.thematic = False
        self.tempdir = '.'
        self.resampleMethod = DEFAULT_RESAMPLEMETHOD
        
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
            if not self.optionsByImage.has_key(option):
                self.optionsByImage[option] = {}
            self.optionsByImage[option][imagename] = value
            
    def getOptionForImagename(self, option, imagename):
        """
        Returns the value of a particular option for the 
        given imagename. If only the global option has been set,
        then that is returned, but if a specific value has been set for 
        the given imagename, then use that. 
        
        The imagename is the same internal name as used for the image
        in the FilenameAssociations objects. 
        
        """
        value = getattr(self, option)
        if self.optionsByImage.has_key(option):
            if self.optionsByImage[option].has_key(imagename):
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
        to the amount of margin added to each block of input, so that the blocks 
        will overlap, hence the actual amount of overlap is really more like
        double this value (allowing for odd and even numbers, etc). 
        """
        self.overlap = overlap
        
    def setOutputDriverName(self, drivername, imagename=None):
        """
        Set the output driver name to the given GDAL shortname
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
        
    def setFootprintType(self, footprint):
        "Set type of footprint, one of INTERSECTION or UNION from this module"
        self.footprint = footprint
        
    def setReferenceImage(self, referenceImage):
        """
        Set the name of the image to use for the reference pixel grid and 
        projection. If not set, then no resampling will be allowed
        """
        self.referenceImage = referenceImage
        
    def setProgress(self, progress):
        """
        Set the progress display object. Default is to 
        use cuiprogress.CUIProgressBar
        """
        self.progress = progress
        
    def setCreationOptions(self, creationoptions, imagename=None):
        """
        Set a list of GDAL creation options (should match with the driver). 
        Each list element is a string of the form "NAME=VALUE". 
        """
        self.setOptionForImagename('creationoptions', imagename, creationoptions)
        
    def setStatsCache(self, statscache, imagename=None):
        "Set the stats cache, if statistics are known from some other source."
        self.setOptionForImagename('statscache', imagename, statscache)
        
    def setStatsIgnore(self, statsIgnore, imagename=None):
        """
        Set the global default value to use as the 
        null value when calculating stats.
        """
        self.setOptionForImagename('statsIgnore', imagename, statsIgnore)
        
    def setCalcStats(self, calcStats, imagename=None):
        """
        Set True to calc stats, False otherwise. If True, then statistics and 
        pyramid layers are calculated (if supported by the driver
        """
        self.setOptionForImagename('calcStats', imagename, calcStats)
        
    def setThematic(self, thematicFlag, imagename=None):
        "Set boolean value of thematic flag (may not be supported by the GDAL driver)"
        self.setOptionForImagename('thematic', imagename, thematicFlag)
        
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
    
    def makeResampleDict(self, imageDict):
        """
        Make a dictionary of resample methods, one for every image
        name in the given dictionary
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


def apply(userFunction, infiles, outfiles, otherArgs=None, controls=None):
        """
        Apply the given 'userFunction' to the given
        input and output files. 
        
        infiles and outfiles are FilenameAssociations objects to 
        define associations between internal variable names and
        external filenames, for the raster file inputs and outputs. 
        
        otherArgs is an object of extra arguments to be passed to the 
        userFunction, each with a sensible name on the object. These 
        can be either input or output arguments, entirely at the discretion
        of userFunction(). 
        
        The userFunction has the following call sequence
            userFunction(info, inputs, outputs)
        or
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
        
        The numpy arrays are always 3-d arrays, with shape
            (numBands, numRows, numCols)
        The datatype of the output image(s) is determined directly
        from the datatype of the numpy arrays in the outputs object. 
        
        The info object contains many useful details about the processing, 
        and will always be passed to the userFunction. It can, of course, 
        be ignored. It is an instance of the readerinfo.ReaderInfo class. 
        
        The controls argument, if given, is an instance of the 
        ApplierControls class, which allows control of various 
        aspects of the reading and writing of images. See the class 
        documentation for further details. 
        
        
        """
        # Get default controls object if none given. 
        if controls is None:
            controls = ApplierControls()
        
        inputBlocks = BlockAssociations()
        outputBlocks = BlockAssociations()
        reader = imagereader.ImageReader(infiles.__dict__, 
            controls.footprint, controls.windowxsize, controls.windowysize, 
            controls.overlap, controls.statscache, loggingstream=controls.loggingstream)

        if controls.referenceImage is not None:
            resampleDict = controls.makeResampleDict(infiles.__dict__)
            reader.allowResample(refpath=controls.referenceImage, tempdir=controls.tempdir,
                resamplemethod=resampleDict, useVRT=True)

        writerdict = {}
        
        if controls.progress is not None:
            controls.progress.setTotalSteps(100)
            controls.progress.setProgress(0)
            lastpercent = 0
        
        for (info, blockdict) in reader:
            inputBlocks.__dict__.update(blockdict)
            
            # Make a tuple of the arguments to pass to the function. 
            # Must have inputBlocks and outputBlocks, but if otherArgs 
            # is not None, then that is also included. 
            functionArgs = (info, inputBlocks, outputBlocks)
            if otherArgs is not None:
                functionArgs += (otherArgs, )
            
            # Now call the function with those args
            userFunction(*functionArgs)
            
            for name in outfiles.__dict__.keys():

                if name not in outputBlocks.__dict__:
                    msg = 'Output key %s not found in output blocks' % name
                    raise rioserrors.KeysMismatch(msg)

                outblock = outputBlocks.__dict__[name]
                outfileName = getattr(outfiles, name)
                if name not in writerdict:
                    # We have not yet created the output writers
                    if isinstance(outfileName, list):
                        # We have a list of filenames under this name in the dictionary,
                        # and so we must create a list of writers. The outblock will also be 
                        # a list of blocks
                        writerdict[name] = []
                        numFiles = len(outfileName)
                        if len(outblock) != numFiles:
                            raise rioserrors.MismatchedListLengthsError(("Output '%s' writes %d files, "+
                                "but only %d blocks given")%(name, numFiles, len(outblock)))
                        for i in range(numFiles):
                            filename = outfileName[i]
                            writer = imagewriter.ImageWriter(filename, info=info, 
                                firstblock=outblock[i], 
                                drivername=controls.getOptionForImagename('drivername', name), 
                                creationoptions=controls.getOptionForImagename('creationoptions', name))
                            writerdict[name].append(writer)
                            if controls.getOptionForImagename('thematic', name):
                                writer.setThematic()
                    else:
                        # This name in the dictionary is just a single filename
                        writer = imagewriter.ImageWriter(outfileName, info=info, firstblock=outblock,
                            drivername=controls.getOptionForImagename('drivername', name), 
                            creationoptions=controls.getOptionForImagename('creationoptions', name))
                        writerdict[name] = writer
                        if controls.getOptionForImagename('thematic', name):
                            writer.setThematic()
                else:
                    # The output writers exist, so select the correct one and write the block
                    if isinstance(outfileName, list):
                        # We have a list of files for this name, and a list of blocks to write
                        numFiles = len(outfileName)
                        if len(outblock) != numFiles:
                            raise rioserrors.MismatchedListLengthsError(("Output '%s' writes %d files, "+
                                "but only %d blocks given")%(name, numFiles, len(outblock)))
                        for i in range(numFiles):
                            writerdict[name][i].write(outblock[i])
                    else:
                        # This name is just a single file, and we write a single block
                        writerdict[name].write(outblock)
                    
            if controls.progress is not None:
                percent = info.getPercent()
                if percent != lastpercent:
                    controls.progress.setProgress(percent)
                    lastpercent = percent
                
        if controls.progress is not None:
            controls.progress.setProgress(100)    
                
        for name in outfiles.__dict__.keys():
            writer = writerdict[name]
            if isinstance(writer, list):
                for singleWriter in writer:
                    singleWriter.close(controls.getOptionForImagename('calcStats', name), 
                        controls.getOptionForImagename('statsIgnore', name), controls.progress)
            else:
                writer.close(controls.getOptionForImagename('calcStats', name), 
                    controls.getOptionForImagename('statsIgnore', name), controls.progress)
        


############
# Example

#def thefunc(info, inputs, outputs, otherargs):
#     bob_a = inputs.bob1 * otherargs.factor
#     outputs.bob3 = bob_a + inputs.bob2

#inputs = FilenameAssociations()
#inputs.bob1 = "some/dir/bob1.img"
#inputs.bob2 = "some/other/dir/bob2.img"
#outputs = FilenameAssociations()
#outputs.bob3 = "output/dir/bob3.img"
#otherargs = OtherInputs()
#otherargs.factor = 12

#apply(thefunc,inputs,outputs,otherargs)


