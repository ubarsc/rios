"""
An implementation os rios.applier that uses the multiprocessing module
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
from multiprocessing import Pool

from rios import rioserrors
from rios import imagereader

# import the bits we need from rios.applier
from rios.applier import FilenameAssociations, BlockAssociations, OtherInputs
from rios.applier import closeOutputImages, updateProgress, handleInputResampling
from rios.applier import writeOutputBlocks, separateVectors, opensAsRaster
from rios.applier import opensAsVector, makeVectorObjects
# give it a different name so we can derive our own with the same name
from rios.applier import ApplierControls as BasicControls

DEFAULT_NPROCESSES = int(os.getenv('RIOS_DFLT_NPROCESSES', default=2))

class ApplierControls(BasicControls):
    """
    Specialised version of ApplierControls that contains
    ways of controlling 
    """
    def __init__(self):
        BasicControls.__init__(self)
        self.nprocesses = DEFAULT_NPROCESSES

    def setNProcesses(self, nprocesses):
        """
        Set the number of processes to use default is DEFAULT_NPROCESSES
        """
        self.nprocesses = nprocesses

def multiUserFunction(args):
    """
    Called by the map function. Takes an argument which contains all the
    info needed to run the function. Returns the outputBlocks the function
    has created.
    """
    # unpack info
    userFunction, info, inputBlocks, otherArgs = args
    # create a new association object for the outputs
    outputBlocks = BlockAssociations()

    # Make a tuple of the arguments to pass to the function. 
    # Must have inputBlocks and outputBlocks, but if otherArgs 
    # is not None, then that is also included. 
    functionArgs = (info, inputBlocks, outputBlocks)
    if otherArgs is not None:
        functionArgs += (otherArgs, )
    # now set this to something sensible
    # (we set this to None in the main process as it
    # couldn't be serialised)
    info.loggingstream = sys.stdout

    # Now call the function with those args
    userFunction(*functionArgs)

    return outputBlocks

def apply(userFunction, infiles, outfiles, otherArgs=None, controls=None):
    """
    Apply the given 'userFunction' to the given
    input and output files in a parallel manner with the Python
    multiprocessing module.

    Number of processes your data will be run on is controled
    by the nprocesses field of the controls.
        
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
    documentation for further details. This is identical to the controls
    class in rios.applier with the addition of a setNProcesses() call
    which allows control over the number of processes to use.
        
    """
    # Get default controls object if none given. 
    if controls is None:
        controls = ApplierControls()
    if not hasattr(controls, 'nprocesses'):
        msg = "Controls object must have nprocesses field"
        raise rioserrors.WrongControlsObject(msg)
    if controls.nprocesses < 2:
        msg = "Number of processes must be 2 or greater"
        raise rioserrors.ParameterError(msg)
        
    (imagefiles, vectorfiles) = separateVectors(infiles)
    reader = imagereader.ImageReader(imagefiles.__dict__, 
        controls.footprint, controls.windowxsize, controls.windowysize, 
        controls.overlap, controls.statscache, loggingstream=controls.loggingstream)

    vecreader = None
    if len(vectorfiles) > 0:
        vectordict = makeVectorObjects(vectorfiles, controls)
        vecreader = vectorreader.VectorReader(vectordict, progress=controls.progress)
        
    handleInputResampling(imagefiles, controls, reader)
    reader.prepare(controls.workingGrid)

    writerdict = {}
        
    if controls.progress is not None:
        controls.progress.setTotalSteps(100)
        controls.progress.setProgress(0)
    lastpercent = 0

    # create the pool
    # one block is always done by this process so just need -1
    pool = Pool(processes=(controls.nprocesses-1))

    # get an iterator onto the reader object 
    # so we don't start at the beginning for each read
    # might be a more Pythonic way of doing this
    readeritr = reader.__iter__()

    done = False
    while not done:

        # list of arguments to be passed to Pool.map
        mapArgs = []
        # so we can keep a track of the info for each
        infoList = []
        # now read in controls.nprocesses blocks
        while len(mapArgs) < controls.nprocesses:
            try:
                (info, blockdict) = readeritr.next()

                inputBlocks = BlockAssociations()
                inputBlocks.__dict__.update(blockdict)
                if vecreader is not None:
                    vecblocks = vecreader.rasterize(info)
                    inputBlocks.__dict__.update(vecblocks)

                # we don't need to make a copy since info already is a copy
                # need to clobber a few things as these can't be copied to 
                # the subprocesses
                # info.blocklookup contains GDAL datasets
                # plus is keyed on the id() of the blocks which 
                info.blocklookup = None 
                # stderr can't be copied - reset in the subprocess
                info.loggingstream = None 

                args = (userFunction, info, inputBlocks, otherArgs)
                mapArgs.append(args)
                infoList.append(info)

            except StopIteration:
                done = True
                break

        if len(mapArgs) > 0:

            if len(mapArgs) > 1:
                
                # get the most of blocks processed by map
                # but asynchronisly so we can process the first one ourselves
                map_result = pool.map_async(multiUserFunction, mapArgs[1:])

            # do the first one ourselves so we aren't just waiting
            outputFirstBlock = multiUserFunction(mapArgs[0])
            outputBlockList = [outputFirstBlock]

            if len(mapArgs) > 1:
                # wait and get the results
                outputRestList = map_result.get(timeout=None)
                outputBlockList.extend(outputRestList)
            
            # write them out - need the input info as well
            for info, outputBlocks in zip(infoList, outputBlockList):
                writeOutputBlocks(writerdict, outfiles, outputBlocks, controls, info)
                lastpercent = updateProgress(controls, info, lastpercent)

    # exit the sub processes
    pool.close()
    pool.join()

    if controls.progress is not None:
        controls.progress.setProgress(100)

    closeOutputImages(writerdict, outfiles, controls)
