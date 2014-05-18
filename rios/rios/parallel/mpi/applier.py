"""
An implementation os rios.applier that uses the mpi4py module
that allows parallel processing of blocks 
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
from mpi4py import MPI

from rios import imagereader

# import the bits we need from rios.applier
from rios.applier import FilenameAssociations, BlockAssociations, OtherInputs
from rios.applier import closeOutputImages, updateProgress, handleInputResampling
from rios.applier import writeOutputBlocks, separateVectors, opensAsRaster
from rios.applier import opensAsVector, makeVectorObjects, ApplierControls
# need these constants
from rios import imageio
INTERSECTION = imageio.INTERSECTION
UNION = imageio.UNION
BOUNDS_FROM_REFERENCE = imageio.BOUNDS_FROM_REFERENCE

def apply(userFunction, infiles, outfiles, otherArgs=None, controls=None):
    """
    Apply the given 'userFunction' to the given
    input and output files in a parallel manner using MPI.

    Number of processes your data will be run on will be controled
    by the -n switch to mpirun.
        
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
    # get the MPI info
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # if we are the 'root' process do all the preparation

        # Get default controls object if none given. 
        if controls is None:
            controls = ApplierControls()
        
        (imagefiles, vectorfiles) = separateVectors(infiles)
        reader = imagereader.ImageReader(imagefiles.__dict__, 
            controls.footprint, controls.windowxsize, controls.windowysize, 
            controls.overlap, controls.statscache, loggingstream=controls.loggingstream)

        vecreader = None
        if len(vectorfiles) > 0:
            vectordict = makeVectorObjects(vectorfiles, controls)
            vecreader = vectorreader.VectorReader(vectordict, progress=controls.progress)
        
        handleInputResampling(imagefiles, controls, reader)

        writerdict = {}
        
        if controls.progress is not None:
            controls.progress.setTotalSteps(100)
            controls.progress.setProgress(0)
        lastpercent = 0

        # get an iterator onto the reader object 
        # so we don't start at the beginning for each read
        # might be a more Pythonic way of doing this
        readeritr = reader.__iter__()

    # now code that gets run by all the processes

    done = False

    while not done:

        if rank == 0:
            # need to prepare the data
            data = []
            # so we can keep a track of the info for each
            infoList = []
            # now read in controls.nprocesses blocks
            while len(data) < size:
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
                    info.blocklookup = None # contains GDAL datasets
                    info.loggingstream = None # stderr can't be copied - reset in the subprocess
                    args = (userFunction, info, inputBlocks, otherArgs)
                    data.append(args)
                    infoList.append(info)

                except StopIteration:
                    done = True
                    break

            while len(data) < size:
                # we need to make sure data has size elements
                data.append(None)

        else:
            # all other ranks
            # magically gets filled in by MPI
            data = None

        # now issue scatter command 
        mydata = comm.scatter(data, root=0)

        if mydata is not None:
            # now mydata is the data for the current process to run
            # unpack info
            userFunction, info, inputBlocks, otherArgs = mydata

            outputBlocks = BlockAssociations()
            functionArgs = (info, inputBlocks, outputBlocks)
            if otherArgs is not None:
                functionArgs += (otherArgs, )
            # now set this to something sensible
            # (we set this to None in the main process as it
            # couldn't be serialised)
            info.loggingstream = sys.stdout

            # Now call the function with those args
            userFunction(*functionArgs)
        else:
            outputBlocks = None

        # now issue gather with the result
        outputBlockList = comm.gather(outputBlocks, root=0)

        if rank == 0:
            
            # write them out - need the input info as well
            for info, outputBlocks in zip(infoList, outputBlockList):
                writeOutputBlocks(writerdict, outfiles, outputBlocks, controls, info)
                lastpercent = updateProgress(controls, info, lastpercent)

        # broadcast out finished state
        # this should override the non-root values of done
        done = comm.bcast(done, root=0)

    if rank == 0:
        if controls.progress is not None:
            controls.progress.setProgress(100)

        closeOutputImages(writerdict, outfiles, controls)
