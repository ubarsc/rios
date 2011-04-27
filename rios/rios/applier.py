#!/usr/bin/env python
"""
Basic tools for setting up a function to be applied over 
a raster processing chain. 

"""

from . import imagereader
from . import imagewriter
from . import rioserrors

class FilenameAssociations(object): pass
class BlockAssociations(object): pass
class OtherInputs(object): pass

def apply(userFunction, infiles, outfiles, otherArgs=None, progress=None, 
                referenceImage=None,footprint=imagereader.DEFAULTFOOTPRINT,
                windowxsize=imagereader.DEFAULTWINDOWXSIZE, windowysize=imagereader.DEFAULTWINDOWYSIZE,
                overlap=imagereader.DEFAULTOVERLAP, statscache=None, drivername=imagewriter.DEFAULTDRIVERNAME,
                creationoptions=imagewriter.DEFAULTCREATIONOPTIONS,
                calcStats=True,statsIgnore=0):
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
        with the same names as those given in thee infiles and outfiles 
        objects. In the inputs and outputs objects, available inside 
        userFunction, these attributes contain numpy arrays of data read 
        from/written to the corresponding image file. 
        
        The numpy arrays are always 3-d arrays, with shape
            (numBands, numRows, numCols)
        The datatype of the output image(s) is determined directly
        from the datatype of the numpy arrays in the outputs object. 
        
        The info object contains many useful details about the processing, 
        and will always be passed to the userFunction. It can, of course, 
        be ignored. It is an instance of the readerinfo.ReaderInfo class. 
        
        referenceImage defines which of the input images will be used to 
        supply the pixel grid on which numpy arrays will be supplied. 
        If None, then no reprojecting will be allowed. 
        
        footprint is one of imagereader.INTERSECTION or imagereader.UNION. 
        
        windowxsize and windowysize define the size of the blocks in which 
        data is processed. When a value is given for overlap, the blocks 
        will actually be larger by this much, to allow them to overlap, 
        for use with focal operations such as local mean. 
        
        drivername and creationoptions are given to GDAL to control how 
        output files are created. 
        
        calcStats controls whether statistics are calculated on the output 
        file(s). When True, stats (and pyramid layers) will be saved on the
        output files. The given statsIgnore value will be used for calculation
        of statistics, and a statscache object can be given if these values
        are already known from some other source. 
        
        """
        
        inputBlocks = BlockAssociations()
        outputBlocks = BlockAssociations()
        reader = imagereader.ImageReader(infiles.__dict__, footprint, windowxsize, windowysize, overlap, statscache)

        if referenceImage is not None:
            reader.allowResample(refpath=referenceImage)

        writerdict = {}
        
        if progress is not None:
            progress.setTotalSteps(100)
            progress.setProgress(0)
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
                if len(writerdict) == 0:
                    writer = imagewriter.ImageWriter(outfiles.__dict__[name],info=info,firstblock=outblock,
                                            drivername=drivername,creationoptions=creationoptions)
                    writerdict[name] = writer
                else:
                    writerdict[name].write(outblock)
                    
            if progress is not None:
                percent = info.getPercent()
                if percent != lastpercent:
                    progress.setProgress(percent)
                    lastpercent = percent
                
        if progress is not None:
            progress.setProgress(100)    
                
        for name in outfiles.__dict__.keys():
            writerdict[name].close(calcStats,statsIgnore,progress)
        


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


