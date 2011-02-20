#!/usr/bin/env python
"""
Basic tools for setting up a function to be applied over 
a raster processing chain. 

"""

import imagereader
import imagewriter
import exceptions

class FilenameAssociations(object): pass
class BlockAssociations(object): pass
class OtherInputs(object): pass

def apply(userFunction, inputs, outputs, otherArgs=None, progress=None, 
                referenceImage=None,footprint=imagereader.DEFAULTFOOTPRINT,
                windowxsize=imagereader.DEFAULTWINDOWXSIZE, windowysize=imagereader.DEFAULTWINDOWYSIZE,
                overlap=imagereader.DEFAULTOVERLAP, statscache=None, drivername=imagewriter.DEFAULTDRIVERNAME,
                creationoptions=imagewriter.DEFAULTCREATIONOPTIONS,
                calcStats=True,statsIgnore=0):
        """
        Apply the 'thefunc' method of this class to the given
        input and output files. 
        
        inputs and outputs are FilenameAssociations objects to 
        define associations between internal variable names and
        external filenames, for the raster file inputs. 
        
        args is an object of extra arguments to be passed to thefunc,
        each with a sensible name on the object. 
        
        """
        
        inputBlocks = BlockAssociations()
        outputBlocks = BlockAssociations()
        reader = imagereader.ImageReader(inputs.__dict__, footprint, windowxsize, windowysize, overlap, statscache)

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
            
            for name in outputs.__dict__.keys():

                if not outputBlocks.__dict__.has_key(name):
                    msg = 'Output key %s not found in output blocks' % name
                    raise exceptions.KeysMismatch(msg)

                outblock = outputBlocks.__dict__[name]
                if len(writerdict) == 0:
                    writer = imagewriter.ImageWriter(outputs.__dict__[name],info=info,firstblock=outblock,
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
                
        for name in outputs.__dict__.keys():
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


