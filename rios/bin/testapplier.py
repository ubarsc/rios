#!/usr/bin/env python
"""
A test of the applier.apply() function, testing a number of its features. 
Includes multiple input and output files, and arbitrary lists of input 
and output files. 

This program takes an arbitrary number of input files, and writes
an output for the sum of all the inputs, and one output for each input
which is the normalised value of that input, normalised to the sum of all.
Thus the normalised outputs will all sum to 1.0. 

"""
import optparse

import numpy

from rios import applier

def myfunc(info, inputs, outputs):
    # The 'files' attribute is a list of inputs
    # Add them up. 
    filesSum = numpy.zeros(inputs.files[0].shape, dtype=numpy.float32)
    for block in inputs.files:
        filesSum += block
    outputs.filesSum = filesSum
    
    # We create a list in the outputs, which is each of the 
    # inputs, normalized by the sum
    outputs.filesNormed = []
    for block in inputs.files:
        normed = block / filesSum
        outputs.filesNormed.append(normed)


def doit(cmdargs):
    inFiles = applier.FilenameAssociations()
    outFiles = applier.FilenameAssociations()
    
    inFiles.files = cmdargs.files
    outFiles.filesSum = cmdargs.sum
    outFiles.filesNormed = cmdargs.normedfiles
    
    applier.apply(myfunc, inFiles, outFiles)


class CmdArgs:
    def __init__(self):
        p = optparse.OptionParser()
        p.add_option("--infile", dest="files", default=[], action="append")
        p.add_option("--sum", dest="sum")
        p.add_option("--normed", dest="normedfiles", default=[], action="append")
        (options, args) = p.parse_args()
        self.__dict__.update(options.__dict__)


if __name__ == "__main__":
    cmdargs = CmdArgs()
    doit(cmdargs)
