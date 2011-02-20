#!/usr/bin/env python

"""
Test harness for the new applier method. 
"""

from rios.applier import apply,FilenameAssociations
from rios.cuiprogress import CUIProgressBar
import optparse
import numpy
import sys

class CmdArgs(object):
  def __init__(self):
    self.parser = optparse.OptionParser()
    self.parser.add_option("--in",dest="input",help="Input file")
    self.parser.add_option("--out",dest="output",help="Output file")
    self.parser.add_option("--reference",dest="reference",help="Path to reference image")

    (options, self.args) = self.parser.parse_args()
    self.__dict__.update(options.__dict__)

# get command line args
cmdargs = CmdArgs()

# check we have some filenames
if cmdargs.input is None:
    cmdargs.parser.error("Must specify input filename")
    
if cmdargs.output is None:
    cmdargs.parser.error("Must specify output filename")
    
def myFunction(info, inputs, outputs):
    outputs.output = numpy.array([inputs.input[0] * 2])

inputs = FilenameAssociations()
inputs.input = cmdargs.input
outputs = FilenameAssociations()
outputs.output = cmdargs.output
progress = CUIProgressBar()

apply(myFunction, inputs, outputs, referenceImage=cmdargs.reference, progress=progress)
