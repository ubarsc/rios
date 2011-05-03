#!/usr/bin/env python

"""
Test harness for the new PyModeller ImageReader
class. 
"""

from rios.imagereader import ImageReader
import optparse
import numpy
import sys
import os

class CmdArgs(object):
  def __init__(self):
    self.parser = optparse.OptionParser()
    self.parser.add_option("--in",dest="input",help="input file")
    self.parser.add_option("--globalstats",dest="globalstats",action="store_true",default=False,help="Print global statistics")

    (options, self.args) = self.parser.parse_args()
    self.__dict__.update(options.__dict__)

def print_report(info,block,globalstats):

    fname = info.getFilenameFor(block)
        
    for layer in range(block.shape[0]):
        median = numpy.mean(block[layer])
        print(" image %s, layer %d, mean = %f" % (fname,layer,median))
            
        if globalstats:
            globalmean = info.global_mean(block,layer+1)
            print("  global mean = %f" % globalmean)


# get command line args
cmdargs = CmdArgs()

if cmdargs.input is None:
    raise SystemExit("Must pass input file")

reader = ImageReader(cmdargs.input)

# now read thru the image
for (info,block) in reader:

    # print info about the block
    (blocktl,blockbr) = info.getBlockBounds()
    (xblock,yblock) = info.getBlockCount()
    print("block %d %d starting at %fE %fN" % (xblock,yblock,blocktl.x,blocktl.y))
    
    print_report( info, block, cmdargs.globalstats) 
    sys.stdout.write("%d%%\n" % info.getPercent())
        
