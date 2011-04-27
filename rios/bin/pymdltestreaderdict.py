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
    self.parser.add_option("--reference",dest="reference",help="Path to reference image")
    self.parser.add_option("--globalstats",dest="globalstats",action="store_true",default=False,help="Print global statistics")

    (options, self.args) = self.parser.parse_args()
    self.__dict__.update(options.__dict__)

def print_report(name,info,block,globalstats):

    fname = info.getFilenameFor(block)
        
    for layer in range(block.shape[0]):
        median = numpy.mean(block[layer])
        print(" image %s %s, layer %d, mean = %f" % (name,fname,layer,median))
            
        if globalstats:
            globalmean = info.global_mean(block,layer+1)
            print("  global mean = %f" % globalmean)


# get command line args
cmdargs = CmdArgs()

# check we have some filenames
if len(cmdargs.args) == 0:
    cmdargs.parser.error("Must specify input filenames")

# create ImageReader instance
imageDict = {}
for fname in cmdargs.args:
    name = os.path.basename(fname)
    imageDict[name] = fname
    
reader = ImageReader(imageDict)

# have they given us a reference dataset
if cmdargs.reference is not None:
    reader.allowResample(refpath=cmdargs.reference)
    
# now read thru the image(s)
for (info,blockdict) in reader:

    # print info about the block
    (blocktl,blockbr) = info.getBlockBounds()
    (xblock,yblock) = info.getBlockCount()
    print("block %d %d starting at %fE %fN" % (xblock,yblock,blocktl.x,blocktl.y))
    
    # now go thru each block and print something
    for key in list(blockdict.keys()):
    
        block = blockdict[key]
        print_report(key, info, block, cmdargs.globalstats) 
        sys.stdout.write("%d%%\n" % info.getPercent())
        
