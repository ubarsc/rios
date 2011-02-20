#!/opt/osgeo/fw/python/2.6.6/bin/python

"""
Test harness for the new PyModeller ImageReader/ImageWriter
classes. 
"""

from rios.imagereader import ImageReader
from rios.imagewriter import ImageWriter
import optparse
import numpy
import sys

class CmdArgs(object):
  def __init__(self):
    self.parser = optparse.OptionParser()
    self.parser.add_option("--out",dest="out",help="Print global statistics")
    self.parser.add_option("--reference",dest="reference",help="Path to reference image")

    (options, self.args) = self.parser.parse_args()
    self.__dict__.update(options.__dict__)

# get command line args
cmdargs = CmdArgs()

# check we have some filenames
if len(cmdargs.args) == 0:
    cmdargs.parser.error("Must specify input filenames")
    
if cmdargs.out is None:
    cmdargs.parser.error("Must specify output filename")
    
# create ImageReader instance
reader = ImageReader(cmdargs.args)

# have they given us a reference dataset
if cmdargs.reference is not None:
    reader.allowResample(refpath=cmdargs.reference)
    
# create the output image as None until we have an info object
writer = None

# now read thru the image(s) adding them up
for (info,blocklist) in reader:

    # get the size of the current block and 
    # create an empty array
    (blockxsize,blockysize) = info.getBlockSize()
    outblock = numpy.zeros((1,blockysize,blockxsize),numpy.int32)

    (nxblocks,nyblocks) = info.getTotalBlocks()
    (xblock,yblock) = info.getBlockCount()
    print 'Block', nxblocks * yblock + xblock + 1, 'of', nxblocks * nyblocks

    for block in blocklist:
    
        # just add the first band
        outblock = outblock + block[0]
    
    # have we already created the writer?
    if writer is None:
        
        # no, create it with the info object, and the firstblock
        writer = ImageWriter(cmdargs.out, info=info, firstblock=outblock)
        
    else:
        # yep, just write this block
        writer.write(outblock)
        
    sys.stdout.write("%d%%\n" % info.getPercent())
            
writer.close(calcStats=True)
