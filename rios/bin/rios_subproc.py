#!/usr/bin/env python
"""
Main program for RIOS subprocesses. 

"""
from __future__ import print_function

from rios import applier

import sys
import os
try:
    import cPickle as pickle
except ImportError:
    import pickle

def mainRoutine():
    """
    Main program
    """
    nArgs = len(sys.argv) - 1
    # use binary buffer objects
    # otherwise unpickling fails
    inf = sys.stdin.buffer
    outf = sys.stdout.buffer
    if nArgs >= 1:
        inf = open(sys.argv[1], 'rb')
    if nArgs == 2:
        outf = open(sys.argv[2], 'wb')

    # Read the pickled input
    (fn, a) = pickle.load(inf)
    
    # If using a disk file for input, close it and remove it now that we have read it
    if nArgs >= 1:
        inf.close()
        os.remove(sys.argv[1])
    
    info = a[0]
    inputs = a[1]
    # TODO: create blank output in general way
    outputs = applier.BlockAssociations()
    funcArgs = (info, inputs, outputs)
    if len(a) > 2:
        funcArgs += (a[2],)
    
    # Execute the function, with the given input data
    fn(*funcArgs)
    
    # Pickle and write out the output
    pickle.dump(outputs, outf)

if __name__ == "__main__":
    mainRoutine()
