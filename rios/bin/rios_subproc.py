#!/usr/bin/env python
"""
Main program for RIOS subprocesses. 

"""
from __future__ import print_function

from rios import applier

import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle

def mainRoutine():
    """
    Main program
    """
    nArgs = len(sys.argv) - 1
    inf = sys.stdin
    outf = sys.stdout
    if nArgs >= 1:
        inf = open(sys.argv[1])
    if nArgs == 2:
        outf = open(sys.argv[2], 'w')

    (fn, a) = pickle.load(inf)
    info = a[0]
    inputs = a[1]
    outputs = applier.BlockAssociations()
    funcArgs = (info, inputs, outputs)
    if len(a) > 2:
        funcArgs += (a[2],)
    
    fn(*funcArgs)
    
    pickle.dump(outputs, outf)

if __name__ == "__main__":
    mainRoutine()
