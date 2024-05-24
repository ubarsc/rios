"""
Does a basic test of rios concurrency, using the CW_SUBPROC option. This
is a simple way to emulate most of the requirements of the batch-oriented
concurrency options, without requiring all the batch infrastructure.

Generates a pair of images, and then applies a function to calculate
the average of them. Checks the resulting output against a known 
correct answer. 

Steals heavily from testavg
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
from __future__ import print_function
import os
from multiprocessing import cpu_count

import numpy
from osgeo import gdal

from rios import applier, structures
from . import riostestutils

TESTNAME = "TESTAVGSUBPROC"

TEST_NCPUS = 2


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    if structures.cloudpickle is None:
        riostestutils.report(TESTNAME, "Skipped, as cloudpickle unavailable")
        return True
    
    ramp1 = 'ramp1.img'
    ramp2 = 'ramp2.img'
    riostestutils.genRampImageFile(ramp1)
    riostestutils.genRampImageFile(ramp2, reverse=True)
    outfile = 'rampavg.img'

    try:
        calcAverage(ramp1, ramp2, outfile)
        ok = checkResult(outfile)
    finally:
        # Clean up, even when an exception raised
        for filename in [ramp1, ramp2, outfile]:
            if os.path.exists(filename):
                try:
                    riostestutils.removeRasterFile(filename)
                except Exception:
                    pass
    
    return ok


def calcAverage(file1, file2, avgfile):
    """
    Use RIOS to calculate the average of two files using MPI.

    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    infiles.img = [file1, file2]
    outfiles.avg = avgfile

    controls = applier.ApplierControls()
    numComputeWorkers = min(2, cpu_count())
    conc = structures.ConcurrencyStyle(numReadWorkers=1,
        numComputeWorkers=numComputeWorkers,
        computeWorkerKind=structures.CW_SUBPROC)
    controls.setConcurrencyStyle(conc)

    applier.apply(doAvg, infiles, outfiles, controls=controls)


def doAvg(info, inputs, outputs):
    """
    Called from RIOS.
    
    Calculate the average of the input files. 
    
    """
    tot = inputs.img[0].astype(numpy.float32)
    for img in inputs.img[1:]:
        tot += img
    outputs.avg = (tot / len(inputs.img)).astype(numpy.uint8)


def checkResult(avgfile):
    """
    Read in from the given file, and check that it matches what we 
    think it should be
    """
    # Work out the correct answer
    ramp1 = riostestutils.genRampArray()
    ramp2 = riostestutils.genRampArray()[:, ::-1]
    tot = (ramp1.astype(numpy.float32) + ramp2)
    avg = (tot / 2.0).astype(numpy.uint8)
    
    # Read what RIOS wrote
    ds = gdal.Open(avgfile)
    band = ds.GetRasterBand(1)
    riosavg = band.ReadAsArray()
    del ds
    
    # Check that they are the same
    ok = True
    if avg.shape != riosavg.shape:
        riostestutils.report(TESTNAME, "Shape mis-match: %s != %s"%(avg.shape, riosavg.shape))
        ok = False
    elif (riosavg - avg).any():
        riostestutils.report(TESTNAME, "Incorrect result. Average difference = %s"%(riosavg - avg).mean())
        ok = False
    else:
        riostestutils.report(TESTNAME, "Passed")

    return ok


if __name__ == "__main__":
    run()
