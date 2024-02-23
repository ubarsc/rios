"""
Test the operation of the apply() return object, running with
multiple compute threads.



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
import numpy
from osgeo import gdal
from rios import applier, structures

from . import riostestutils

TESTNAME = "TESTAPPLYRETURN"


def run():
    """
    Run the test
    """
    riostestutils.reportStart(TESTNAME)
    
    ramp1 = 'ramp1.img'
    riostestutils.genRampImageFile(ramp1)
    
    avg_threads = calcAverage(ramp1, structures.CW_THREADS)
    avg_subproc = calcAverage(ramp1, structures.CW_SUBPROC)
    
    ok = checkResult(avg_threads, avg_subproc)
    
    # Clean up
    for filename in [ramp1]:
        riostestutils.removeRasterFile(filename)
    
    return ok


def calcAverage(file1, cwKind):
    """
    Use RIOS to calculate the average value over the file.
    """
    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    infiles.img = file1
    otherargs = applier.OtherInputs()
    otherargs.sum = 0
    otherargs.num = 0
    controls = applier.ApplierControls()
    controls.setWindowXsize(100)
    controls.setWindowYsize(100)
    conc = structures.ConcurrencyStyle(numComputeWorkers=4,
        computeWorkerKind=cwKind, numReadWorkers=1)
    controls.setConcurrencyStyle(conc)
    
    rtn = applier.apply(doSums, infiles, outfiles, otherargs, controls=controls)

    tot = sum([oa.sum for oa in rtn.otherArgsList])
    num = sum([oa.num for oa in rtn.otherArgsList])
    avg = tot / num
    return avg


def doSums(info, inputs, outputs, otherargs):
    """
    Called from RIOS.
    
    Calculate the sum and count of all pixels in each block. 
    
    """
    otherargs.sum += inputs.img[0].sum()
    otherargs.num += inputs.img[0].size


def checkResult(avg_threads, avg_subproc):
    """
    Read in from the given file, and check that it matches what we 
    think it should be
    """
    # Work out the correct answer
    ramp1 = riostestutils.genRampArray()
    avg_numpy = ramp1.mean()
    
    # Check that they are the same
    ok = True
    if avg_threads != avg_numpy:
        msg = ("Incorrect result. RIOS (CW_THREADS) gives {}, " +
            "numpy gives {}").format(avg_threads, avg_numpy)
        riostestutils.report(TESTNAME, msg)
            
        ok = False
    if avg_subproc != avg_numpy:
        msg = ("Incorrect result. RIOS (CW_SUBPROC) gives {}, " +
            "numpy gives {}").format(avg_subproc, avg_numpy)
        riostestutils.report(TESTNAME, msg)
            
        ok = False

    if ok:
        riostestutils.report(TESTNAME, "Passed")

    return ok
