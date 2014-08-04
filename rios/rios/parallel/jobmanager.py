"""
Possible JobManager for RIOS. Experimental......

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
import math
import abc
import copy
import subprocess
import tempfile
import time
try:
    import cPickle as pickle        # For Python 2.x
except ImportError:
    import pickle

import numpy

from .. import rioserrors
# Import a pickler which can pickle functions, with their dependencies, as well
# as data. First try to import the one from the full PiCloud package, in case
# that happens to be installed. If that fails, then import an earlier version of
# the same thing which we filched from the playdoh package, who had filched it 
# from cloud and made it work in isolation. 
try:
    from cloud.serialization import cloudpickle
except ImportError:
    # Import from our own local copy. This is what will usually happen. 
    from .cloud_playdoh import cloudpickle

class BlockAssociations(object):
    """
    Dummy class, to mimic applier.BlockAssociations, while avoiding circular imports. 
    """

class JobManager(object):
    """
    Manage breaking up of RIOS processing into sub-jobs, and farming them out. 
    
    Should be sub-classed to create new ways of farming out jobs. The sub-class 
    should at least over-ride the following abstract methods:
        startOneJob()
        waitOnJobs()
        gatherAllOutputs()
    More sophisticated sub-classes might also need to over-ride:
        startAllJobs()
    
    """
    __metaclass__ = abc.ABCMeta
    jobMgrType = None
    
    def __init__(self, numSubJobs):
        """
        numSubJobs is the number of sub-jobs
        """
        self.numSubJobs = numSubJobs
        self.margin = 0
    
    def setOverlapMargin(self, margin):
        """
        Set the overlap margin being used on this job manager. Generally 
        this will be set by applier.apply(), with the value it is using. 
        We just need to hang onto it locally. 
        
        """
        self.margin = margin
    
    def runSubJobs(self, function, functionArgs):
        """
        Take the given function arguments, break up the input arrays 
        into sub-arrays, and run the given function for each one, as 
        a separate asynchronous job. 
        
        """
        info = functionArgs[0]
        inputs = functionArgs[1]
        outputs = functionArgs[2]
        otherargs = None
        if len(functionArgs) == 4:
            otherargs = functionArgs[3]

        inputBlocksList = []
        infoList = []
        for i in range(self.numSubJobs):
            subInputs = self.replicateInputWithSubarrays(inputs, i)
            inputBlocksList.append(subInputs)
            infoList.append(self.getSubReaderInfo(info, i))
        
        jobIDlist = self.startAllJobs(function, inputBlocksList, infoList, otherargs)
        self.waitOnJobs(jobIDlist)
        
        outputBlocksList = self.gatherAllOutputs(jobIDlist)
        self.combineAllOutputs(outputBlocksList, outputs)
    
    def startAllJobs(self, function, inputBlocksList, infoList, otherargs):
        """
        Start up all of the jobs processing sub-arrays. Default implementation
        loops over the lists of jobs, starting each job separately. Keeps the
        first job aside, and runs it here before going off to wait for 
        the others. This means that the first element in the jobID list is not
        a jobID, but the results of the first sub-job. 
        
        """
        jobIDlist = [None]
        for i in range(1, self.numSubJobs):
            inputs = inputBlocksList[i]
            info = infoList[i]
            functionArgs = (info, inputs)
            if otherargs is not None:
                functionArgs += (otherargs, )
                
            jobID = self.startOneJob(function, functionArgs)
            jobIDlist.append(jobID)
        
        # Run the first one here
        inputs = inputBlocksList[0]
        info = infoList[0]
        outputs = BlockAssociations()
        functionArgs = (info, inputs, outputs)
        if otherargs is not None:
            functionArgs += (otherargs, )
        function(*functionArgs)
        jobIDlist[0] = outputs

        return jobIDlist
    
    @abc.abstractmethod
    def startOneJob(self, userFunc, functionArgs):
        """
        Start one job. Return a jobID object suitable for identifying the
        job, with all information required to wait for it, and 
        recover its output. This jobID is specific to the subclass. 
        
        This is an abstract method, and must be over-ridden in a sub-class.
        
        """
    
    @abc.abstractmethod
    def waitOnJobs(self, jobIDlist):
        """
        Wait until all the jobs in the given list have completed. This is
        an abstract method, and must be over-ridden in a sub-class. 
        
        """

    @abc.abstractmethod
    def gatherAllOutputs(self, jobIDlist):
        """
        Gather up outputs from sub-jobs, and return a list of the
        outputs objects. 
        This is an abstract method, and must be over-ridden in a sub-class. 
        
        """
    
    def combineAllOutputs(self, outputBlocksList, outputs):
        """
        Combine the list of output blocks into a single outputs object,
        by combining the sub-arrays into full arrays. 
        
        """
        # Get the attribute names from the first one
        nameList = outputBlocksList[0].__dict__.keys()
        for name in nameList:
            attrList = [getattr(b, name) for b in outputBlocksList]
            attr = self.combineOneOutput(attrList)
            setattr(outputs, name, attr)
    
    def combineOneOutput(self, attrList):
        """
        Combine separate sub-outputs for a given attribute into
        a single output. This could be either a single array,
        or a list of arrays, depending on what the user function 
        is working with. 
        
        """
        if isinstance(attrList[0], list):
            numOutBlocks = len(attrList[0])
            outBlockList = []
            for i in range(numOutBlocks):
                subArrList = [blockList[i] for blockList in attrList]
                fullArr = self.combineSubArrays(subArrList)
                outBlockList.append(fullArr)
            attr = outBlockList
        else:
            attr = self.combineSubArrays(attrList)
        return attr
    
    def combineSubArrays(self, subarrList):
        """
        Combine the given list of sub-arrays into a single full array. Each
        sub-array has shape
            (nBands, nRowsSub, nCols)
        and the sub-arrays are combined along the second dimension (i.e. rows are
        stacked together). Resulting array has shape
            (nBands, nRows, nCols)
        
        Note that we must honour the margin, except for the first and last 
        sub-arrays. 
        
        """
        # First remove the margin
        subarrList_nomargin = []
        numSubarr = len(subarrList)
        for i in range(numSubarr):
            subArrShape = subarrList[i].shape
            startRow = 0
            if i > 0:
                startRow += self.margin
            stopRow = subArrShape[1]
            if i < (numSubarr-1):
                stopRow -= self.margin
            subarr = subarrList[i][:, startRow:stopRow, :]
            
            subarrList_nomargin.append(subarr)
        fullArr = numpy.concatenate(subarrList_nomargin, axis=1)
        return fullArr

    def replicateInputWithSubarrays(self, inputs, i):
        """
        Replicate the given BlockAssociations object, but with a
        set of sub-arrays in place of the full arrays. The sub-arrays
        are the i-th sub-arrays of a total of n. Note that i begins with zero. 
        
        The margin argument is the value the margin being added for
        overlapping blocks, and is honoured in the sub-arrays, too, so
        that they can still be used for processing which requires it. 
        
        """
        newInputs = BlockAssociations()
        
        nameList = inputs.__dict__.keys()
        for name in nameList:
            attr = getattr(inputs, name)
            if isinstance(attr, list):
                arrList = [self.getSubArray(arr, i) for arr in attr]
                setattr(newInputs, name, arrList)
            else:
                setattr(newInputs, name, self.getSubArray(attr, i))
        return newInputs
    
    def getSubArray(self, fullArr, i):
        """
        Use getSubArraySlice() to slice out a sub-array. See docstring
        for getSubArraySlice() for details. 
        """
        (nBands, nRows, nCols) = fullArr.shape
        s = self.getSubArraySlice(nRows, i)
        return fullArr[:, s, :]
    
    def getSubArraySlice(self, nRows, i):
        """
        Return a slice which will select the i-th subarray (slicing only the rows)
        out of a total of self.numSubJobs sub-arrays, from the given nRows. 
        Note that i begins with zero. 
        
        The given margin is the margin being added for overlapping blocks. This 
        must be honoured in between the sub-arrays, but not before the first one 
        or after the final one. 
        
        Note that, in general terms, because the sub-array is created as 
        a slice of the original arrays, they will share the data, as
        this does not actually create a new copy of the array data. 
        
        """
        rowsPerPiece = int(math.ceil(float(nRows) / self.numSubJobs))
        
        # startRow:stopRow is to be a slice in the fullArr
        startRow = i * rowsPerPiece
        stopRow = (i+1) * rowsPerPiece
        
        # Cope with the margin
        startRow = max(0, (startRow - self.margin))
        stopRow = min(nRows, (stopRow + self.margin))
        
        # Generate the slice
        return slice(startRow, stopRow)
    
    def getSubReaderInfo(self, info, i):
        """
        Return a ReaderInfo object which replicates the given info object,
        but with various bounds changed to match the i-th subarray. 
        
        This is more complicated that one might think, probably because 
        the ReaderInfo object contains far more information than it ought.
        However, in the interests of full compatibility, we have to do everything. 
        
        There are some features of the ReaderInfo object which are never going to 
        work in this context. Notably, looking things up by block id (the block id's 
        will be wrong when shifted to a different subprocess). Fortunately, this
        is one of the things which probably should never be used anyway. 
        
        TODO: Until I get around to fixing it, the really useful stuff 
        like info.getPixRowColBlock() and info.getBlockCoordArrays() also
        won't work when running sub-jobs. However this is do-able, and just waiting 
        on me. 
        
        """
        # Make a shallow copy, assuming the objects pointed to by this are
        # not functions of the block (which I think is true, but I could be wrong)
        newInfo = copy.copy(info)
        
        newInfo.blocklookup = {}       # These are never going to work in the subprocess
        newInfo.loggingstream = None    # Should not be using this from userFunc anyway
        
        # Need to recalculate corners, etc.
        #(nRows, nCols) = info.getBlockSize()
        #s = self.getSubArraySlice(nRows, i)
        #nRowsSub = s.stoprow - s.startrow
        
        #newInfo.blockheight = nRowsSub
        # This is not yet finished.........
        
        return newInfo
    
    def __str__(self):
        """
        String representation
        """
        return "jobMgrType=%s, numSubJobs=%s, margin=%s" % (self.jobMgrType, self.numSubJobs, 
            self.margin)


class SubprocJobManager(JobManager):
    """
    Use Python's standard subprocess module to run individual jobs. 
    Passes input and output to/from the subprocesses using their 
    stdin/stdout. The command being executed is a simple main program
    which runs the user function on the given data, and passes back
    the resulting outputs object. 
    
    This JobManager sub-class should be used with caution, as it does not 
    involve any kind of load balancing, and all sub-processes simply run 
    concurrently. If you have enough spare cores and memory to do that, then
    no problem, but if not, you may clog the system. 
    
    """
    jobMgrType = "subproc"
    
    def startOneJob(self, userFunc, functionArgs):
        """
        Start one job. We execute the rios_subproc.py command,
        communicating via its stdin/stdout. We give it the pickled
        function and all input objects, and we get back a pickled
        outputs object. 
        
        """
        allInputs = (userFunc, functionArgs)
        allInputsPickled = cloudpickle.dumps(allInputs)
        
        proc = subprocess.Popen(['rios_subproc.py'], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        proc.stdin.write(allInputsPickled)
        return proc
    
    def waitOnJobs(self, jobIDlist):
        """
        Wait until all the jobs in the given list have completed. This
        implementation doesn't wait at all, because the subprocesses may
        block on writing their output to the stdout pipe. So, we do  
        nothing here, and actually wait on the read of the stdout from
        the subprocesses. 
        
        """

    def gatherAllOutputs(self, jobIDlist):
        """
        Gather up outputs from sub-jobs, and return a list of the
        outputs objects. Note that we assume that the first element of
        jobIDlist is actually an outputs object, from running the first sub-array
        in the current process. 
        
        """
        outputBlocksList = [jobIDlist[0]]
        for proc in jobIDlist[1:]:
            pickledOutput = proc.stdout.read()
            outputObj = pickle.loads(pickledOutput)
            outputBlocksList.append(outputObj)
        return outputBlocksList


class PbsJobManager(JobManager):
    """
    Use PBS to run individual jobs
    
    """
    jobMgrType = "pbs"
    
    def startOneJob(self, userFunc, functionArgs):
        """
        Start one job. We create a shell script to submit to a PBS batch queue.
        When executed, the job will execute the rios_subproc.py command, giving
        it the names of two pickle files. The first is the pickle of all inputs
        (including the function), and the second is where it will write the 
        pickle of outputs. 
        
        Uses $RIOS_PBSJOBMGR_QSUBOPTIONS to pick up any desired options to the 
        qsub command. This should be used to control such things as requested 
        amount of memory or walltime for each job, which will otherwise be
        defaulted by PBS. 
        
        """
        allInputs = (userFunc, functionArgs)
        allInputsPickled = cloudpickle.dumps(allInputs)
        
        (fd, inputsfile) = tempfile.mkstemp(prefix='rios_pbsin_', dir='.', suffix='.tmp')
        os.close(fd)
        outputsfile = inputsfile.replace('pbsin', 'pbsout')
        scriptfile = inputsfile.replace('pbsin', 'pbs').replace('.tmp', '.sh')
        logfile = outputsfile.replace('.tmp', '.log')
        
        qsubOptions = os.getenv('RIOS_PBSJOBMGR_QSUBOPTIONS')
        
        scriptCmdList = [
            "#!/bin/bash",
            "#PBS -j oe -o %s" % logfile
        ]
        if qsubOptions is not None:
            scriptCmdList.append("#PBS %s" % qsubOptions)
            
        pbsInitCmds = os.getenv('RIOS_PBSJOBMGR_INITCMDS')
        if pbsInitCmds is not None:
            scriptCmdList.append(pbsInitCmds)
            
        scriptCmdList.append("rios_subproc.py %s %s"%(inputsfile, outputsfile))
        scriptStr = '\n'.join(scriptCmdList)
        
        open(scriptfile, 'w').write(scriptStr+'\n')
        open(inputsfile, 'w').write(allInputsPickled)
        
        submitCmdWords = ["qsub", scriptfile]
        proc = subprocess.Popen(submitCmdWords, stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
        # The qsub command exits almost immediately, printing the PBS job id
        # to stdout. So, we just wait for the qsub to finish, and grab the
        # jobID string.     
        (stdout, stderr) = proc.communicate()
        pbsJobID = stdout.strip()
        
        # Remove the script file, assuming that qsub took a copy of it. 
        os.remove(scriptfile)

        # If there was something in stderr from the qsub command, then probably 
        # something bad happened, so we pass it on to the user in the form of
        # an exception. 
        if len(stderr) > 0:
            msg = "Error from qsub. Message:\n"+stderr
            raise rioserrors.JobMgrError(msg)
        
        return (pbsJobID, outputsfile)
    
    def waitOnJobs(self, jobIDlist):
        """
        Wait until all jobs in the given list have completed. The jobID values
        are tuples whose first element is a PBS job id string. We poll the PBS 
        queue until none of them are left in the queue, and then return. 
        
        Note that this also assumes the technique used by the default startAllJobs()
        method, of executing the first job in the current process, and so the first
        jobID is not a jobID but the results of that. Hence we do not try to wait on
        that job, but on all the rest. 
        
        Returns only when all the listed jobID strings are not longer found in the
        PBS queue. Currently has no time-out, although perhaps it should. 
        
        """
        allFinished = False
        
        # Extract the actual PBS job ID strings, skipping the first element. 
        # Express as a set, for efficiency later on
        pbsJobIdSet = set([jobID for (jobID, outputsfile) in jobIDlist[1:]])
        
        while not allFinished:
            qstatCmd = ["qstat"]
            proc = subprocess.Popen(qstatCmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (stdout, stderr) = proc.communicate()
            
            stdoutLines = [line for line in stdout.split('\n') if len(line) > 0]   # No blank lines
            # Skip header lines, and grab first word on each line, which is jobID
            qstatJobIDlist = [line.split()[0] for line in stdoutLines[2:]]
            qstatJobIDset = set(qstatJobIDlist)
            
            allFinished = pbsJobIdSet.isdisjoint(qstatJobIDset)
            
            if not allFinished:
                # Sleep for a bit before checking again
                time.sleep(60)
    
    def gatherAllOutputs(self, jobIDlist):
        """
        Gather up outputs from sub-jobs, and return a list of the
        outputs objects. Note that we assume that the first element of
        jobIDlist is actually an outputs object, from running the first sub-array
        in the current process. 
        
        The jobIDlist is a list of tuples whose second element is the name of 
        the output file containing the pickled outputs object. 
        
        """
        outputBlocksList = [jobIDlist[0]]
        for (jobID, outputsfile) in jobIDlist[1:]:
            try:
                pickledOutput = open(outputsfile).read()
                outputObj = pickle.loads(pickledOutput)
                os.remove(outputsfile)
            except Exception as e:
                msg = "Error collecting output from PBS sub-job. Exception message:\n"+str(e)
                raise rioserrors.JobMgrError(msg)
            outputBlocksList.append(outputObj)
        return outputBlocksList
        
    
class SlurmJobManager(JobManager):
    """
    Use SLURM to run individual jobs
    
    """
    jobMgrType = "slurm"
    
#class MpiJobManager(JobManager):
#    """
#    Use MPI to run individual jobs. Requires mpi4py module. 
#    
#    """
#    jobMgrType = "mpi"
#    
#class MultiJobManager(JobManager):
#    """
#    Use Python's standard multiprocessing module to run individual jobs
#    
#    """
#    jobMgrType = "multiprocessing"


# This mechanism for selecting which job manager sub-class to use is important in 
# order to allow an application to run without modification on different systems.
# Our own example is that JRSRP has a system which uses PBS and another which
# uses Slurm, and we want the applications to run the same on both, which means
# that there should be a way of selecting this from the environment. 
DEFAULT_JOBMGRTYPE = os.getenv('RIOS_DFLT_JOBMGRTYPE')
def getJobManagerClassByType(jobMgrType=DEFAULT_JOBMGRTYPE):
    """
    Return a sub-class of JobManager, selected by the type name
    given. The default jobMgrType is loaded from the environment variable
        $RIOS_DFLT_JOBMGRTYPE
    All sub-classes of JobManager will be searched for the 
    given jobMgrType string. 
        
    """
    jobMgr = None
    subClasses = JobManager.__subclasses__()
    for c in subClasses:
        if c.jobMgrType == jobMgrType:
            jobMgr = c
    return jobMgr


def getAvailableJobManagerTypes():
    """
    Return a list of currently known job manager types
    
    """
    subClasses = JobManager.__subclasses__()
    typeList = [c.jobMgrType for c in subClasses]
    return typeList

