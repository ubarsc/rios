"""
Base class and sub-classes for managing parallel processing in RIOS. 

It should be emphasised at the start that it is only worth using
parallel processing in RIOS for tasks which are very computationally
intensive, as there is significant overhead in setting up the sub-jobs. 
Most image processing is I/O bound, and will not benefit from parallel 
processing. 

The base class is JobManager(). This is an abstract base class,
and must be sub-classed before use. Any sub-class is intended to manage 
processing of the user function in a set of sub-jobs, splitting the
arrays into sub-arrays, farming out sub-jobs to process them all 
in parallel, and gathering up the results, and re-combining into a
single set of outputs. 

Most of this work is handled in the base class, and should be generic
for different methods of parallel processing. The reason for the
sub-classes is to allow different approaches to be used, depending on 
the system configuration. In particular, one can use a cluster batch 
queue system such as PBS or SLURM to run sub-jobs as independent jobs, 
allowing it to manage scheduling of jobs and resource management. 
Alternatively, one can use MPI or Python's own multiprocessing module,
if this is more appropriate for the system configuration available. 

Sub-classes are provided for using PBS, SLURM, MPI, multiprocessing
or Python's native subprocess module. Other sub-classes can be made
as required, outside this module, and will be visible to the function
    getJobManagerClassByName()
which is the main function used for selecting which sub-class is required.

The calling program controls the parallel processing through the
ApplierControls() object. Normal usage would be as follows:
    from rios import applier
    controls = applier.ApplierControls()
    controls.setNumThreads(5)

If a custom JobManager sub-class is used, its module should be imported 
into the calling program (in order to create the sub-class), but its use 
is selected using the same call to controls.setJobManagerType(), giving 
the jobMgrType of the custom sub-class. 

If $RIOS_DFLT_JOBMGRTYPE is set, this will be used as the default jobMgrType.
This facilitates writing of application code which can run unmodified on 
systems with different configurations. Alternatively, this can be set on
the controls object, e.g.
    controls.setJobManagerType('pbs')
    
Environment Variables
---------------------
    RIOS_DFLT_JOBMGRTYPE                Name string of default JobManager subclass
    RIOS_PBSJOBMGR_QSUBOPTIONS          String of commandline options to be used with PBS qsub.
                                        Use this for things like walltime and queue name. 
    RIOS_PBSJOBMGR_INITCMDS             String of shell command(s) which will be executed 
                                        inside each PBS job, before executing the
                                        processing commands. Not generally required, but was
                                        useful for initial testing. 

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
# as data. Either use the version installed with cloudpickle
# (https://github.com/cloudpipe/cloudpickle) or the bundled versin
try:
    from cloudpickle import cloudpickle
except ImportError:
    # Import from our own local copy. This is what will usually happen. 
    from . import cloudpickle

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
    
    A sub-class must also include a class attribute called jobMgrType, which has 
    string value, which is the name used to select this sub-class. 
    
    """
    __metaclass__ = abc.ABCMeta
    jobMgrType = None
    
    def __init__(self, numSubJobs):
        """
        numSubJobs is the number of sub-jobs
        """
        self.numSubJobs = numSubJobs
        self.margin = 0
        self.tempdir = '.'
    
    def setOverlapMargin(self, margin):
        """
        Set the overlap margin being used on this job manager. Generally 
        this will be set by applier.apply(), with the value it is using. 
        We just need to hang onto it locally. 
        
        """
        self.margin = margin
    
    def setTempdir(self, tempdir):
        """
        Directory to use for temporary files. This is generally set by apply(),
        using the one it has been given on the ApplierControls object. The
        default is '.'. 
        
        """
        self.tempdir = tempdir
    
    def runSubJobs(self, function, fnInputs):
        """
        Take the given list of function arguments, run the given function 
        for each one, as a separate asynchronous job. 

        Returns a list of output BlockAssociations.
        
        """
        jobIDlist = self.startAllJobs(function, fnInputs)
        self.waitOnJobs(jobIDlist)
        
        outputBlocksList = self.gatherAllOutputs(jobIDlist)
        return outputBlocksList
    
    def startAllJobs(self, function, fnInputs):
        """
        Start up all of the jobs processing blocks. Default implementation
        loops over the lists of jobs, starting each job separately. Keeps the
        first job aside, and runs it here before going off to wait for 
        the others. This means that the first element in the jobID list is not
        a jobID, but the results of the first sub-job. 
        
        """
        jobIDlist = [None]
        for i in range(1, self.numSubJobs):
            inputs = fnInputs[i]
                
            jobID = self.startOneJob(function, inputs)
            jobIDlist.append(jobID)
        
        # Run the first one here
        inputs = fnInputs[0]
        function(*inputs)

        # TODO: determine output in a RIOS independent way
        outputs = inputs[2]
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
        # TODO: Can't pickle GDAL datasets. Need to find a 
        # generic way of doing this...
        info = functionArgs[0]
        info.blocklookup = {}
        # explude output. TODO: generic way
        newFunctionArgs = (info,) + functionArgs[1:-1]

        allInputs = (userFunc, newFunctionArgs)
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
        
        (fd, inputsfile) = tempfile.mkstemp(prefix='rios_pbsin_', dir=self.tempdir, suffix='.tmp')
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
        
        return (pbsJobID, outputsfile, logfile)
    
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
        pbsJobIdSet = set([t[0] for t in jobIDlist[1:]])
        
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
        for (jobID, outputsfile, logfile) in jobIDlist[1:]:
            try:
                pickledOutput = open(outputsfile).read()
                outputObj = pickle.loads(pickledOutput)
                os.remove(outputsfile)
            except Exception as e:
                logfileContents = 'No logfile found'
                if os.path.exists(logfile):
                    logfileContents = open(logfile).read()
                msg = ("Error collecting output from PBS sub-job. Exception message:\n"+str(e)+
                    "\nPBS Logfile:\n"+logfileContents)
                raise rioserrors.JobMgrError(msg)
            outputBlocksList.append(outputObj)
            os.remove(logfile)
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
def getJobManagerClassByType(jobMgrType):
    """
    Return a sub-class of JobManager, selected by the type name
    given. 
    
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


def getJobMgrObject(controls):
    """
    Take an ApplierControls object and return a JobManager sub-class 
    object which meets the needs specified in the controls object. 
    If none is required, or none is available, then return None
    
    """
    jobmgr = None
    if controls.numThreads > 1:
        if controls.jobManagerType is None:
            raise rioserrors.JobMgrError('%d threads requested, but no jobManagerType set'%controls.numThreads)
        jobMgrTypeList = getAvailableJobManagerTypes()
        if controls.jobManagerType not in jobMgrTypeList:
            raise rioserrors.JobMgrError("JobMgrType '%s' is not known"%controls.jobManagerType)
        jobmgrClass = getJobManagerClassByType(controls.jobManagerType)
        jobmgr = jobmgrClass(controls.numThreads)
        jobmgr.setOverlapMargin(controls.overlap)
        jobmgr.setTempdir(controls.tempdir)
    return jobmgr
