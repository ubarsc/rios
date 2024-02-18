import os
from abc import ABC, abstractmethod
from concurrent import futures
import queue
import subprocess
import time

from . import rioserrors
from .structures import Timers, BlockAssociations, NetworkDataChannel
from .readerinfo import makeReaderInfo


class ComputeWorkerManager(ABC):
    """
    Abstract base class for all compute-worker manager subclasses

    A subclass implements a particular way of managing RIOS
    compute-workers. It should over-ride all abstract methods given here.
    """
    computeWorkerKind = None

    @abstractmethod
    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockCache=None, outBlockCache=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start the specified compute workers
        """

    @abstractmethod
    def shutdown(self):
        """
        Shutdown the computeWorkerManager
        """


def getComputeWorkerManager(cwKind):
    """
    Returns a compute-worker manager object of the requested kind.
    """
    cwMgrClass = None
    subClasses = ComputeWorkerManager.__subclasses__()
    for c in subClasses:
        if c.computeWorkerKind == cwKind:
            cwMgrClass = c

    if cwMgrClass is None:
        msg = "Unknown compute-worker kind '{}'".format(cwKind)
        raise ValueError(msg)

    cwMgrObj = cwMgrClass()
    return cwMgrObj


class ThreadsComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using the threads within the current process.
    """
    def __init__(self):
        self.threadPool = None
        self.workerList = None
        self.taskQ = queue.Queue()
        self.outqueue = queue.Queue()

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockCache=None, outBlockCache=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start <numWorkers> threads to process blocks of data
        """
        # Put all blockDefn objects into a queue. The compute workers will
        # get() their next job from this. The actual data will come from the
        # inBlockCache, where the read workers have placed it, but the taskQ
        # tells them which block to look for in inBlockCache.
        for blockDefn in blockList:
            self.taskQ.put(blockDefn)

        self.threadPool = futures.ThreadPoolExecutor(max_workers=numWorkers)
        self.workerList = []
        for i in range(numWorkers):
            worker = self.threadPool.submit(self.worker, userFunction, infiles,
                outfiles, otherArgs, controls, allInfo, workinggrid,
                self.taskQ, inBlockCache, outBlockCache, self.outqueue)
            self.workerList.append(worker)

    @staticmethod
    def worker(userFunction, infiles, outfiles, otherArgs, controls,
            allInfo, workinggrid, taskQ, inBlockCache, outBlockCache,
            outqueue):
        """
        This function is a worker for a single thread.

        It reads from the taskQ for tasks to do. When the queue is empty,
        the worker exits

        """
        timings = Timers()
        blockDefn = taskQ.get(block=False)
        while blockDefn is not None:
            readerInfo = makeReaderInfo(workinggrid, blockDefn, controls)
            with timings.interval('waitpopincache'):
                inputs = inBlockCache.popCompleteBlock(blockDefn)
            outputs = BlockAssociations()
            userArgs = (readerInfo, inputs, outputs)
            if otherArgs is not None:
                userArgs += otherArgs

            with timings.interval('userfunction'):
                userFunction(*userArgs)

            with timings.interval('waitaddoutcache'):
                outBlockCache.insertCompleteBlock(blockDefn, outputs)

            blockDefn = taskQ.get(block=False)

        outqueue.put(otherArgs)
        outqueue.put(timings)

    def shutdown(self):
        """
        Shut down the thread pool
        """
        futures.wait(self.workerList)
        self.threadPool.shutdown()

        # Make a list of all the objects the workers put into outqueue
        # on completion
        self.outObjList = []
        outObj = self.outqueue.get(block=False)
        while outObj is not None:
            self.outObjList.append(outObj)
            outObj = self.outqueue.get(block=False)


class PBSComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using the PBS batch queue.
    """
    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockCache=None, outBlockCache=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start <numWorkers> PBS jobs to process blocks of data
        """
        self.haveSharedTemp = haveSharedTemp
        self.scriptfileList = []
        self.logfileList = []
        if singleBlockComputeWorkers:
            # We ignore numWorkers, and have a worker for each block
            numWorkers = len(blockList)

        workerIDnumList = range(numWorkers)

        # Divide the block list into a sublist for each worker
        allSublists = [blockList[i::numWorkers] for i in range(numWorkers)]
        # Set up the data which is common for all workers
        workerCommonData = {}
        workerCommonData['userFunction'] = userFunction
        workerCommonData['infiles'] = infiles
        workerCommonData['outfiles'] = outfiles
        workerCommonData['otherArgs'] = otherArgs
        workerCommonData['controls'] = controls
        workerCommonData['workinggrid'] = workinggrid
        workerCommonData['allInfo'] = allInfo

        # Set up the data which is local to each worker
        workerLocalData = {}
        for workerID in workerIDnumList:
            # The only per-worker value is the block sublist. Maybe there
            # should be other things ?????
            workerLocalData[workerID] = allSublists[workerID]

        self.dataChan = NetworkDataChannel(workerCommonData,
            workerLocalData, inBlockCache, outBlockCache)

        self.addressFile = None
        if self.haveSharedTemp:
            self.addressFile = tmpfileMgr.mktempfile(prefix='rios_pbs_',
                suffix='.chnl')
            address = "{},{},{}".format(self.dataChan.hostname,
                self.dataChan.portnum, self.dataChan.authkey)
            open(self.addressFile, 'w').write(address + '\n')

        self.pbsId = {}
        for workerID in workerIDnumList:
            self.worker(workerID, tmpfileMgr)

    def worker(self, workerID, tmpfileMgr):
        scriptfile = tmpfileMgr.mktempfile(prefix='rios_pbsscript_',
            suffix='.sh')
        logfile = tmpfileMgr.mktempfile(prefix='rios_pbsscript_',
            suffix='.log')
        self.scriptfileList.append(scriptfile)
        self.logfileList.append(logfile)

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

        computeWorkerCmd = ["rios_computeworker", "-i", str(workerID)]
        if self.addressFile is not None:
            addressArgs = ["--channaddrfile", self.addressFile]
        else:
            (host, port, authkey) = (self.dataChan.hostname,
                    self.dataChan.portnum, self.dataChan.authkey)
            addressArgs = ["--channaddr",
                "{},{},{}".format(host, port, authkey)]
        computeWorkerCmd.extend(addressArgs)
        computeWorkerCmdStr = " ".join(computeWorkerCmd)

        scriptCmdList.append(computeWorkerCmdStr)
        scriptStr = '\n'.join(scriptCmdList)

        open(scriptfile, 'w').write(scriptStr + "\n")

        submitCmdWords = ["qsub", scriptfile]
        proc = subprocess.Popen(submitCmdWords, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True)
        # The qsub command exits almost immediately, printing the PBS job id
        # to stdout. So, we just wait for the qsub to finish, and grab the
        # jobID string.
        (stdout, stderr) = proc.communicate()
        pbsJobID = stdout.strip()
        self.pbsId[workerID] = pbsJobID

        # If there was something in stderr from the qsub command, then probably
        # something bad happened, so we pass it on to the user in the form of
        # an exception.
        if len(stderr) > 0:
            msg = "Error from qsub. Message:\n" + stderr
            raise rioserrors.JobMgrError(msg)

    def waitOnJobs(self):
        """
        Wait for all PBS batch jobs to complete
        """
        allFinished = False

        # Extract the actual PBS job ID strings, skipping the first element.
        # Express as a set, for efficiency later on
        pbsJobIdSet = set([pbsjob for pbsjob in self.pbsId.values()])

        while not allFinished:
            qstatCmd = ["qstat"]
            proc = subprocess.Popen(qstatCmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True)
            (stdout, stderr) = proc.communicate()

            stdoutLines = [line for line in stdout.split('\n')
                if len(line) > 0]   # No blank lines
            # Skip header lines, and grab first word on each line,
            # which is the PBS jobID
            qstatJobIDlist = [line.split()[0] for line in stdoutLines[2:]]
            qstatJobIDset = set(qstatJobIDlist)

            allFinished = pbsJobIdSet.isdisjoint(qstatJobIDset)

            if not allFinished:
                # Sleep for a bit before checking again
                time.sleep(60)

    def shutdown(self):
        """
        Shutdown the compute manager. Wait on batch jobs, then
        shut down the data channel
        """
        self.waitOnJobs()
        if self.addressFile is not None:
            os.remove(self.addressFile)
        self.dataChan.shutdown()

        # Make a list of all the objects the workers put into outqueue
        # on completion
        self.outObjList = []
        outObj = self.dataChan.outqueue.get(block=False)
        while outObj is not None:
            self.outObjList.append(outObj)
            outObj = self.dataChan.outqueue.get(block=False)