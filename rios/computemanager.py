import os
from abc import ABC, abstractmethod
from concurrent import futures
import queue
import subprocess
import time
import threading
import copy

from . import rioserrors
from .structures import Timers, BlockAssociations, NetworkDataChannel
from .structures import WorkerErrorRecord
from .structures import CW_NONE, CW_THREADS, CW_PBS, CW_SLURM, CW_AWSBATCH
from .structures import CW_SUBPROC
from .readerinfo import makeReaderInfo


class ComputeWorkerManager(ABC):
    """
    Abstract base class for all compute-worker manager subclasses

    A subclass implements a particular way of managing RIOS
    compute-workers. It should over-ride all abstract methods given here.
    """
    computeWorkerKind = CW_NONE
    outObjList = None

    @abstractmethod
    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
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
    unImplemented = {CW_SLURM: 'CW_SLURM',
        CW_AWSBATCH: 'CW_AWSBATCH'}
    if cwKind in unImplemented:
        msg = ("computeWorkerKind '{}' is known, " +
            "but not yet implemented").format(unImplemented[cwKind])
        raise NotImplementedError(msg)

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
    computeWorkerKind = CW_THREADS

    def __init__(self):
        self.threadPool = None
        self.workerList = None
        self.taskQ = queue.Queue()
        self.outqueue = queue.Queue()
        self.forceExit = threading.Event()

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start <numWorkers> threads to process blocks of data
        """
        # Put all blockDefn objects into a queue. The compute workers will
        # get() their next job from this. The actual data will come from the
        # inBlockBuffer, where the read workers have placed it, but the taskQ
        # tells them which block to look for in inBlockBuffer.
        for blockDefn in blockList:
            self.taskQ.put(blockDefn)

        self.threadPool = futures.ThreadPoolExecutor(max_workers=numWorkers)
        self.workerList = []
        for workerID in range(numWorkers):
            # otherArgs are not thread-safe, so each worker gets its own copy
            otherArgsCopy = copy.deepcopy(otherArgs)
            worker = self.threadPool.submit(self.worker, userFunction, infiles,
                outfiles, otherArgsCopy, controls, allInfo,
                workinggrid, self.taskQ, inBlockBuffer, outBlockBuffer,
                self.outqueue, workerID)
            self.workerList.append(worker)

    def worker(self, userFunction, infiles, outfiles, otherArgs,
            controls, allInfo, workinggrid, taskQ, inBlockBuffer,
            outBlockBuffer, outqueue, workerID):
        """
        This function is a worker for a single thread.

        It reads from the taskQ for tasks to do. When the queue is empty,
        the worker exits

        """
        try:
            timings = Timers()
            try:
                blockDefn = taskQ.get(block=False)
            except queue.Empty:
                blockDefn = None
            while blockDefn is not None and not self.forceExit.is_set():
                readerInfo = makeReaderInfo(workinggrid, blockDefn, controls)
                with timings.interval('pop_inbuffer'):
                    (blockDefn, inputs) = inBlockBuffer.popNextBlock()
                outputs = BlockAssociations()
                userArgs = (readerInfo, inputs, outputs)
                if otherArgs is not None:
                    userArgs += (otherArgs, )

                with timings.interval('userfunction'):
                    userFunction(*userArgs)

                with timings.interval('add_outbuffer'):
                    outBlockBuffer.insertCompleteBlock(blockDefn, outputs)

                try:
                    blockDefn = taskQ.get(block=False)
                except queue.Empty:
                    blockDefn = None

            if otherArgs is not None:
                outqueue.put(otherArgs)
            outqueue.put(timings)
        except Exception as e:
            workerErr = WorkerErrorRecord(workerID, e)
            outqueue.put(workerErr)

    def shutdown(self):
        """
        Shut down the thread pool
        """
        self.forceExit.set()
        futures.wait(self.workerList)
        self.threadPool.shutdown()

        # Make a list of all the objects the workers put into outqueue
        # on completion
        self.outObjList = []
        done = False
        while not done:
            try:
                outObj = self.outqueue.get(block=False)
                self.outObjList.append(outObj)
            except queue.Empty:
                done = True

        for obj in self.outObjList:
            if isinstance(obj, WorkerErrorRecord):
                print(obj)
                print()


class PBSComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using the PBS batch queue.
    """
    computeWorkerKind = CW_PBS

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start <numWorkers> PBS jobs to process blocks of data
        """
        self.haveSharedTemp = haveSharedTemp
        self.scriptfileList = []
        self.logfileList = []
        self.pbsId = {}
        if singleBlockComputeWorkers:
            # We ignore numWorkers, and have a worker for each block
            numWorkers = len(blockList)

        workerIDnumList = range(numWorkers)

        # Divide the block list into a sublist for each worker
        allSublists = [blockList[i::numWorkers] for i in range(numWorkers)]
        # Set up the data which is common for all workers
        workerInitData = {}
        workerInitData['userFunction'] = userFunction
        workerInitData['infiles'] = infiles
        workerInitData['outfiles'] = outfiles
        workerInitData['otherArgs'] = otherArgs
        workerInitData['controls'] = controls
        workerInitData['workinggrid'] = workinggrid
        workerInitData['allInfo'] = allInfo

        # Set up the data which is local to each worker
        blockListByWorker = {}
        workerInitData['blockListByWorker'] = blockListByWorker
        for workerID in workerIDnumList:
            blockListByWorker[workerID] = allSublists[workerID]

        self.dataChan = NetworkDataChannel(workerInitData, inBlockBuffer,
            outBlockBuffer)
        self.outqueue = self.dataChan.outqueue

        try:
            self.addressFile = None
            if self.haveSharedTemp:
                self.addressFile = tmpfileMgr.mktempfile(prefix='rios_pbs_',
                    suffix='.chnl')
                address = "{},{},{}".format(self.dataChan.hostname,
                    self.dataChan.portnum, self.dataChan.authkey)
                open(self.addressFile, 'w').write(address + '\n')

            for workerID in workerIDnumList:
                self.worker(workerID, tmpfileMgr)
        except Exception as e:
            self.dataChan.shutdown()
            raise e

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

        # Mark the start of outputs from the worker command in the log
        scriptCmdList.append("echo 'Begin-rios-worker'")
        scriptCmdList.append(computeWorkerCmdStr)
        # Capture the exit status from the command
        scriptCmdList.append("WORKERCMDSTAT=$?")
        # Mark the end of outputs from the worker command in the log
        scriptCmdList.append("echo 'End-rios-worker'")
        # Make sure the log includes the exit status from the command
        scriptCmdList.append("echo 'rios_computeworker status:' $WORKERCMDSTAT")
        scriptStr = '\n'.join(scriptCmdList)

        open(scriptfile, 'w').write(scriptStr + "\n")

        submitCmdWords = ["qsub", scriptfile]
        try:
            proc = subprocess.Popen(submitCmdWords, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True)
            pbsAvailable = True
        except FileNotFoundError:
            pbsAvailable = False
        if not pbsAvailable:
            raise rioserrors.UnavailableError("PBS is not available")
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

        # Extract the actual PBS job ID strings, skipping the first element.
        # Express as a set, for efficiency later on
        pbsJobIdSet = set([pbsjob for pbsjob in self.pbsId.values()])

        numJobs = len(pbsJobIdSet)
        allFinished = (numJobs == 0)
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

    def findExtraErrors(self):
        """
        Look for errors in the log files. These would be errors which were
        not reported via the data channel
        """
        numWorkers = len(self.scriptfileList)
        for workerID in range(numWorkers):
            logf = open(self.logfileList[workerID], 'r')
            loglines = [line.strip('\n') for line in logf.readlines()]
            i = self.findLine(loglines, 'Begin-rios-worker')
            if i is None:
                i = -1
            j = self.findLine(loglines, 'End-rios-worker')
            if j is None:
                j = len(loglines)

            workerOutLines = loglines[i + 1:j]
            statusNdx = self.findLine(loglines, 'rios_computeworker status:')
            if statusNdx is not None:
                statusLine = loglines[statusNdx]
                statusVal = int(statusLine.split(':')[-1])
            else:
                statusVal = 1
            if statusVal != 0:
                print("\nError in compute worker", workerID)
                print('\n'.join(workerOutLines))
                print()

    @staticmethod
    def findLine(linelist, s):
        """
        Find the first line which begins with the given string.
        Return the index of that line, or None if not found.
        """
        ndx = None
        for i in range(len(linelist)):
            line = linelist[i].strip()
            if ndx is None and line.startswith(s):
                ndx = i
        return ndx

    def shutdown(self):
        """
        Shutdown the compute manager. Wait on batch jobs, then
        shut down the data channel
        """
        self.waitOnJobs()
        self.dataChan.shutdown()

        # Make a list of all the objects the workers put into outqueue
        # on completion
        self.outObjList = []
        done = False
        while not done:
            try:
                outObj = self.outqueue.get(block=False)
                self.outObjList.append(outObj)
            except queue.Empty:
                done = True

        # Report on errors from the workers
        for obj in self.outObjList:
            if isinstance(obj, WorkerErrorRecord):
                print(obj)
                print()
        self.findExtraErrors()


class SubprocComputeWorkerManager(ComputeWorkerManager):
    """
    Purely for testing, not for normal use.

    This class manages compute workers run through subprocess.Popen.
    This is not normally any improvement over using CW_THREADS, and
    should be avoided. I am using this purely as a test framework
    to emulate the batch queue types of compute worker, which are
    similarly disconnected from the main process, so I can work out the
    right mechanisms to use for exception handling and such like,
    and making sure the rios_computeworker command line works.

    """
    computeWorkerKind = CW_SUBPROC

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True):
        """
        Start the specified compute workers
        """
        self.haveSharedTemp = haveSharedTemp
        self.processes = {}
        self.results = {}

        workerIDnumList = range(numWorkers)

        # Divide the block list into a sublist for each worker
        allSublists = [blockList[i::numWorkers] for i in range(numWorkers)]
        # Set up the data which is common for all workers
        workerInitData = {}
        workerInitData['userFunction'] = userFunction
        workerInitData['infiles'] = infiles
        workerInitData['outfiles'] = outfiles
        workerInitData['otherArgs'] = otherArgs
        workerInitData['controls'] = controls
        workerInitData['workinggrid'] = workinggrid
        workerInitData['allInfo'] = allInfo

        # Set up the data which is local to each worker
        blockListByWorker = {}
        workerInitData['blockListByWorker'] = blockListByWorker
        for workerID in workerIDnumList:
            blockListByWorker[workerID] = allSublists[workerID]

        self.dataChan = NetworkDataChannel(workerInitData, inBlockBuffer,
            outBlockBuffer)
        self.outqueue = self.dataChan.outqueue

        try:
            self.addressFile = None
            if self.haveSharedTemp:
                self.addressFile = tmpfileMgr.mktempfile(prefix='rios_subproc_',
                    suffix='.chnl')
                address = "{},{},{}".format(self.dataChan.hostname,
                    self.dataChan.portnum, self.dataChan.authkey)
                open(self.addressFile, 'w').write(address + '\n')

            for workerID in workerIDnumList:
                self.worker(workerID)
        except Exception as e:
            self.dataChan.shutdown()
            raise e

    def worker(self, workerID):
        """
        Start one worker
        """
        cmdList = ["rios_computeworker", "-i", str(workerID),
            "--channaddrfile", self.addressFile]
        self.processes[workerID] = subprocess.Popen(cmdList,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True)

    def waitOnJobs(self):
        """
        Wait for all worker subprocesses to complete
        """
        for (workerID, proc) in self.processes.items():
            (stdout, stderr) = proc.communicate()
            results = {
                'returncode': proc.returncode,
                'stdoutstr': stdout,
                'stderrstr': stderr
            }
            self.results[workerID] = results

    def findExtraErrors(self):
        """
        Check for errors in any worker stderr. These would be errors not
        reported via the data channel
        """
        for (workerID, proc) in self.processes.items():
            retcode = proc.returncode
            if retcode is not None and retcode != 0:
                print("\nError in compute worker", workerID)
                stderrStr = self.results[workerID]['stderrstr']
                print(stderrStr.strip('\n'))
                print()

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
        done = False
        while not done:
            try:
                outObj = self.outqueue.get(block=False)
                self.outObjList.append(outObj)
            except queue.Empty:
                done = True

        # Report on errors from the workers
        for obj in self.outObjList:
            if isinstance(obj, WorkerErrorRecord):
                print(obj)
                print()
        self.findExtraErrors()
