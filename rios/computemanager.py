import sys
import os
from abc import ABC, abstractmethod
from concurrent import futures
import queue
import subprocess
import time
import threading
import copy
import random

try:
    import boto3
except ImportError:
    boto3 = None

from . import rioserrors
from .structures import Timers, BlockAssociations, NetworkDataChannel
from .structures import WorkerErrorRecord
from .structures import CW_NONE, CW_THREADS, CW_PBS, CW_SLURM, CW_AWSBATCH
from .structures import CW_SUBPROC, CW_ECS
from .readerinfo import makeReaderInfo


def getComputeWorkerManager(cwKind):
    """
    Returns a compute-worker manager object of the requested kind.
    """
    if cwKind in (CW_PBS, CW_SLURM):
        cwMgrObj = ClassicBatchComputeWorkerMgr()
        cwMgrObj.computeWorkerKind = cwKind
    else:
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


class ComputeWorkerManager(ABC):
    """
    Abstract base class for all compute-worker manager subclasses

    A subclass implements a particular way of managing RIOS
    compute-workers. It should over-ride all abstract methods given here.
    """
    computeWorkerKind = CW_NONE
    outObjList = None
    outqueue = None
    jobName = None

    @abstractmethod
    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True, exceptionQue=None):
        """
        Start the specified compute workers
        """

    @abstractmethod
    def shutdown(self):
        """
        Shutdown the computeWorkerManager
        """

    def setupNetworkCommunication(self, userFunction, infiles, outfiles,
            otherArgs, controls, workinggrid, allInfo, blockList,
            numWorkers, inBlockBuffer, outBlockBuffer, forceExit,
            exceptionQue, workerBarrier):
        """
        Set up the standard methods of network communication between
        the workers and the main thread. This is expected to be the
        same for all workers running on separate machines from the
        main thread.

        Creates the dataChan and outqueue attributes.

        This routine is not needed for the Threads subclass, because it
        does not use the network versions of these communications.

        """
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
        for workerID in range(numWorkers):
            blockListByWorker[workerID] = allSublists[workerID]

        # Create the network-visible data channel
        try:
            self.dataChan = NetworkDataChannel(workerInitData, inBlockBuffer,
                outBlockBuffer, forceExit, exceptionQue, workerBarrier)
        except rioserrors.UnavailableError as e:
            if str(e) == "Failed to import cloudpickle":
                msg = ("computeWorkerKind '{}' requires the cloudpickle " +
                       "package, which appears to be unavailable")
                msg = msg.format(self.computeWorkerKind)
                raise rioserrors.UnavailableError(msg) from None
            else:
                raise
        self.outqueue = self.dataChan.outqueue
        self.exceptionQue = self.dataChan.exceptionQue

    def makeOutObjList(self):
        """
        Make a list of all the objects the workers put into outqueue
        on completion
        """
        self.outObjList = []
        done = False
        while not done:
            try:
                outObj = self.outqueue.get(block=False)
                self.outObjList.append(outObj)
            except queue.Empty:
                done = True

    def setJobName(self, jobName):
        """
        Sets the job name string, which is made available to worker
        processes. Defaults to None, and has only cosmetic effects.
        """
        self.jobName = jobName

    def getWorkerName(self, workerID):
        """
        Return a string which uniquely identifies each work, including
        the jobName, if given.
        """
        if self.jobName is not None:
            workerName = "RIOS_{}_{}".format(self.jobName, workerID)
        else:
            workerName = "RIOS_{}".format(workerID)
        return workerName


class ThreadsComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using the threads within the current process.
    """
    computeWorkerKind = CW_THREADS

    def __init__(self):
        self.threadPool = None
        self.workerList = None
        self.outqueue = queue.Queue()
        self.forceExit = threading.Event()

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True, exceptionQue=None):
        """
        Start <numWorkers> threads to process blocks of data
        """
        # Divide the block list into a sublist for each worker
        allSublists = [blockList[i::numWorkers] for i in range(numWorkers)]

        self.threadPool = futures.ThreadPoolExecutor(max_workers=numWorkers)
        self.workerList = []
        for workerID in range(numWorkers):
            # otherArgs are not thread-safe, so each worker gets its own copy
            otherArgsCopy = copy.deepcopy(otherArgs)
            subBlocklist = allSublists[workerID]
            worker = self.threadPool.submit(self.worker, userFunction, infiles,
                outfiles, otherArgsCopy, controls, allInfo,
                workinggrid, subBlocklist, inBlockBuffer, outBlockBuffer,
                self.outqueue, workerID, exceptionQue)
            self.workerList.append(worker)

    def worker(self, userFunction, infiles, outfiles, otherArgs,
            controls, allInfo, workinggrid, blockList, inBlockBuffer,
            outBlockBuffer, outqueue, workerID, exceptionQue):
        """
        This function is a worker for a single thread, with no reading
        or writing going on. All I/O is via the inBlockBuffer and
        outBlockBuffer objects.

        """
        numBlocks = len(blockList)

        try:
            timings = Timers()
            blockNdx = 0
            while blockNdx < numBlocks and not self.forceExit.is_set():
                with timings.interval('pop_readbuffer'):
                    (blockDefn, inputs) = inBlockBuffer.popNextBlock()
                readerInfo = makeReaderInfo(workinggrid, blockDefn, controls,
                    infiles, inputs, allInfo)
                outputs = BlockAssociations()
                userArgs = (readerInfo, inputs, outputs)
                if otherArgs is not None:
                    userArgs += (otherArgs, )

                with timings.interval('userfunction'):
                    userFunction(*userArgs)

                with timings.interval('insert_computebuffer'):
                    outBlockBuffer.insertCompleteBlock(blockDefn, outputs)

                blockNdx += 1

            if otherArgs is not None:
                outqueue.put(otherArgs)
            outqueue.put(timings)
        except Exception as e:
            workerErr = WorkerErrorRecord(e, 'compute', workerID)
            exceptionQue.put(workerErr)

    def shutdown(self):
        """
        Shut down the thread pool
        """
        self.forceExit.set()
        futures.wait(self.workerList)
        self.threadPool.shutdown()

        self.makeOutObjList()


class ECSComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using Amazon AWS ECS
    """
    computeWorkerKind = CW_ECS

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True, exceptionQue=None):
        """
        Start <numWorkers> ECS tasks to process blocks of data
        """
        if boto3 is None:
            raise rioserrors.UnavailableError("boto3 is unavailable")

        self.forceExit = threading.Event()
        self.workerBarrier = threading.Barrier(numWorkers + 1)

        self.setupNetworkCommunication(userFunction, infiles, outfiles,
            otherArgs, controls, workinggrid, allInfo, blockList,
            numWorkers, inBlockBuffer, outBlockBuffer, self.forceExit,
            exceptionQue, self.workerBarrier)

        channAddr = self.dataChan.addressStr()
        self.ecsClient = boto3.client("ecs")
        extraParams = controls.concurrency.computeWorkerExtraParams
        if extraParams is None:
            msg = "ECSComputeWorkerMgr requires computeWorkerExtraParams"
            raise ValueError(msg)

        # Create the ECS task definition
        taskDef_kwArgs = extraParams.get('register_task_definition')
        if taskDef_kwArgs is not None:
            taskDefResponse = self.ecsClient.register_task_definition(**taskDef_kwArgs)
            self.taskDefArn = taskDefResponse['taskDefinition']['taskDefinitionArn']

        # Now create a task for each compute worker
        runTask_kwArgs = extraParams['run_task']
        runTask_kwArgs['taskDefinition'] = self.taskDefArn
        containerOverrides = runTask_kwArgs['overrides']['containerOverrides'][0]

        for workerID in range(numWorkers):
            # Construct the command args entry with the current workerID
            workerCmdArgs = ['-i', str(workerID), '--channaddr', channAddr]
            containerOverrides['command'] = workerCmdArgs

            runTaskResponse = self.ecsClient.run_task(**runTask_kwArgs)

            failuresList = runTaskResponse['failures']
            if len(failuresList) > 0:
                self.dataChan.shutdown()
                msgList = []
                for failure in failuresList:
                    reason = failure['reason']
                    detail = failure['detail']
                    msg = "Worker {}: {}\n{}".format(workerID, reason, detail)
                    msgList.append(msg)
                fullMsg = '\n'.join(msgList)
                raise rioserrors.ECSError(fullMsg)

        # Do not proceed until all workers have started
        computeBarrierTimeout = controls.concurrency.computeBarrierTimeout
        self.workerBarrier.wait(timeout=computeBarrierTimeout)

    def shutdown(self):
        """
        Shut down the workers
        """
        self.forceExit.set()
        self.makeOutObjList()
        self.ecsClient.deregister_task_definition(taskDefinition=self.taskDefArn)
        if hasattr(self, 'dataChan'):
            self.dataChan.shutdown()

    @staticmethod
    def makeFargateExtraParams(jobName=None, containerImage=None, taskRoleArn=None,
            executionRoleArn=None, subnets=None, securityGroups=None,
            cpu='0.5 vCPU', memory='1GB', cpuArchitecture=None):
        """
        Helper function to construct a minimal computeWorkerExtraParams
        dictionary suitable for using ECS with Fargate launchType, given
        just the bare essential information.

        Returns a Python dictionary.

        jobName: str
            Arbitrary string. If given, this name will be incorporated into
            some AWS/ECS names for the compute workers, including the container
            name and the task family name.
        containerImage: str
            Required. URI of the container image to use for compute workers
        executionRoleArn: str
            Required. ARN for an AWS role. This allows ECS to use AWS services on
            your behalf. A good start is a role including
            AmazonECSTaskExecutionRolePolicy, which allows access to ECR
            container registries and CloudWatch logs.
        taskRoleArn: str
            Required. ARN for an AWS role. This allows your code to use AWS
            services. This role should include policies such as AmazonS3FullAccess,
            covering any AWS services your compute workers will need.
        subnets: list of str
            Required. List of subnet ID strings associated with the VPC in which
            workers will run.
        securityGroups: list of str
            Required. List of security groups associated with the VPC.
        cpu: str
            Number of CPU units requested for each compute worker, expressed in
            AWS's own units. For example, '0.5 vCPU', or '1024' (which
            corresponds to the same thing). Both must be strings.
        memory: str
            Amount of memory requested for each compute worker, expressed in MiB,
            or with a units suffix. For example, '1024' or its equivalent '1GB'.
        cpuArchitecture: str
            If given, selects the CPU architecture of the hosts to run worker on.
            Can be 'ARM64', defaults to 'X86_64'.

        Only certain combinations of cpu and memory are allowed, as these are used
        by Fargate to select a suitable VM instance type. See run_task()
        documentation for further details.

        """
        jobSubstr = ""
        if jobName is not None:
            jobSubstr = "_" + jobName
        containerName = 'RIOS{}_container'.format(jobSubstr)
        taskDefIDstr = random.randbytes(4).hex()
        taskFamily = "RIOS{}_{}_task".format(jobSubstr, taskDefIDstr)

        containerDefs = [{'name': containerName,
                          'image': containerImage,
                          'entryPoint': ['/usr/bin/env', 'rios_computeworker']}]

        networkConf = {
            'awsvpcConfiguration': {
                'assignPublicIp': 'DISABLED',
                'subnets': subnets,
                'securityGroups': securityGroups
            }
        }

        taskDefParams = {
            'family': taskFamily,
            'networkMode': 'awsvpc',
            'requiresCompatibilities': ['FARGATE'],
            'containerDefinitions': containerDefs,
            'cpu': cpu,
            'memory': memory
        }
        if taskRoleArn is not None:
            taskDefParams['taskRoleArn'] = taskRoleArn
        if executionRoleArn is not None:
            taskDefParams['executionRoleArn'] = executionRoleArn
        if cpuArchitecture is not None:
            taskDefParams['runtimePlatform'] = {'cpuArchitecture': cpuArchitecture}

        runTaskParams = {
            'launchType': 'FARGATE',
            'networkConfiguration': networkConf,
            'taskDefinition': 'Dummy, to be over-written within RIOS',
            'overrides': {'containerOverrides': [{
                "command": 'Dummy, to be over-written within RIOS',
                'name': containerName}]}
        }

        extraParams = {
            'register_task_definition': taskDefParams,
            'run_task': runTaskParams
        }
        return extraParams


class AWSBatchComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using AWS Batch.
    """
    computeWorkerKind = CW_AWSBATCH

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True, exceptionQue=None):
        """
        Start <numWorkers> AWS Batch jobs to process blocks of data
        """
        self.forceExit = threading.Event()
        self.workerBarrier = threading.Barrier(numWorkers + 1)
        if boto3 is None:
            raise rioserrors.UnavailableError("boto3 is unavailable")

        self.STACK_NAME = os.getenv('RIOS_AWSBATCH_STACK', default='RIOS')
        self.REGION = os.getenv('RIOS_AWSBATCH_REGION',
            default='ap-southeast-2')

        self.stackOutputs = self.getStackOutputs()
        self.batchClient = boto3.client('batch', region_name=self.REGION)

        # check what the maximum number of jobs can be run based on the 
        # vCPUS and maxvCPUs settings
        maxBatchJobs = int(int(self.stackOutputs['BatchMaxVCPUS']) / 
            int(self.stackOutputs['BatchVCPUS']))
        if numWorkers > maxBatchJobs:
            raise ValueError('Requested number of compute workers is ' +
                'greater than (MaxVCPUS / VCPUS). Either increase ' +
                'this ratio, or reduce numComputeWorkers')

        self.setupNetworkCommunication(userFunction, infiles, outfiles,
            otherArgs, controls, workinggrid, allInfo, blockList,
            numWorkers, inBlockBuffer, outBlockBuffer, self.forceExit,
            exceptionQue, self.workerBarrier)

        channAddr = self.dataChan.addressStr()

        jobQueue = self.stackOutputs['BatchProcessingJobQueueName']
        jobDefinition = self.stackOutputs['BatchProcessingJobDefinitionName']

        self.jobList = []
        for workerID in range(numWorkers):
            workerCmdArgs = ['-i', str(workerID), '--channaddr', channAddr]
            containerOverrides = {"command": workerCmdArgs}
            jobRtn = self.batchClient.submit_job(
                jobName=self.getWorkerName(workerID),
                jobQueue=jobQueue,
                jobDefinition=jobDefinition,
                containerOverrides=containerOverrides)
            self.jobList.append(jobRtn)

        if not singleBlockComputeWorkers:
            # Do not proceed until all workers have started
            computeBarrierTimeout = controls.concurrency.computeBarrierTimeout
            self.workerBarrier.wait(timeout=computeBarrierTimeout)

    def shutdown(self):
        """
        Shut down the job pool
        """
        self.forceExit.set()
        self.workerBarrier.abort()
        self.makeOutObjList()
        self.dataChan.shutdown()

    def getStackOutputs(self):
        """
        Helper function to query the CloudFormation stack for outputs.

        Uses the RIOS_AWSBATCH_STACK and RIOS_AWSBATCH_REGION env vars to
        determine which stack and region to query.
        """
        client = boto3.client('cloudformation', region_name=self.REGION)
        resp = client.describe_stacks(StackName=self.STACK_NAME)
        if len(resp['Stacks']) == 0:
            msg = "AWS Batch stack '{}' is not available".format(
                self.STACK_NAME)
            raise rioserrors.UnavailableError(msg)

        outputsRaw = resp['Stacks'][0]['Outputs']
        # convert to a normal dictionary
        outputs = {}
        for out in outputsRaw:
            key = out['OutputKey']
            value = out['OutputValue']
            outputs[key] = value
        return outputs


class ClassicBatchComputeWorkerMgr(ComputeWorkerManager):
    """
    Manage compute workers using a classic batch queue, notably
    PBS or SLURM. Initially constructed with computeWorkerKind = None,
    one must then assign computeWorkerKind as either CW_PBS or CW_SLURM
    before use.

    """
    computeWorkerKind = None

    def startWorkers(self, numWorkers=None, userFunction=None,
            infiles=None, outfiles=None, otherArgs=None, controls=None,
            blockList=None, inBlockBuffer=None, outBlockBuffer=None,
            workinggrid=None, allInfo=None, computeWorkersRead=False,
            singleBlockComputeWorkers=False, tmpfileMgr=None,
            haveSharedTemp=True, exceptionQue=None):
        """
        Start <numWorkers> PBS or SLURM jobs to process blocks of data
        """
        self.checkBatchSystemAvailable()
        self.haveSharedTemp = haveSharedTemp
        self.scriptfileList = []
        self.logfileList = []
        self.jobId = {}
        self.forceExit = threading.Event()
        self.workerBarrier = threading.Barrier(numWorkers + 1)
        if singleBlockComputeWorkers:
            # We ignore numWorkers, and have a worker for each block
            numWorkers = len(blockList)

        self.setupNetworkCommunication(userFunction, infiles, outfiles,
            otherArgs, controls, workinggrid, allInfo, blockList,
            numWorkers, inBlockBuffer, outBlockBuffer, self.forceExit,
            exceptionQue, self.workerBarrier)

        try:
            self.addressFile = None
            if self.haveSharedTemp:
                self.addressFile = tmpfileMgr.mktempfile(prefix='rios_batch_',
                    suffix='.chnl')
                address = self.dataChan.addressStr()
                open(self.addressFile, 'w').write(address + '\n')

            for workerID in range(numWorkers):
                self.worker(workerID, tmpfileMgr)
        except Exception as e:
            self.dataChan.shutdown()
            raise e

        if not singleBlockComputeWorkers:
            # Do not proceed until all workers have started
            computeBarrierTimeout = controls.concurrency.computeBarrierTimeout
            self.workerBarrier.wait(timeout=computeBarrierTimeout)

    def checkBatchSystemAvailable(self):
        """
        Check whether the selected batch queue system is available.
        If not, raise UnavailableError
        """
        cmd = self.getQueueCmd()
        try:
            subprocess.Popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True)
            batchSysAvailable = True
        except FileNotFoundError:
            batchSysAvailable = False
        if not batchSysAvailable:
            if self.computeWorkerKind == CW_PBS:
                msg = "PBS is not available"
            elif self.computeWorkerKind == CW_SLURM:
                msg = "SLURM is not available"
            raise rioserrors.UnavailableError(msg)

    def worker(self, workerID, tmpfileMgr):
        """
        Assemble a worker job and submit it to the batch queue
        """
        scriptfile = tmpfileMgr.mktempfile(prefix='rios_batch_',
            suffix='.sh')
        logfile = tmpfileMgr.mktempfile(prefix='rios_batch_',
            suffix='.log')
        self.scriptfileList.append(scriptfile)
        self.logfileList.append(logfile)

        scriptCmdList = self.beginScript(logfile, workerID)

        computeWorkerCmd = ["rios_computeworker", "-i", str(workerID)]
        if self.addressFile is not None:
            addressArgs = ["--channaddrfile", self.addressFile]
        else:
            addressArgs = ["--channaddr", self.dataChan.addressStr()]
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

        submitCmdWords = self.getSubmitCmd()
        submitCmdWords.append(scriptfile)
        proc = subprocess.Popen(submitCmdWords, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True)

        # The submit command exits almost immediately, printing the job id
        # to stdout. So, we just wait for the command to finish, and grab
        # the jobID string.
        (stdout, stderr) = proc.communicate()
        self.jobId[workerID] = self.getJobId(stdout)

        # If there was something in stderr from the submit command, then
        # probably something bad happened, so we pass it on to the user
        # in the form of an exception.
        if (len(stderr) > 0) or (self.jobId[workerID] is None):
            msg = "Error from submit command. Message:\n" + stderr
            raise rioserrors.JobMgrError(msg)

    def waitOnJobs(self):
        """
        Wait for all batch jobs to complete
        """
        jobIdSet = set([jobId for jobId in self.jobId.values()])

        numJobs = len(jobIdSet)
        allFinished = (numJobs == 0)
        while not allFinished:
            qlistCmd = self.getQueueCmd()
            proc = subprocess.Popen(qlistCmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, universal_newlines=True)
            (stdout, stderr) = proc.communicate()

            stdoutLines = [line for line in stdout.split('\n')
                if len(line) > 0]   # No blank lines
            # Skip header lines, and grab first word on each line,
            # which is the jobID
            nskip = self.getQlistHeaderCount()
            qlistJobIDlist = [line.split()[0] for line in
                stdoutLines[nskip:]]
            qlistJobIDset = set(qlistJobIDlist)

            allFinished = jobIdSet.isdisjoint(qlistJobIDset)

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
                print("\nError in compute worker", workerID, file=sys.stderr)
                print('\n'.join(workerOutLines), file=sys.stderr)
                print(file=sys.stderr)

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

    def beginScript(self, logfile, workerID):
        """
        Return list of initial script commands, depending on
        whether we are PBS or SLURM
        """
        workerName = self.getWorkerName(workerID)
        if self.computeWorkerKind == CW_PBS:
            scriptCmdList = [
                "#!/bin/bash",
                "#PBS -j oe -o {}".format(logfile),
                "#PBS -N {}".format(workerName)
            ]
            qsubOptions = os.getenv('RIOS_PBSJOBMGR_QSUBOPTIONS')
            if qsubOptions is not None:
                scriptCmdList.append("#PBS %s" % qsubOptions)

            pbsInitCmds = os.getenv('RIOS_PBSJOBMGR_INITCMDS')
            if pbsInitCmds is not None:
                scriptCmdList.append(pbsInitCmds)
        elif self.computeWorkerKind == CW_SLURM:
            scriptCmdList = [
                "#!/bin/bash",
                "#SBATCH -o %s" % logfile,
                "#SBATCH -e %s" % logfile,
                "#SBATCH -J {}".format(workerName)
            ]
            sbatchOptions = os.getenv('RIOS_SLURMJOBMGR_SBATCHOPTIONS')
            if sbatchOptions is not None:
                scriptCmdList.append("#SBATCH %s" % sbatchOptions)

            slurmInitCmds = os.getenv('RIOS_SLURMJOBMGR_INITCMDS')
            if slurmInitCmds is not None:
                scriptCmdList.append(slurmInitCmds)

        return scriptCmdList

    def getSubmitCmd(self):
        """
        Return the command name for submitting a job, depending on
        whether we are PBS or SLURM. Return as a list of words,
        ready to give to Popen.
        """
        if self.computeWorkerKind == CW_PBS:
            cmd = ["qsub"]
        elif self.computeWorkerKind == CW_SLURM:
            cmd = ["sbatch"]
        return cmd

    def getQueueCmd(self):
        """
        Return the command name for listing the current jobs in the
        batch queue, depending on whether we are PBS or SLURM. Return
        as a list of words, ready to give to Popen.
        """
        if self.computeWorkerKind == CW_PBS:
            cmd = ["qstat"]
        elif self.computeWorkerKind == CW_SLURM:
            cmd = ["squeue", "--noheader"]
        return cmd

    def getJobId(self, stdout):
        """
        Extract the jobId from the string returned when the job is
        submitted, depending on whether we are PBS or SLURM
        """
        if self.computeWorkerKind == CW_PBS:
            jobID = stdout.strip()
            if len(jobID) == 0:
                jobID = None
        elif self.computeWorkerKind == CW_SLURM:
            slurmOutputList = stdout.strip().split()
            jobID = None
            # slurm prints a sentence to the stdout:
            # 'Submitted batch job X'
            if len(slurmOutputList) >= 4:
                jobID = slurmOutputList[3]
        return jobID

    def getQlistHeaderCount(self):
        """
        Number of lines to skip at the head of the qlist output
        """
        if self.computeWorkerKind == CW_PBS:
            nskip = 0
        elif self.computeWorkerKind == CW_SLURM:
            nskip = 2
        return nskip

    def shutdown(self):
        """
        Shutdown the compute manager. Wait on batch jobs, then
        shut down the data channel
        """
        self.forceExit.set()
        self.waitOnJobs()

        self.makeOutObjList()
        self.findExtraErrors()
        self.dataChan.shutdown()


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
            haveSharedTemp=True, exceptionQue=None):
        """
        Start the specified compute workers
        """
        self.haveSharedTemp = haveSharedTemp
        self.processes = {}
        self.results = {}
        self.forceExit = threading.Event()
        self.workerBarrier = threading.Barrier(numWorkers + 1)

        self.setupNetworkCommunication(userFunction, infiles, outfiles,
            otherArgs, controls, workinggrid, allInfo, blockList,
            numWorkers, inBlockBuffer, outBlockBuffer, self.forceExit,
            exceptionQue, self.workerBarrier)

        try:
            self.addressFile = None
            if self.haveSharedTemp:
                self.addressFile = tmpfileMgr.mktempfile(prefix='rios_subproc_',
                    suffix='.chnl')
                address = self.dataChan.addressStr()
                open(self.addressFile, 'w').write(address + '\n')

            for workerID in range(numWorkers):
                self.worker(workerID)
        except Exception as e:
            self.dataChan.shutdown()
            raise e

        if not singleBlockComputeWorkers:
            # Do not proceed until all workers have started
            computeBarrierTimeout = controls.concurrency.computeBarrierTimeout
            self.workerBarrier.wait(timeout=computeBarrierTimeout)

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
                stderrStr = self.results[workerID]['stderrstr'].strip()
                if len(stderrStr) > 0:
                    print("\nError in compute worker", workerID, file=sys.stderr)
                    print(stderrStr, file=sys.stderr)
                    print(file=sys.stderr)

    def shutdown(self):
        """
        Shutdown the compute manager. Wait on batch jobs, then
        shut down the data channel
        """
        self.forceExit.set()
        self.workerBarrier.abort()
        self.waitOnJobs()
        if self.addressFile is not None:
            os.remove(self.addressFile)

        self.makeOutObjList()
        self.findExtraErrors()
        self.dataChan.shutdown()
