"""
Many of the major data structures to support the new applier parallel
architecture.

"""
import os
import socket
from multiprocessing.managers import BaseManager
from concurrent import futures
import threading
import secrets
import time
import contextlib
import queue
import tempfile

import numpy
from osgeo import gdal
import cloudpickle

from . import rioserrors


CW_NONE = 1
CW_THREADS = 2
CW_PBS = 3
CW_SLURM = 4
CW_AWSBATCH = 5
CW_SUBPROC = 10


class ConcurrencyStyle:
    """
    Class to hold all parameters associated with the different styles
    of concurrency.

    By default, concurrency is switched off. Concurrency can be
    switched on in two areas - reading, and computation.

    The writing of output blocks is always done one block at a time,
    because GDAL does not support write concurrency, but it can
    overlap with the reading and/or computation of later blocks.

    Concurrency in computation is supported at the block level. Each
    block of data is computed by a single call to the user function,
    and these calls can be distributed to a number of compute workers,
    with a choice of different distributed paradigms.

    Concurrency in computation is likely to be of benefit only when
    the user function is carrying out a computationally intensive
    task. If the computation is very simple and fast, then I/O is likely
    to be the main bottleneck, and compute concurrency will provide
    little or no speedup.

    Concurrency in reading is supported in two ways. Firstly,
    a pool of threads can read single blocks of data from individual
    files, which are then cached, ready for use in computation. This
    allows for reading multiple input files at the same time, and for
    reading ahead of the computation. Secondly, each compute worker can
    do its own reading (assuming that the input files are accessible
    to the compute workers), and the data is consumed directly by each
    worker. If desired, these two read paradigms can be used together,
    allowing each compute worker to run its own pool of read threads.

    Concurrency in reading is mainly beneficial when the input data is
    on a device which scales well with high load, such as a cluster
    disk array. There is even more benefit when the device also has high
    latency, such as an AWS S3 bucket. If input data is on a single local
    disk, it is unlikely that these techniques will improve on the caching
    and buffering already provided by a sensible operating system.

    Read Concurrency
        numReadWorkers: int
            The number of read workers. A value of 0 means that reading
            happens sequentially within the main processing loop.
        computeWorkersRead: bool
            If True, then each compute worker does its own reading,
            either sequentially or with its own pool of read workers
            (<numReadWorkers> for each compute worker)

    Compute Concurrency
        computeWorkerKind:
            Choose the paradigm used to distribute compute workers. Available
            values are {CW_NONE, CW_THREADS, CW_PBS, CW_SLURM, CW_AWSBATCH}.
            The CW_THREADS option means a pool of compute threads
            running within the same process as the rest of RIOS.
            The PBS, SLURM and AWSBATCH options all refer to different
            batch queue systems, so that compute workers can run as jobs
            on the batch queue. Not only do these run as separate
            processes, they may also be running on separate machines.
            A value of CW_NONE means that computation happens sequentially
            within the main processing loop.
        numComputeWorkers: int
            The number of distinct compute workers
        computeWorkersRead: bool
            If True, then each compute worker does its own reading,
            in accordance with the read concurrency parameters. This
            is likely to be a good option when used with the batch
            queue oriented compute workers. If False, then all reading
            is done by the main RIOS process (possibly using one or
            more read workers) and data is sent directly to each compute
            worker. This is required for CW_THREADS compute workers,
            but may also be useful in cases when batch queue nodes are
            on an internal network, but input files are not accessible
            to the batch nodes, and must be read by a process on the
            gateway machine.
        singleBlockComputeWorkers: bool
            This applies only to the batch queue paradigms. In some
            batch configurations, it is advantageous to run many small,
            quick jobs. If singleBlockComputeWorkers is True, then
            numComputeWorkers is ignored, and a batch job is generated
            for every block to be processed. It is then up to the batch
            queue system's own load balancing to decide how many jobs are
            running concurrently. This is likely to be the best option
            for large shared PBS and SLURM batch queues, where the user
            has no control over exactly when jobs run.
        haveSharedTemp: bool
            If True, then the compute workers are all able to see a shared
            temporary directory. This is ignored for some computeWorkerKinds,
            but for the PBS and SLURM kinds, this is used to share a small
            text file giving the network address for all other communication.
            If False, then the address information is passed on the command
            line of the batch jobs, which is notionally publicly visible and
            so less secure.

    Buffering Timeouts (seconds)
        The block buffers have several timeout periods defined, with default
        values. These can be over-ridden here. Mostly these timeouts should
        not be reached, but it is vital to have them. In the event of errors
        in one or more workers, whatever is waiting for that worker to respond
        would otherwise wait forever, with no explanation. The times given 
        are all in terms of the wait for a single block of data to become 
        available. For very slow input devices or very long computations, 
        they may need to be increased, but generally, if a timeout occurs, 
        one should first rule out any errors in the relevant workers before 
        increasing the timeout period.

        readBufferInsertTimeout: int
            Time to wait to insert a block into the read buffer
        readBufferPopTimeout: int
            Time to wait to pop a block out of the read buffer
        computeBufferInsertTimeout: int
            Time to wait to insert a block into the compute buffer
        computeBufferPopTimeout: int
            Time to wait to pop a block out of the compute buffer

    Note that not all possible combinations of parameters are supported,
    and some combinations make no sense at all.

    """
    def __init__(self, numReadWorkers=0, numComputeWorkers=0,
                 computeWorkerKind=CW_NONE,
                 computeWorkersRead=False,
                 singleBlockComputeWorkers=False,
                 haveSharedTemp=True,
                 readBufferInsertTimeout=10,
                 readBufferPopTimeout=10,
                 computeBufferInsertTimeout=10,
                 computeBufferPopTimeout=10,
                 ):
        self.numReadWorkers = numReadWorkers
        self.numComputeWorkers = numComputeWorkers
        self.computeWorkerKind = computeWorkerKind
        self.computeWorkersRead = computeWorkersRead
        self.singleBlockComputeWorkers = singleBlockComputeWorkers
        self.haveSharedTemp = haveSharedTemp
        self.readBufferInsertTimeout = readBufferInsertTimeout
        self.readBufferPopTimeout = readBufferPopTimeout
        self.computeBufferInsertTimeout = computeBufferInsertTimeout
        self.computeBufferPopTimeout = computeBufferPopTimeout

        # Perform checks for any invalid combinations of parameters

        if singleBlockComputeWorkers and numComputeWorkers > 0:
            msg = ("numComputeWorkers should not be specified when " +
                   "singleBlockComputeWorkers is True")
            raise ValueError(msg)

        if (computeWorkersRead and computeWorkerKind == CW_THREADS):
            msg = "CW_THREADS compute workers cannot do their own reading"
            raise ValueError(msg)

        if singleBlockComputeWorkers and (computeWorkerKind == CW_THREADS):
            msg = ("THREADS compute workers cannot also be " +
                   "singleBlockComputeWorkers")
            raise ValueError(msg)

        if numComputeWorkers > 0 and (computeWorkerKind == CW_NONE):
            msg = "Compute workers requested, but no computeWorkerKind given"
            raise ValueError(msg)

        if ((numComputeWorkers > 0) and (not computeWorkersRead) and
                (numReadWorkers == 0)):
            msg = ("Multiple non-reading compute workers with " +
                   "no read workers is not a sensible choice. Best "
                   "to make numReadWorkers at least 1")
            raise ValueError(msg)


class FilenameAssociations(object):
    """
    Class for associating external image filenames with internal
    names, which are then the same names used inside a function given
    to the :func:`rios.applier.apply` function.

    Each attribute created on this object should be a filename, or a
    list of filenames. The corresponding attribute names will appear
    on the 'inputs' or 'outputs' objects inside the applied function.
    Each such attribute will be an image data block or a list of image
    data blocks, accordingly.

    """
    def __getitem__(self, key):
        if isinstance(key, tuple):
            (symbolicName, seqNum) = key
        elif isinstance(key, str):
            symbolicName = key
            seqNum = None
        else:
            symbolicName = None

        if symbolicName in self.__dict__:
            entry = self.__dict__[symbolicName]
            if isinstance(entry, str):
                if seqNum is None:
                    value = entry
                else:
                    raise KeyError(key)
            elif isinstance(entry, list):
                if seqNum is not None and seqNum < len(entry):
                    value = entry[seqNum]
                elif seqNum is None:
                    value = entry
                else:
                    raise KeyError(key)
            else:
                msg = "Invalid entry for name '{}'".format(symbolicName)
                raise ValueError(msg)
        else:
            raise KeyError(key)

        return value

    def __contains__(self, key):
        try:
            self[key]
            isIn = True
        except KeyError:
            isIn = False
        return isIn

    def __iter__(self):
        return FilenameAssocIterator(self)


class FilenameAssocIterator(object):
    def __init__(self, infiles):
        self.fullList = []
        for symbolicName in infiles.__dict__:
            entry = infiles.__dict__[symbolicName]
            if isinstance(entry, str):
                seqNum = None
                self.fullList.append((symbolicName, seqNum, entry))
            elif isinstance(entry, list):
                for i in range(len(entry)):
                    self.fullList.append((symbolicName, i, entry[i]))
        self.currentNdx = 0

    def __next__(self):
        if self.currentNdx < len(self.fullList):
            retVal = self.fullList[self.currentNdx]
            self.currentNdx += 1
            return retVal
        else:
            raise StopIteration()


class BlockAssociations:
    """
    Container class to hold raster arrays for a single block.

    If the constructor is given a FilenameAssociations object,
    it populates the BlockAssociations object with None to match
    the same structure of names and sequences. Otherwise the object
    is empty.

    """
    def __init__(self, fnameAssoc=None):
        if fnameAssoc is not None:
            for (name, val) in fnameAssoc.__dict__.items():
                if isinstance(val, list):
                    self.__dict__[name] = [None] * len(val)
                else:
                    self.__dict__[name] = None

    def __setitem__(self, key, value):
        if isinstance(key, tuple):
            (symbolicName, seqNum) = key
        elif isinstance(key, str):
            symbolicName = key
            seqNum = None
        else:
            symbolicName = None

        if symbolicName in self.__dict__:
            entry = self.__dict__[symbolicName]
            if isinstance(entry, list):
                if seqNum is not None and seqNum < len(entry):
                    entry[seqNum] = value
                else:
                    raise KeyError(key)
            else:
                self.__dict__[symbolicName] = value
        else:
            if seqNum is None:
                self.__dict__[symbolicName] = value

    def __getitem__(self, key):
        if isinstance(key, tuple):
            (symbolicName, seqNum) = key
        elif isinstance(key, str):
            symbolicName = key
            seqNum = None
        else:
            symbolicName = None

        if symbolicName in self.__dict__:
            entry = self.__dict__[symbolicName]
            if isinstance(entry, list):
                if seqNum is not None and seqNum < len(entry):
                    value = entry[seqNum]
                else:
                    raise KeyError(key)
            else:
                value = entry
        else:
            raise KeyError(key)

        return value

    def __len__(self):
        count = 0
        for symbolicName in self.__dict__:
            entry = self.__dict__[symbolicName]
            if isinstance(entry, list):
                count += len(entry)
            else:
                count += 1
        return count


class OtherInputs(object): 
    """
    Generic object to store any extra inputs and outputs used 
    inside the function being applied. This class was originally
    named for inputs, but in fact works just as well for outputs, 
    too. Any items stored on this will be persistent between 
    iterations of the block loop. 
    """
    pass


class BlockBuffer:
    """
    Cache of blocks of data which have been read in. Blocks
    may exist but be incomplete, as individual inputs are
    added to them. This structure is shared by all read workers
    within a given process, so includes locking mechanisms to
    make it thread-safe.
    """
    def __init__(self, filenameAssoc, numWorkers,
            insertTimeout, popTimeout):
        self.CACHEMAX = 2 * numWorkers
        self.lock = threading.Lock()
        self.cache = {}
        self.completionEvents = {}
        self.insertTimeout = insertTimeout
        self.popTimeout = popTimeout
        self.nextBlockQ = queue.Queue()

        # This semaphore counts backwards for the number of blocks
        # currently in the cache. A semaphore value of zero would
        # mean the cache is full
        self.cacheCount = threading.BoundedSemaphore(self.CACHEMAX)

        # Save the filenameAssoc so we can replicate its structure
        self.filenameAssoc = filenameAssoc

    def waitCompletion(self, blockDefn, timeout=None):
        """
        Wait until the given block is complete
        """
        key = blockDefn
        with self.lock:
            if key not in self.completionEvents:
                self.completionEvents[key] = threading.Event()

        blockCompleted = self.completionEvents[key].wait(timeout=timeout)
        return blockCompleted

    def addBlockData(self, blockDefn, name, seqNum, arr):
        """
        Use when building up blocks one array at a time
        """
        # Acquire (i.e. decrement) this semaphore, in case we are about
        # to add a whole new block
        self.cacheCount.acquire(timeout=self.insertTimeout)

        with self.lock:
            if blockDefn not in self.cache:
                self.cache[blockDefn] = BlockBufferValue(
                    filenameAssoc=self.filenameAssoc)
            else:
                # We are not adding a new block, so release (increment)
                # the semaphore back again
                self.cacheCount.release()

            self.cache[blockDefn].addData(name, seqNum, arr)

            if blockDefn not in self.completionEvents:
                self.completionEvents[blockDefn] = threading.Event()
            if self.cache[blockDefn].complete():
                self.completionEvents[blockDefn].set()
                self.nextBlockQ.put(blockDefn)

    def insertCompleteBlock(self, blockDefn, blockData):
        """
        Use when inserting a complete BlockAssociations object at once
        """
        self.cacheCount.acquire(self.insertTimeout)
        with self.lock:
            if blockDefn in self.cache:
                # We did not actually add a new entry, so increment
                # the semaphore
                self.cacheCount.release()

            val = BlockBufferValue(blockData=blockData)
            self.cache[blockDefn] = val
            if blockDefn not in self.completionEvents:
                self.completionEvents[blockDefn] = threading.Event()
            self.completionEvents[blockDefn].set()
            self.nextBlockQ.put(blockDefn)

    def popCompleteBlock(self, blockDefn):
        """
        Returns the BlockAssociations object for the given blockDefn,
        and removes it from the cache
        """
        completed = self.waitCompletion(blockDefn, timeout=self.popTimeout)
        if completed:
            with self.lock:
                blockData = self.cache[blockDefn].blockData

                # Now remove this block from the cache
                self.cache.pop(blockDefn)
                self.completionEvents.pop(blockDefn)
                # One less block in the cache, so increment the semaphore
                self.cacheCount.release()
        else:
            blockData = None

        return blockData

    def popNextBlock(self):
        """
        Pop the next completed block from the cache, without regard to
        which block it is. Return a tuple of objects
            (ApplierBlockDefn, BlockAssociations)

        """
        try:
            nextBlock = self.nextBlockQ.get(timeout=self.popTimeout)
            timedout = False
        except queue.Empty:
            timedout = True

        if timedout:
            msg = "BlockBuffer timeout at {} seconds".format(self.popTimeout)
            raise rioserrors.TimeoutError(msg)

        blockData = self.popCompleteBlock(nextBlock)
        return (nextBlock, blockData)


class BlockBufferValue:
    def __init__(self, filenameAssoc=None, blockData=None):
        if filenameAssoc is not None:
            self.blockData = BlockAssociations(filenameAssoc)
            self.numMissing = len(self.blockData)
        elif blockData is not None:
            self.blockData = blockData
            self.numMissing = 0

        self.lock = threading.Lock()

    def complete(self):
        return (self.numMissing == 0)

    def addData(self, name, seqNum, arr):
        with self.lock:
            self.blockData[name, seqNum] = arr
            self.numMissing -= 1


class ApplierBlockDefn:
    """
    Defines a single block of the working grid.
    """
    def __init__(self, top, left, nrows, ncols):
        self.top = top
        self.left = left
        self.nrows = nrows
        self.ncols = ncols

    # Define __hash__ and __eq__ so we can use these objects as
    # dictionary keys
    def __hash__(self):
        return hash((self.top, self.left, self.nrows, self.ncols))

    def __eq__(self, other):
        thisID = (self.top, self.left, self.nrows, self.ncols)
        otherID = (other.top, other.left, other.nrows, other.ncols)
        return (thisID == otherID)

    def __lt__(self, other):
        thisID = (self.top, self.left, self.nrows, self.ncols)
        otherID = (other.top, other.left, other.nrows, other.ncols)
        return (thisID < otherID)

    def __gt__(self, other):
        thisID = (self.top, self.left, self.nrows, self.ncols)
        otherID = (other.top, other.left, other.nrows, other.ncols)
        return (thisID > otherID)

    def __le__(self, other):
        thisID = (self.top, self.left, self.nrows, self.ncols)
        otherID = (other.top, other.left, other.nrows, other.ncols)
        return (thisID <= otherID)

    def __ge__(self, other):
        thisID = (self.top, self.left, self.nrows, self.ncols)
        otherID = (other.top, other.left, other.nrows, other.ncols)
        return (thisID >= otherID)

    def __repr__(self):
        return 'ApplierBlockDefn({}, {}, {}, {})'.format(self.top,
            self.left, self.nrows, self.ncols)


class Timers:
    """
    Manage multiple named timers.
    """
    def __init__(self, pairs=None, withlock=True):
        if pairs is None:
            self.pairs = {}
        else:
            self.pairs = pairs
        if withlock:
            self.lock = threading.Lock()
        else:
            self.lock = None

    @contextlib.contextmanager
    def interval(self, intervalName):
        startTime = time.time()
        yield
        endTime = time.time()
        with self.lock:
            if intervalName not in self.pairs:
                self.pairs[intervalName] = []
            self.pairs[intervalName].append((startTime, endTime))

    def getDurationsForName(self, intervalName):
        if intervalName in self.pairs:
            intervals = [(p[1] - p[0]) for p in self.pairs[intervalName]]
        else:
            intervals = None
        return intervals

    def merge(self, other):
        """
        Merge another timers object into this one
        """
        with self.lock:
            for intervalName in other.pairs:
                if intervalName in self.pairs:
                    self.pairs[intervalName].extend(other.pairs[intervalName])
                else:
                    self.pairs[intervalName] = other.pairs[intervalName]

    def makeSummaryDict(self):
        """
        Make some summary statistics, and return them in a dictionary
        """
        d = {}
        for name in self.pairs:
            intervals = numpy.array(self.getDurationsForName(name))
            tot = intervals.sum()
            minVal = intervals.min()
            maxVal = intervals.max()
            pcnt25 = numpy.percentile(intervals, 25)
            pcnt50 = numpy.percentile(intervals, 50)
            pcnt75 = numpy.percentile(intervals, 75)
            d[name] = {'tot': tot, 'min': minVal, 'max': maxVal,
                'lower': pcnt25, 'median': pcnt50, 'upper': pcnt75,
                'N': len(intervals)}
        return d

    def formatReport(self, level=0):
        """
        Format a simple report. Return as a formatted string
        """
        d = self.makeSummaryDict()
        reportLines = [
            "Wall clock elapsed time: {:.1f} seconds".format(
                d['walltime']['tot']),
            "",
            "{:14s}       {:13s}".format("Timer", "Total (sec)"),
            ("-" * 31)
        ]
        fieldOrder = ['reading', 'userfunction', 'writing', 'closing',
            'add_inbuffer', 'pop_inbuffer', 'add_outbuffer',
            'pop_outbuffer']
        for name in fieldOrder:
            if name in d:
                line = "{:14s}    {:8.1f}".format(name, d[name]['tot'])
                reportLines.append(line)

        if level > 0:
            head = "{:14s}       {}".format("Timer",
                "N,Min,Lower,Median,Upper,Max")
            reportLines.extend(["", "", head, ("-" * len(head))])
            for name in fieldOrder:
                if name in d:
                    s = d[name]
                    line = "{:14s}    {},{:.4f},{:.4f},{:.4f},{:.4f},{:.4f}"
                    line = line.format(name, s['N'], s['min'], s['lower'],
                        s['median'], s['upper'], s['max'])
                    reportLines.append(line)
        reportStr = '\n'.join(reportLines)
        return reportStr


class NetworkDataChannel:
    """
    A network-visible channel to serve out all the required information to
    a group of RIOS compute workers.

    Has five major attributes. 
        workerCommonData is a dictionary of objects which are common
            to all workers. 
        workerLocalData is a dictionary, keyed by workerID, of objects
            which are local to each worker.
        inBlockBuffer is None if compute workers are doing their own reading,
            otherwise it is a BlockBuffer supplying input data to the
            compute workers.
        outBlockBuffer is a BlockBuffer where completed 'outputs' objects are
            placed, ready for writing.
        outqueue is a Queue. It is used for any non-pixel output data
            coming from each compute worker, such as modified otherArgs
            objects. Anything in this queue will be collected up by the
            main thread after all compute workers have completed.

    If the constructor is given the workerCommonData, workerLocalData,
    inBlockBuffer and outBlockBuffer arguments, then this is the server of
    these objects, and they are served to the network on a selected port
    number. The address of this server is available on the instance as
    hostname, portnum and authkey attributes. The server will create its
    own thread in which to run.

    A client instance can be created using these three address attributes
    from the server as arguments to the constructor, in which case it will
    connect to the server and make available the data attributes it was
    given.

    The server must be shut down correctly, and so the shutdown() method
    should always be called explicitly.

    """
    def __init__(self, workerCommonData=None, workerLocalData=None,
            inBlockBuffer=None, outBlockBuffer=None, hostname=None,
            portnum=None, authkey=None):
        class DataChannelMgr(BaseManager):
            pass

        if None not in (workerCommonData, workerLocalData, outBlockBuffer):
            self.hostname = socket.gethostname()
            # Authkey is a big long random bytes string. Making one which is
            # also printable ascii.
            self.authkey = secrets.token_hex()

            self.workerCommonData = cloudpickle.dumps(workerCommonData)
            self.workerLocalData = workerLocalData
            self.inBlockBuffer = inBlockBuffer
            self.outBlockBuffer = outBlockBuffer
            self.outqueue = queue.Queue()

            DataChannelMgr.register("get_commondata",
                callable=lambda: self.workerCommonData)
            DataChannelMgr.register("get_localdata",
                callable=lambda: self.workerLocalData)
            DataChannelMgr.register("get_inblockbuffer",
                callable=lambda: self.inBlockBuffer)
            DataChannelMgr.register("get_outblockbuffer",
                callable=lambda: self.outBlockBuffer)
            DataChannelMgr.register("get_outqueue",
                callable=lambda: self.outqueue)

            self.mgr = DataChannelMgr(address=(self.hostname, 0),
                                     authkey=bytes(self.authkey, 'utf-8'))

            self.server = self.mgr.get_server()
            self.portnum = self.server.address[1]
            self.threadPool = futures.ThreadPoolExecutor(max_workers=1)
            self.serverThread = self.threadPool.submit(
                self.server.serve_forever)
        elif None not in (hostname, portnum, authkey):
            DataChannelMgr.register("get_commondata")
            DataChannelMgr.register("get_localdata")
            DataChannelMgr.register("get_outblockbuffer")
            DataChannelMgr.register("get_inblockbuffer")
            DataChannelMgr.register("get_outqueue")

            self.mgr = DataChannelMgr(address=(hostname, portnum),
                                     authkey=authkey)
            self.hostname = hostname
            self.portnum = portnum
            self.authkey = authkey
            self.mgr.connect()

            # Get the proxy objects.
            self.workerCommonData = cloudpickle.loads(eval(str(
                self.mgr.get_commondata())))
            self.workerLocalData = self.mgr.get_localdata()
            self.inBlockBuffer = self.mgr.get_inblockbuffer()
            self.outBlockBuffer = self.mgr.get_outblockbuffer()
            self.outqueue = self.mgr.get_outqueue()
        else:
            msg = ("Must supply either (filenameAssoc & numWorkers) or " +
                   "all of (hostname, portnum and authkey)")
            raise ValueError(msg)

    def shutdown(self):
        """
        Shut down the NetworkDataChannel in the right order. This should always
        be called explicitly by the creator, when it is no longer
        needed. If left to the garbage collector and/or the interpreter
        exit code, things are shut down in the wrong order, and the
        interpreter hangs on exit.

        I have tried __del__, also weakref.finalize and atexit.register,
        and none of them avoid these problems. So, just make sure you
        call shutdown explicitly, in the process which created the
        NetworkDataChannel.

        The client processes don't seem to care, presumably because they
        are not running the server thread. Calling shutdown on the client
        does nothing.

        """
        if hasattr(self, 'server'):
            self.server.stop_event.set()
            futures.wait([self.serverThread])
            self.threadPool.shutdown()


class RasterizationMgr:
    """
    Manage rasterization of vector inputs, shared across multiple
    read workers within a single process. Not intended to be shared
    across compute workers on separate machines.

    """
    def __init__(self):
        self.lock = threading.Lock()
        self.perFileLocks = {}
        self.lookup = {}

    def rasterize(self, vectorfile, rasterizeOptions, tmpfileMgr):
        """
        Rasterize the given vector file, according to the given
        rasterizeOptions.

        Return the name of the temporary raster file.

        This is thread-safe. Any other thread trying to raterize the
        same vector file will block until this has completed, and then
        be given exactly the same temporary raster file.

        """
        with self.lock:
            if vectorfile not in self.perFileLocks:
                self.perFileLocks[vectorfile] = threading.Lock()
        with self.perFileLocks[vectorfile]:
            if vectorfile not in self.lookup:
                tmpraster = tmpfileMgr.mktempfile(prefix='rios_vecrast_',
                    suffix='.tif')
                gdal.Rasterize(tmpraster, vectorfile, options=rasterizeOptions)
                self.lookup[vectorfile] = tmpraster
                
        return self.lookup[vectorfile]


class TempfileManager:
    """
    A single object which can keep track of all the temporary files
    created during a run. Shared between read worker threads, within
    a single process, so must be thread-safe. Not shared across processes.

    Includes methods to make and delete the temporary files.

    Constructor takes a single string for tempdir. All subsequent
    temp files will be created in a subdirectory underneath this.
    """
    def __init__(self, tempdir):
        self.tempsubdir = tempfile.mkdtemp(dir=tempdir, prefix='rios_')
        self.tempfileList = []
        self.lock = threading.Lock()

    def mktempfile(self, prefix=None, suffix=None):
        """
        Make a new tempfile, and return the full name
        """
        with self.lock:
            (fd, name) = tempfile.mkstemp(dir=self.tempsubdir,
                prefix=prefix, suffix=suffix)
            os.close(fd)
            self.tempfileList.append(name)
        return name

    def cleanup(self):
        """
        Remove all the temp files created here
        """
        for filename in self.tempfileList:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

        # Now remove the temp subdir itself
        try:
            os.rmdir(self.tempsubdir)
        except FileNotFoundError:
            pass

    def __del__(self):
        self.cleanup()


class ApplierReturn:
    """
    Hold all objects returned by the applier.apply() function
    """
    def __init__(self):
        self.timings = None
        self.otherArgsList = None
