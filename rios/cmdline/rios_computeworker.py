#!/usr/bin/env python
"""
Main script for a compute worker running in a separate process.
"""
import argparse

from osgeo import gdal

from rios import applier
from rios.structures import NetworkDataChannel, Timers, WorkerErrorRecord


# Compute workers in separate processes should always use GDAL exceptions,
# regardless of whether the main script is doing so.
gdal.UseExceptions()


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser(description="""
        Main script run by each RIOS batch system compute worker. A range
        of command line options are used to control the desired mode of
        operation.
    """)
    p.add_argument("-i", "--idnum", type=int, help="Worker ID number")
    p.add_argument("--channaddrfile", help="File with data channel address")
    p.add_argument("--channaddr", help=("Directly specified data channel " +
        "address, as 'hostname,portnum,authkey'. This is less secure, and " +
        "should only be used if the preferred option --channaddrfile " +
        "cannot be used"))

    cmdargs = p.parse_args()
    return cmdargs


def mainCmd():
    """
    Main entry point for command script. This is referenced by the install
    configuration to generate the actual command line main script.
    """
    gdal.UseExceptions()

    cmdargs = getCmdargs()

    if cmdargs.channaddrfile is not None:
        addrStr = open(cmdargs.channaddrfile).readline().strip()
    else:
        addrStr = cmdargs.channaddr

    (host, port, authkey) = tuple(addrStr.split(','))
    port = int(port)
    authkey = bytes(authkey, 'utf-8')

    riosRemoteComputeWorker(cmdargs.idnum, host, port, authkey)


def riosRemoteComputeWorker(workerID, host, port, authkey):
    """
    The main routine to run a compute worker on a remote host.

    """
    dataChan = NetworkDataChannel(hostname=host, portnum=port, authkey=authkey)

    userFunction = dataChan.workerInitData.get('userFunction', None)
    infiles = dataChan.workerInitData.get('infiles', None)
    outfiles = dataChan.workerInitData.get('outfiles', None)
    otherArgs = dataChan.workerInitData.get('otherArgs', None)
    controls = dataChan.workerInitData.get('controls', None)
    allInfo = dataChan.workerInitData.get('allInfo', None)
    workinggrid = dataChan.workerInitData.get('workinggrid', None)
    outBlockBuffer = dataChan.outBlockBuffer
    if not controls.concurrency.computeWorkersRead:
        inBlockBuffer = dataChan.inBlockBuffer
    else:
        inBlockBuffer = None
    forceExit = dataChan.forceExit
    workerBarrier = dataChan.workerBarrier

    blockListByWorker = dataChan.workerInitData.get('blockListByWorker', None)
    blockList = blockListByWorker[workerID]

    if (not controls.concurrency.singleBlockComputeWorkers and
            hasattr(workerBarrier, 'wait')):
        # Wait at the barrier, so nothing proceeds until all workers have had
        # a chance to start
        computeBarrierTimeout = controls.concurrency.computeBarrierTimeout
        workerBarrier.wait(timeout=computeBarrierTimeout)

    try:
        rtn = applier.apply_singleCompute(userFunction, infiles, outfiles,
            otherArgs, controls, allInfo, workinggrid, blockList,
            outBlockBuffer, inBlockBuffer, workerID, forceExit)

        # Make a pickleable version of the timings
        timings = Timers(pairs=rtn.timings.pairs, withlock=False)
        dataChan.outqueue.put(timings)
        if otherArgs is not None:
            dataChan.outqueue.put(otherArgs)
    except Exception as e:
        # Send a printable version of the exception back to main thread
        workerErr = WorkerErrorRecord(e, 'compute', workerID)
        dataChan.exceptionQue.put(workerErr)


if __name__ == "__main__":
    mainCmd()
