#!/usr/bin/env python
"""
Main script for a compute worker
"""
import argparse

from osgeo import gdal

from rios import applier
from rios.structures import NetworkDataChannel, Timers


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

    userFunction = dataChan.workerCommonData.get('userFunction', None)
    infiles = dataChan.workerCommonData.get('infiles', None)
    outfiles = dataChan.workerCommonData.get('outfiles', None)
    otherArgs = dataChan.workerCommonData.get('otherArgs', None)
    controls = dataChan.workerCommonData.get('controls', None)
    allInfo = dataChan.workerCommonData.get('allInfo', None)
    workinggrid = dataChan.workerCommonData.get('workinggrid', None)
    outBlockBuffer = dataChan.outBlockBuffer
    if not controls.concurrency.computeWorkersRead:
        inBlockBuffer = dataChan.inBlockBuffer
    else:
        inBlockBuffer = None

    blockList = dataChan.workerLocalData.get(workerID, None)

    rtn = applier.apply_singleCompute(userFunction, infiles, outfiles,
        otherArgs, controls, allInfo, workinggrid, blockList, outBlockBuffer,
        inBlockBuffer, workerID)

    # Make a pickleable version of the timings
    timings = Timers(pairs=rtn.timings.pairs, withlock=False)
    dataChan.outqueue.put(timings)
    dataChan.outqueue.put(otherArgs)


if __name__ == "__main__":
    mainCmd()
