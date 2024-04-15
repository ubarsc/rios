#!/usr/bin/env python
"""
A simple stand-alone program to use for testing concurrency. This is NOT
a part of the standard test suite, but just for use during development.

"""
import argparse
import json
import time

import numpy
import pystac_client
from osgeo import ogr
from numba import jit

from rios import applier, pixelgrid, fileinfo
from rios import RIOS_VERSION, VersionObj
from rios.applier import CW_NONE, CW_THREADS, CW_AWSBATCH       # noqa: F401


ogr.UseExceptions()

stacServer = "https://earth-search.aws.element84.com/v1/"
collection = "sentinel-2-l2a"


def getCmdargs():
    """
    Get command line arguments
    """
    p = argparse.ArgumentParser()

    search = p.add_argument_group('Search Parameters')
    search.add_argument("-t", "--tile", default="56JPQ",
        help="Nominated tile (default=%(default)s)")
    search.add_argument("-y", "--year", default=2023, type=int,
        help="Year (default=%(default)s)")
    search.add_argument("-q", "--quarter", type=int, choices=[1, 2, 3, 4],
        default=1, help="Quarter (of year) (default=%(default)s)")
    search.add_argument("-b", "--band", default='B02',
        choices=['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08',
            'B8A', 'B09', 'B10', 'B11', 'B12'],
        help="Band ID string (default=%(default)s)")

    conc = p.add_argument_group("Concurrency Parameters")
    conc.add_argument("-r", "--numreadworkers", type=int, default=0,
        help="Number of read workers (default=%(default)s)")
    conc.add_argument("-c", "--numcomputeworkers", type=int, default=0,
        help="Number of compute workers (default=%(default)s)")
    conc.add_argument("-k", "--kind", default="none",
        choices=['none', 'threads', 'awsbatch'],
        help="Kind of compute worker (default=%(default)s)")

    proj = p.add_argument_group("Projection Parameters")
    proj.add_argument("--reproj", default=False, action="store_true",
        help="Reproject inputs to Aus Albers")
    proj.add_argument("--resample", default="near",
        help="Resample algorithm to use with --reproj (default=%(default)s)")

    cmdargs = p.parse_args()

    return cmdargs


def main():
    cmdargs = getCmdargs()

    fileList = searchStac(cmdargs)
    print("Found {} dates".format(len(fileList)))

    infiles = applier.FilenameAssociations()
    outfiles = applier.FilenameAssociations()
    controls = applier.ApplierControls()

    infiles.img = fileList
    outfiles.median = 'median.tif'
    controls.setOutputDriverName('GTiff')
    if VersionObj(RIOS_VERSION) < VersionObj('2.0.0'):
        # Get rid of rios-1.4 reproj messages
        controls.setLoggingStream(open('/dev/null', 'w'))
    if cmdargs.reproj:
        pixGrid = makeRefPixgrid(fileList[0])
        controls.setReferencePixgrid(pixGrid)
        controls.setResampleMethod(cmdargs.resample)
    if cmdargs.numreadworkers > 0 or cmdargs.numcomputeworkers > 0:
        cwKind = CW_NONE
        if cmdargs.numcomputeworkers > 0:
            cwKind = eval("CW_{}".format(cmdargs.kind.upper()))
        conc = applier.ConcurrencyStyle(
            numReadWorkers=cmdargs.numreadworkers,
            numComputeWorkers=cmdargs.numcomputeworkers,
            computeWorkerKind=cwKind
        )
        controls.setConcurrencyStyle(conc)

    t0 = time.time()
    rtn = applier.apply(doMedian, infiles, outfiles, controls=controls)
    t1 = time.time()
    if rtn is not None:
        print(rtn.timings.formatReport())
    else:
        print("Wall clock elapsed time: {:.1f} seconds".format(t1 - t0))


def doMedian(info, inputs, outputs):
    """
    Calculate per-pixel median of the list of files. Sam's code
    """
    nbands, ysize, xsize = inputs.img[0].shape
    nodata = 0

    # the median of each of the input bands
    outStack = numpy.empty((nbands, ysize, xsize),
        dtype=inputs.img[0].dtype)

    # all the inputs for one band
    # (note shape - imageidx is last)
    dataStack = numpy.empty((ysize, xsize, len(inputs.img)),
        dtype=inputs.img[0].dtype)

    for bandIdx in range(nbands):

        for imgIdx, image in enumerate(inputs.img):

            # extract the band we want
            dataStack[:, :, imgIdx] = image[bandIdx]

        medianVal = numbaMedian(dataStack, nodata)
        outStack[bandIdx] = medianVal

    outputs.median = outStack


@jit(nopython=True, nogil=True)
def numbaMedian(data, nodata):
    """
    Function utilises Numba to calculate median from stacked arrays.
    returns 3d array. Sam's code.
    """
    ysize, xsize, nbands = data.shape
    temp = numpy.empty((nbands,), dtype=data.dtype)
    result = numpy.empty((ysize, xsize), dtype=data.dtype)
    count = 0
   
    for y in range(ysize):
        for x in range(xsize):
            count = 0
            for n in range(nbands):
                val = data[y, x, n]
                if val != nodata:
                    temp[count] = val
                    count += 1
            if count == 0:
                result[y, x] = nodata
            elif count == 1:
                result[y, x] = temp[0]
            else:
                result[y, x] = numpy.median(temp[:count])
               
    return result


def searchStac(cmdargs):
    """
    Search the STAC server for suitable tiles. Return a dictionary
    of tiles, keyed by date.
    """
    centroidJson = getCentroid(cmdargs.tile)
    quarterBounds = {1: ('01-01', '03-31'), 2: ('04-01', '06-30'),
        3: ('07-01', '09-30'), 4: ('10-01', '12-31')}
    (start, end) = quarterBounds[cmdargs.quarter]
    dateRange = '{year}-{start}/{year}-{end}'.format(year=cmdargs.year,
        start=start, end=end)

    client = pystac_client.Client.open(stacServer)
    results = client.search(collections=collection, intersects=centroidJson,
        datetime=dateRange)
    featureCollection = results.item_collection_as_dict()

    fileDict = {}
    nullPcntList = []
    for feature in featureCollection['features']:
        props = feature['properties']
        path = props['earthsearch:s3_path']
        nullPcnt = props['s2:nodata_pixel_percentage']
        nullPcntList.append(nullPcnt)
        # Work out the sequence numbers for each tile/date, so we can keep
        # only the latest one
        tile = props['grid:code'][5:]
        date = props['datetime'].split('T')[0]
        seqNum = props['s2:sequence']
        if (tile, date) not in fileDict:
            fileDict[(tile, date)] = (path, seqNum)
        else:
            if seqNum > fileDict[(tile, date)][1]:
                fileDict[(tile, date)] = (path, seqNum)

    fileList = [fileDict[k][0] for k in sorted(fileDict.keys())]

    print('Median null pcnt =', numpy.median(nullPcntList))

    # Turn the directory names into file names for the requested band
    fileList = ["{}/{}.tif".format(fn, cmdargs.band) for fn in fileList]
    # Make the names suitable for GDAL
    fileList = [fn.replace('s3:/', '/vsis3') for fn in fileList]

    return fileList


def getCentroid(tile):
    """
    Query the shapefile of centroids to 
    """
    shp = 'sentinel_2_index_centroid.gpkg'
    ds = ogr.Open(shp)
    lyr = ds.GetLayer(0)
    nameNdx = lyr.FindFieldIndex('Name', True)
    pointXNdx = lyr.FindFieldIndex('POINT_X', True)
    pointYNdx = lyr.FindFieldIndex('POINT_Y', True)
    x = y = None
    for feat in lyr:
        name = feat.GetField(nameNdx)
        if name == tile:
            x = feat.GetField(pointXNdx)
            y = feat.GetField(pointYNdx)

    if x is None:
        raise ValueError('Tile {} not found'.format(tile))

    # Now make a GeoJSON string of the centroid geometry
    centroidGeom = {"type": "Point", "coordinates": [x, y]}
    centroidJson = json.dumps(centroidGeom)

    return centroidJson


def makeRefPixgrid(img):
    imginfo = fileinfo.ImageInfo(img)
    corners = imginfo.getCorners(outEPSG=3577)
    (ul_x, ul_y, ur_x, ur_y, lr_x, lr_y, ll_x, ll_y) = corners
    xMin = min(ul_x, ll_x)
    xMax = max(ur_x, lr_x)
    yMin = min(ll_y, lr_y)
    yMax = max(ul_y, ur_y)
    xMin = pixelgrid.PixelGridDefn.snapToGrid(xMin, 0, imginfo.xRes)
    xMax = pixelgrid.PixelGridDefn.snapToGrid(xMax, 0, imginfo.xRes)
    yMin = pixelgrid.PixelGridDefn.snapToGrid(yMin, 0, imginfo.yRes)
    yMax = pixelgrid.PixelGridDefn.snapToGrid(yMax, 0, imginfo.yRes)
    from osgeo import osr
    outSR = osr.SpatialReference()
    outSR.ImportFromEPSG(3577)
    outProj = outSR.ExportToWkt()
    refgrid = pixelgrid.PixelGridDefn(projection=outProj,
                xMin=xMin, xMax=xMax, yMin=yMin, yMax=yMax,
                xRes=imginfo.xRes, yRes=imginfo.yRes)
    return refgrid


if __name__ == "__main__":
    main()
