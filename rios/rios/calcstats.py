"""
This module creates pyramid layers and calculates statistics for orignally for
 ERDAS Imagine files but should work with any other format that supports
 pyramid layers and statistics
"""
# This file is part of RIOS - Raster I/O Simplification
# Copyright (C) 2012  Sam Gillingha, Neil Flood
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
from osgeo import gdalconst
from . import cuiprogress
from .rioserrors import ProcessCancelledError

# we don't want to build unnecessarily small overview layers  
# we stop when the smallest dimension in the overview is less 
# than this number
minoverviewdim = 33

def progressFunc(value,string,userdata):
    """
    Progress callback for BuildOverviews
    """
    percent = (userdata.curroffset + (value / userdata.nbands) * 100)
    userdata.progress.setProgress(percent)
    if value == 1.0:
        userdata.curroffset = userdata.curroffset + 100 / userdata.nbands
    return not userdata.progress.wasCancelled()
  
# make userdata object with progress and num bands
class ProgressUserData:
    pass

def addPyramid(ds,progress):
    """
    Adds Pyramid layers to the dataset
    """
    progress.setLabelText("Computing Pyramid Layers...")
    progress.setProgress(0)
    levels = [ 4, 8, 16, 32, 64, 128, 256, 512 ]

    # first we work out how many overviews to build based on the size
    if ds.RasterXSize < ds.RasterYSize:
        mindim = ds.RasterXSize
    else:
        mindim = ds.RasterYSize
    
    nOverviews = 0
    for i in levels:
        if (mindim / i ) > minoverviewdim:
            nOverviews = nOverviews + 1

    # Need to find out if we are thematic or continuous 
    tmpmeta = ds.GetRasterBand(1).GetMetadata()
    if 'LAYER_TYPE' in tmpmeta:
        if tmpmeta['LAYER_TYPE'] == 'athematic':
            aggregationType = "AVERAGE"
        else:
            aggregationType = "NEAREST"
    else:
        aggregationType = "AVERAGE"
    
    userdata = ProgressUserData()
    userdata.progress = progress
    userdata.nbands = ds.RasterCount
    userdata.curroffset = 0
   
    ds.BuildOverviews(aggregationType, levels[:nOverviews], progressFunc, userdata )
  
    if progress.wasCancelled():
        raise ProcessCancelledError()

    # make sure it goes to 100%
    progress.setProgress(100)


def addStatistics(ds,progress,ignore=None):
    """
    Calculates statistics and adds the to the image
    """
    progress.setLabelText("Computing Statistics...")
    progress.setProgress(0)
    percent = 0
    percentstep = 100 / (ds.RasterCount * 2) # 2 steps for each layer
  
    for bandnum in range(ds.RasterCount):
        band = ds.GetRasterBand(bandnum + 1)

        # fill in the metadata
        tmpmeta = band.GetMetadata()
    
        if ignore is not None:
            # tell QGIS that the ignore value was ignored
            band.SetNoDataValue(ignore)
            tmpmeta["STATISTICS_EXCLUDEDVALUES"] = str(ignore) # doesn't seem to do anything
      
        # get GDAL to calculate statistics - force recalculation
        (minval,maxval,meanval,stddevval) = band.GetStatistics(False,True)

        percent = percent + percentstep
        progress.setProgress(percent)
    
        tmpmeta["STATISTICS_MINIMUM"] = str(minval)
        tmpmeta["STATISTICS_MAXIMUM"] = str(maxval)
        tmpmeta["STATISTICS_MEAN"]    = str(meanval)
        tmpmeta["STATISTICS_STDDEV"]  = str(stddevval)
        # because we did at full res - these are the default anyway
        tmpmeta["STATISTICS_SKIPFACTORX"] = "1"
        tmpmeta["STATISTICS_SKIPFACTORY"] = "1"

        # create a histogram so we can do the rest
        if band.DataType == gdalconst.GDT_Byte:
            # if byte data use 256 bins and the whole range
            histmin = 0
            histmax = 255
            histCalcMin = -0.5
            histCalcMax = 255.5
            histnbins = 256
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
        elif tmpmeta["LAYER_TYPE"] == 'thematic':
            # all other thematic types a bin per value
            histmin = 0
            histmax = int(numpy.ceil(maxval))
            histCalcMin = -0.5
            histCalcMax = maxval + 0.5
            histnbins = histmax + 1
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
        else:
            histrange = int(numpy.ceil(maxval) - numpy.floor(minval))
            histmin = minval
            histmax = maxval
            if histrange <= 256:
                histnbins = histrange
                tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
                histCalcMin = histmin - 0.5
                histCalcMax = histmax + 0.5
            else:
                histnbins = 256
                tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'linear'
                histCalcMin = histmin
                histCalcMax = histmax
      
        userdata = ProgressUserData()
        userdata.progress = progress
        userdata.nbands = ds.RasterCount * 2
        userdata.curroffset = percent
      
        # get histogram and force GDAL to recalulate it
        hist = band.GetHistogram(histCalcMin,histCalcMax,histnbins,False,False,progressFunc,userdata)

        # do the mode - bin with the highest count
        modebin = numpy.argmax(hist)
        step = float(histmax - histmin) / histnbins
        modeval = modebin * step + histmin
        if band.DataType == gdalconst.GDT_Float32 or band.DataType == gdalconst.GDT_Float64:
            tmpmeta["STATISTICS_MODE"] = str(modeval)
        else:
            tmpmeta["STATISTICS_MODE"] = str(int(round(modeval)))
    
        tmpmeta["STATISTICS_HISTOMIN"] = str(histmin)
        tmpmeta["STATISTICS_HISTOMAX"] = str(histmax)
        tmpmeta["STATISTICS_HISTONUMBINS"] = str(histnbins)
        tmpmeta["STATISTICS_HISTOBINVALUES"] = '|'.join(map(str,hist))

        # estimate the median - bin with the middle number
        middlenum = sum(hist) / 2
        medianbin = 0
        total = 0
        for val in hist:
            total += val
            if total >= middlenum:
                break
        medianbin += 1
        medianval = medianbin * step + histmin
        if band.DataType == gdalconst.GDT_Float32 or band.DataType == gdalconst.GDT_Float64:
            tmpmeta["STATISTICS_MEDIAN"]  = str(medianval)
        else:
            tmpmeta["STATISTICS_MEDIAN"]  = str(int(round(medianval)))
    
        # set the data
        band.SetMetadata(tmpmeta)

        # if it is thematic and there is no colour table
        # add one because Imagine fails in weird ways otherwise
        # we make a random colour table to make it obvious
        if tmpmeta["LAYER_TYPE"] == 'thematic' and band.GetColorTable() is None:
            import random # this also seeds on the time
            colorTable = gdal.ColorTable()
            alpha = 255 
            for i in range(histnbins):
                c1 = int(random.random() * 255)
                c2 = int(random.random() * 255)
                c3 = int(random.random() * 255)
                entry = (c1, c2, c3, alpha)
                colorTable.SetColorEntry(i, entry)
            band.SetColorTable(colorTable)
    
        percent = percent + percentstep
        progress.setProgress(percent)

        if progress.wasCancelled():
            raise ProcessCancelledError()
    
    progress.setProgress(100)
    
    
def calcStats(ds,progress=None,ignore=None):
    """
    Does both the stats and pyramid layers
    """
    if progress is None:
        progress = cuiprogress.CUIProgressBar()
    addStatistics(ds,progress,ignore)
    addPyramid(ds,progress)

