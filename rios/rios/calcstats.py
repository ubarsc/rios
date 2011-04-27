"""
    This file is part of PyModeller
    Copyright (C) 2008  Sam Gillingham.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

#This module creates pyramid layers and calculates statistics for ERDAS Imagine files


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
    
        if ignore is not None:
            # tell QGIS that the ignore value was ignored
            band.SetNoDataValue(ignore)
      
        # get GDAL to calculate statistics - force recalculation
        (minval,maxval,meanval,stddevval) = band.GetStatistics(False,True)

        # fill in the metadata
        tmpmeta = band.GetMetadata()
    
        percent = percent + percentstep
        progress.setProgress(percent)
    
        tmpmeta["STATISTICS_MINIMUM"] = str(minval)
        tmpmeta["STATISTICS_MAXIMUM"] = str(maxval)
        tmpmeta["STATISTICS_MEAN"]    = str(meanval)
        tmpmeta["STATISTICS_STDDEV"]  = str(stddevval)

        # create a histogram so we can do the rest
        if band.DataType == gdalconst.GDT_Byte:
            # if byte data use 256 bins and the whole range
            histmin = 0
            histmax = 255
            histnbins = 256
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'direct'
        else:
            # other types use 255 bins and the range of the data
            histmin = minval
            histmax = maxval
            histnbins = 255
            tmpmeta["STATISTICS_HISTOBINFUNCTION"] = 'linear'
      
        userdata = ProgressUserData()
        userdata.progress = progress
        userdata.nbands = ds.RasterCount * 2
        userdata.curroffset = percent
      
        # get histogram and force GDAL to recalulate it
        hist = band.GetHistogram(histmin,histmax,histnbins,False,False,progressFunc,userdata)

        # do the mode - bin with the highest count
        modebin = numpy.argmax(hist)
        step = float(histmax - histmin) / histnbins
        modeval = modebin * step + histmin
        if band.DataType == gdalconst.GDT_Byte:
            tmpmeta["STATISTICS_MODE"] = str(int(round(modeval)))
        else:
            tmpmeta["STATISTICS_MODE"] = str(modeval)
    
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
        if band.DataType == gdalconst.GDT_Byte:
            tmpmeta["STATISTICS_MEDIAN"]  = str(int(round(medianval)))
        else:
            tmpmeta["STATISTICS_MEDIAN"]  = str(medianval)
    
        # set the data
        band.SetMetadata(tmpmeta)
    
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

