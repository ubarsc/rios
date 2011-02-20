
"""
This module contains the InputCollection and InputIterator
classes. These classes are for ImageReader to keep track
of the inputs it has and deal with resampling.

"""

import os
import sys
import subprocess
import imageio
import exceptions
import pixelgrid
from osgeo import gdal

class InputIterator(object):
    """
    Class to allow iteration across an InputCollection instance.
    Do not instantiate this class directly - it is created
    by InputCollection.__iter__().
    
    See http://docs.python.org/library/stdtypes.html#typeiter
    for a description of how this works. There is another way,
    see: http://docs.python.org/reference/expressions.html#yieldexpr
    but it seemed too much like Windows 3.1 programming which
    scared me!
    
    Returns a image name, GDAL Dataset, PixelGridDefn, nullvallist and datatype
    for each iteration
    
    """
    def __init__(self,collection):
        # collection = an InputCollection instance
        self.collection = collection
        self.index = 0 # start at first one
        
    def __iter__(self):
        # For iteration support - just return self.
        return self
        
    def next(self):
        # for iteration support. Raises a StopIteration
        # if we have read beyond the end of the collection
        if self.index >= len(self.collection.datasetList):
            raise StopIteration()
            
        # pull out the values we want to return
        image = self.collection.imageList[self.index]
        ds = self.collection.datasetList[self.index]
        pixgrid = self.collection.pixgridList[self.index]
        nullvals = self.collection.nullValList[self.index]
        datatype = self.collection.dataTypeList[self.index]
        
        # so next time we are looking at the next one
        self.index += 1
        
        return (image, ds, pixgrid, nullvals, datatype)
        
  
class InputCollection(object):
    """
    InputCollection class. Keeps all of the inputs and the
    information we need about them in one place.
    
    Gets passed a list of filenames and opens them, creates
    a PixelGridDefn and pulls out their null values.
    
    Setting the reference dataset is done via setReference()
    (is first dataset by default). Resampling can be 
    performed by resampleToReference(). 
    
    Use checkAllMatch() to see if resampling is necessary.
    
    """
    def __init__(self,imageList,loggingstream=sys.stdout):
        """
        Constructor. imageList is a list of file names that 
        need to be opened. An ImageOpenException() is raised
        should opening fail.
        
        Set loggingstream to a file like object if you wish
        logging about resampling to be sent somewhere else
        rather than stdout.
        
        """
        self.loggingstream = loggingstream
        # initialise our lists
        self.imageList = []
        self.datasetList = []
        self.pixgridList = []
        self.nullValList = [] # NB: a list of lists
        self.dataTypeList = [] # numpy types
        self.filestoremove = [] # any temp resampled files
    
        # go thru each image
        for image in imageList:
        
            # try and open it
            ds = gdal.Open(str(image))
            if ds is None:
                msg = 'Unable to Open %s' % image
                raise exceptions.ImageOpenError(msg)
                
            # create a PixelGridDefn for the dataset
            pixGrid = self.makePixGridFromDataset(ds)

            # Stash the null values for each band
            dsNullValList = []
            for i in range(ds.RasterCount):
                nullVal = ds.GetRasterBand(i+1).GetNoDataValue()
                dsNullValList.append(nullVal)
                
            # get the datatype of band 1
            gdaldatatype = ds.GetRasterBand(1).DataType
            numpytype = imageio.GDALTypeToNumpyType(gdaldatatype)
        
            # store the values in our lists
            self.imageList.append(image)
            self.datasetList.append(ds)
            self.pixgridList.append(pixGrid)
            self.nullValList.append(dsNullValList)
            self.dataTypeList.append(numpytype)

        # by default the reference dataset is the first file
        self.referencePixGrid = self.pixgridList[0]
        
    def __del__(self):
        """
        Remove all resampled files. This does not appear
        to get called if there is an exception in the 
        reader, so there is an exception handler and a finally in
        ImageReader.readBlock which calls cleanup directly.
        """
        self.cleanup()
        
    def close(self):
        """
        Closes all open datasets
        """
        for ds in self.datasetList:
            del ds
        self.datasetList = []
        self.cleanup()
        
        
    def cleanup(self):
        """
        Removes any temp files. To be called from destructor?
        Seems not given Neil's experience - needs to be called
        from finally clause - not sure how to enforce this
        in user's script....
        """
        for f in self.filestoremove:
            if os.path.exists(f):
                os.remove(f)
        self.filestoremove = []        
        
    def __len__(self):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types
        return len(self.datasetList)
        
    def __getitem__(self,key):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types
        # for indexing, returns:
        # imagename, dataset, pixgrid,nullValList, datatype 
        # if key outside range, then the individual lists
        # should raise KeyError
        image = self.imageList[key]
        ds = self.datasetList[key]
        pixgrid = self.pixgridList[key]
        nullValList = self.nullValList[key]
        datatype = self.dataTypeList[key]
        return (image,ds,pixgrid,nullValList,datatype)
        
    def __iter__(self):
        # see http://docs.python.org/reference/datamodel.html#emulating-container-types
        return InputIterator(self)
            
    def setReference(self, refpath=None, refgeotrans=None, 
            refproj=None, refNCols=None, refNRows=None ):
        """
        Sets the reference dataset for resampling purposes.
        
        Either set refpath to a path to a GDAL file and all
        necessary values will be extracted. Or pass refgeotrans,
        refproj, refNCols and refNRows.
        
        """
            
        # if we have a path, open it with GDAL
        if refpath is not None:
            ds = gdal.Open(refpath)
            if ds is None:
                msg = 'Unable to Open %s' % refpath
                raise ImageOpenError(msg)
            
            # make a pixgrid from the dataset
            self.referencePixGrid = self.makePixGridFromDataset(ds)
            # close the dataset
            del ds
            
        # else, check we have all the aother params
        elif refgeotrans is not None and refproj is not None and refNCols is not None and refNRows is not None:
            
            # create a pixgrid from the info we have
            proj = self.specialProjFixes(refproj)
            pixGrid = pixelgrid.PixelGridDefn(geotransform=refgeotrans,
                    nrows=refNRows, ncols=refNCols, projection=proj)
            self.referencePixGrid = pixGrid 
            
        else:
            msg = 'Must pass either refpath or all the other params'
            raise exceptions.ParameterError(msg)
            

    def resampleToReference(self, ds, nullValList, workingRegion, resamplemethod):
        """
        Resamples any inputs that do not match the reference, to the 
        reference image. 
        
        ds is the GDAL dataset that needs to be resampled.
        nullValList is the list of null values for that dataset.
        workingRegion is a PixelGridDefn
        resamplemethod is a string containing a method supported by gdalwarp.
        
        Returns the new (temporary) resampled dataset instance.
        
        Do not call directly, use resampleAllToReference()
        
        """
    
        # get the name of the input file
        infile = ds.GetDescription()

        # create temporary .prf files with the source
        # and destination WKT strings.
        import tempfile
        # the src prf file
        (fileh,src_prf) = tempfile.mkstemp('.prf',dir='.')
        srcproj = self.specialProjFixes(ds.GetProjection())
        os.write(fileh, srcproj)
        os.close(fileh)
        
        # the dest prf file
        (fileh,dest_prf) = tempfile.mkstemp('.prf',dir='.')
        os.write(fileh, self.referencePixGrid.projection)
        os.close(fileh)

        # get the driver from the input dataset
        # so we know the default extension, and type
        # for the temporary file.
        driver = gdal.IdentifyDriver(infile)
        drivermeta = driver.GetMetadata()
        
        # temp image file name - based on driver extension
        ext = ''
        if drivermeta.has_key("DMD_EXTENSION"):
          ext = '.' + drivermeta["DMD_EXTENSION"]
        (fileh,temp_image) = tempfile.mkstemp(ext,dir='.')
        os.close(fileh)
          
        # build the command line for gdalwarp
        # as a list for subprocess - also a bit easier to read
        cmdList = ['gdalwarp']
        
        # source projection prf file
        cmdList.append('-s_srs')
        cmdList.append(src_prf)
        
        # destination projection prf file
        cmdList.append('-t_srs')
        cmdList.append(dest_prf)
        
        # extent
        cmdList.append('-te')
        cmdList.append('%f' % workingRegion.xMin)
        cmdList.append('%f' % workingRegion.yMin)
        cmdList.append('%f' % workingRegion.xMax)
        cmdList.append('%f' % workingRegion.yMax)
        
        # resolution 
        cmdList.append('-tr')
        cmdList.append('%f' % workingRegion.xRes)
        cmdList.append('%f' % workingRegion.yRes)
        
        # output format
        cmdList.append('-of')
        cmdList.append(driver.ShortName)
        
        # resample method
        cmdList.append('-r')
        cmdList.append(resamplemethod)
        
        # null values
        nullOptions = self.makeWarpNullOptions(nullValList)
        if nullOptions is not None:
            cmdList.extend(nullOptions)
        
        # don't have creation options for output like PyModeller did
        # (after all we are just a reader) so don't specify any 
        # creation options. Not sure if this will cause problems...
        
        # input and output files
        cmdList.append(infile)
        cmdList.append(temp_image)
       
        # delete later - add before the system call as if Ctrl-C is hit 
        # control does not always return
        self.filestoremove.append(temp_image)
        self.filestoremove.append(src_prf)
        self.filestoremove.append(dest_prf)
          
        # run the command using subprocess
        # send any output to our self.loggingstream
        # - this is the main advantage over os.system()
        returncode = subprocess.call(cmdList,
                stdout=self.loggingstream,
                stderr=self.loggingstream)

        if returncode != 0:
            msg = 'Unable to run gdalwarp'
            raise exceptions.GdalWarpNotFoundError(msg)
          
        # open the new temp file
        newds = gdal.Open(temp_image)
        if newds is None:
            msg = 'Unable to Open %s' % temp_image
            raise exceptions.ImageOpenError(msg)
        
        # return the new dataset
        return newds
        
            
    def resampleAllToReference(self, footprint, resamplemethod):
        """
        Reamples all datasets that don't match the reference to the
        same as the reference.
        
        footprint is imageio.INTERSECTION or imageio.UNION
        resamplemethod is a string containing a method supported by gdalwarp.
        
        """
    
        # find the working region based on the footprint
        workingRegion = self.findWorkingRegion(footprint)
    
        # go thru each input.
        for count in range(len(self.datasetList)):
            pixGrid = self.pixgridList[count]
            
            # check the pixgrid of the dataset matches the 
            # reference
            allEqual = True
            if not self.referencePixGrid.equalPixSize(pixGrid):
                self.loggingstream.write("Pixel sizes don't match %.20f %.20f %.20f %.20f\n" % 
                (self.referencePixGrid.xRes, pixGrid.xRes, self.referencePixGrid.yRes, pixGrid.yRes))
                allEqual = False
            elif not self.referencePixGrid.equalProjection(pixGrid):
                self.loggingstream.write("Coordinate systems don't match %s %s\n"  % 
                (self.referencePixGrid.projection, pixGrid.projection))
                allEqual = False
            elif not self.referencePixGrid.alignedWith(pixGrid):
                self.loggingstream.write("Images aren't on the same grid\n")
                allEqual = False
            
            # did it fail to match completely?
            if not allEqual:
                
                # resample the dataset
                ds = self.datasetList[count]
                nullVals = self.nullValList[count]
                
                newds = self.resampleToReference(ds, nullVals, workingRegion, resamplemethod)
                
                # stash the new temp dataset as our input item
                self.datasetList[count] = newds
                
                # just assume it has been successfully 
                # resampled to exactly the reference
                self.pixgridList[count] = self.referencePixGrid
                
                # close the original dataset - just using temp 
                # resampled one from now.
                del ds
        
    
    def checkAllMatch(self):
        """
        Returns whether any resampling necessary to match
        reference dataset.
        
        Use as a check if no resampling is done that we
        can proceed ok.
        
        """
    
        match = True
        for pixGrid in self.pixgridList:
            if not self.referencePixGrid.equalPixSize(pixGrid):
                match = False
                break
            elif not self.referencePixGrid.equalProjection(pixGrid):
                match = False
                break
            elif not self.referencePixGrid.alignedWith(pixGrid):
                match = False
                break
                
        return match
            
    @staticmethod
    def makeWarpNullOptions(nullValList):
        """
        Make appropriate options for gdalwarp, to handle 
        null value properly. If any of the null values
        in the list is None, then we can't do it because the null 
        value is not set on the file, so the return value is None. 
    
        Normally returns a list of arguments, formatted 
        with -srcnodata and -dstnodata
        
        Note since this is to be passed to gdalwarp directly,
        ie not thru a shell, we don't need to quote the nulls string.
    
        """
        haveNone = False
        for nullVal in nullValList:
            if nullVal is None:
                haveNone = True
        if haveNone:
            optionList = None
        else:
            nullValStrList = [str(n) for n in nullValList]
            allNulls = ' '.join(nullValStrList)
            optionList = ['-srcnodata',allNulls,'-dstnodata',allNulls]
        return optionList

    @staticmethod
    def specialProjFixes(projwkt):
        """
        Does any special fixes required for the projection. Returns the fixed
        projection WKT string. 
    
        Specifically this does two things, both of which are to cope with rubbish
        that Imagine has put into the projection. Firstly, it removes the
        crappy TOWGS84 parameters which Imagine uses for GDA94, and secondly 
        removes the crappy name which Imagine gives to the correct GDA94.
    
        If neither of these things is found, returns the string unchanged. 
    
        """
        dodgyTOWGSstring = "TOWGS84[-16.237,3.51,9.939,1.4157e-06,2.1477e-06,1.3429e-06,1.91e-07]"
        properTOWGSstring = "TOWGS84[0,0,0,0,0,0,0]"
        if projwkt.find('"GDA94"') > 0:
            newWkt = projwkt.replace(dodgyTOWGSstring, properTOWGSstring)
        else:
            newWkt = projwkt
    
        # Imagine's name for the correct GDA94 also causes problems, so 
        # replace it with something more standard. 
        newWkt = newWkt.replace('GDA94-ICSM', 'GDA94')
    
        return newWkt
        
    @staticmethod
    def makePixGridFromDataset(ds):
        """
        Make a pixelgrid object from the given dataset. 
        """
        proj = InputCollection.specialProjFixes(ds.GetProjection())
        geotrans = ds.GetGeoTransform()
        (nrows, ncols) = (ds.RasterYSize, ds.RasterXSize)
        pixGrid = pixelgrid.PixelGridDefn(geotransform=geotrans, nrows=nrows, ncols=ncols, 
            projection=proj)
        return pixGrid

    def findWorkingRegion(self, footprint):
        """
        Work out what the combined pixel grid should be, in terms of the 
        given reference input raster. Returns a PixelGridDefn object. 
    
        """
    
        combinedGrid = pixelgrid.findCommonRegion(self.pixgridList, 
            self.referencePixGrid, combine=footprint)
        return combinedGrid
