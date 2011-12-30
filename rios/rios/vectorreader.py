
"""
This module contains the Vector and VectorReader
class that perform on-the-fly rasterization of
vectors into raster blocks to fit in with
ImageReader and ImageWriter classes.

"""
import os
import tempfile
from .imagewriter import DEFAULTDRIVERNAME
from .imagewriter import DEFAULTCREATIONOPTIONS
from . import rioserrors
from .imagereader import ImageReader
from .imageio import NumpyTypeToGDALType
from osgeo import ogr
from osgeo import gdal
import numpy

DEFAULTBURNVALUE = 1
DEFAULTPIXTOLERANCE = 0.5

class Vector(object):
    """
    Class that holds information about a vector dataset and how it
    should be rasterized. Used for passing to VectorReader.
    """
    def __init__(self, filename, inputlayer=0, burnvalue=DEFAULTBURNVALUE,
                    attribute=None, filter=None, alltouched=False, datatype=numpy.uint8, 
                    tempdir='.', driver=DEFAULTDRIVERNAME, 
                    driveroptions=DEFAULTCREATIONOPTIONS,
                    nullval=0):
        """
        Constructs a Vector object. filename should be a path to an OGR readable
        dataset. 
        inputlayer should be the OGR layer number (or name) in the dataset to rasterize.
        burnvalue is the value that gets written into the raster inside a polygon.
        Alternatively, attribute may be the name of an attribute that is looked up
        for the value to write into the raster inside a polygon.
        If you want to filter the attributes in the vector, pass filter which
        is in the format of an SQL WHERE clause.
        By default, only pixels whose centre lies within the a polygon get
        rasterised. To have all pixels touched by a polygon rasterised, set
        alltouched=True. 
        datatype is the numpy type to rasterize to - byte by default.
        tempdir is the directory to create the temporary raster file.
        driver and driveroptions set the GDAL raster driver name and options
        for the temporary rasterised file.

        """
        # open the file and get the requested layer
        self.ds = ogr.Open(filename)
        if self.ds is None:
            raise rioserrors.ImageOpenError("Unable to open OGR dataset: %s" % filename)
        self.layer = self.ds.GetLayer(inputlayer)
        if self.layer is None:
            raise rioserrors.VectorLayerError("Unable to find layer: %s" % inputlayer)
        layerdefn = self.layer.GetLayerDefn()

        # check the attribute exists
        if attribute is not None:
            fieldidx = layerdefn.GetFieldIndex(attribute)
            if fieldidx == -1:
                raise rioserrors.VectorAttributeError("Attribute does not exist in file: %s" % attribute)

        # check they have passed a polygon type
        validtypes = [ogr.wkbMultiPolygon,ogr.wkbMultiPolygon25D,ogr.wkbPolygon,ogr.wkbPolygon25D]
        if layerdefn.GetGeomType() not in validtypes:
            raise rioserrors.VectorGeometryTypeError("Can only rasterize polygon types")

        # apply the attribute filter if passed
        if filter is not None:
            self.layer.SetAttributeFilter(filter)     

        # create a temporary file name based on raster
        # driver extension
        # save GDAL driver object for dataset creation later
        self.driver = gdal.GetDriverByName(driver)
        drivermeta = self.driver.GetMetadata()
        ext = ''
        if "DMD_EXTENSION" in drivermeta:
            ext = '.' + drivermeta["DMD_EXTENSION"]
        # save the driver options
        self.driveroptions = driveroptions

        (fileh,self.temp_image) = tempfile.mkstemp(ext,dir=tempdir)
        # close the file so we can get GDAL to clobber it
        # probably a security hole - not sure
        os.close(fileh)

        # create the options string
        self.options = []
        if attribute is not None:
            self.options.append('ATTRIBUTE=%s' % attribute)
        if alltouched:
            self.options.append('ALL_TOUCHED=TRUE')

        # store the data type
        self.datatype = datatype
        # burnvalue
        self.burnvalue = burnvalue
        # Value used for area not burned
        self.nullval = nullval

    def cleanup(self):
        """
        Remove temproary file and close dataset

        """
        if os.path.exists(self.temp_image):
            os.remove(self.temp_image)
        del self.layer
        self.layer = None
        del self.ds
        self.ds = None

    def __del__(self):
        # destructor - call cleanup
        self.cleanup()


class VectorReader(object):
    """
    Class that performs rasterization of Vector objects.

    """
    def __init__(self, vectorContainer):
        """
        vectorContainer is a single Vector object, or a 
        list or dictionary that contains
        the Vector objects of the files to be read.
        If a Vector object is passed, a single block is returned
        from rasterize(), if a list is passed, 
        a list of blocks is returned, if a dictionary a dictionary is
        returned for each call to rasterize() with the same keys.

        """
        self.vectorContainer = vectorContainer

    @staticmethod
    def rasterizeSingle(info, vector, pixtolerance=DEFAULTPIXTOLERANCE):
        """
        Static method to rasterize a single Vector for the extents
        specified in the info object. A test is performed to ensure
        the projections of vector and raster are the same to within
        pixtolerance.
        A single numpy array is returned of rasterized data.
        """
        try:
            if info.isFirstBlock():
                #if not sameProj:
                #    # Replace vector.layer with a vrt of same proj
                    
                # Haven't yet rasterized, so do this for the whole workingGrid
                (nrows, ncols) = info.workingGrid.getDimensions()
                numLayers = 1
                gdaldatatype = NumpyTypeToGDALType(vector.datatype)
                outds = vector.driver.Create(vector.temp_image, ncols, nrows, numLayers, 
                    gdaldatatype, vector.driveroptions)
                if outds is None:
                    raise rioserrors.ImageOpenError("Unable to create temporary file %s" % vector.temp_image)
                outds.SetGeoTransform(info.getTransform())
                outds.SetProjection(info.getProjection())
                err = gdal.RasterizeLayer(outds, [1], vector.layer, burn_values=[vector.burnvalue], 
                                        options=vector.options)
                if err != gdal.CE_None:
                    raise rioserrors.VectorRasterizationError("Rasterization failed")
                
                vector.rasterDS = outds
        except Exception:
            # if there has been an exception
            # ensure all the files are cleaned up
            vector.cleanup()
            # and the exception raised again
            raise
        
        xoff, yoff = info.getPixColRow(0, 0)
        blockcols, blockrows = info.getBlockSize()
        margin = info.getOverlapSize()
        block = ImageReader.readBlockWithMargin(vector.rasterDS, xoff, yoff, blockcols, blockrows, 
            vector.datatype, margin, [vector.nullval])

        return block

    def rasterize(self, info, pixtolerance=DEFAULTPIXTOLERANCE):
        """
        Rasterize the container of Vector objects passed to the 
        constuctor. Returns blocks in the same form as the 
        container passed to the constructor.
        A test is performed to ensure
        the projections of vector and raster are the same to within
        pixtolerance.

        """
        if isinstance(self.vectorContainer, dict):
            blockContainer = {}
            for key in self.vectorContainer:
                vector = self.vectorContainer[key]
                block = self.rasterizeSingle(info, vector, pixtolerance)
                blockContainer[key] = block

        elif isinstance(self.vectorContainer, Vector):
            blockContainer = self.rasterizeSingle(info, self.vectorContainer, pixtolerance)
    
        else:
            blockContainer = []
            for vector in self.vectorContainer:
                block = self.rasterizeSingle(info, vector, pixtolerance)
                blockContainer.append(block)

        return blockContainer
        
    def close(self):
        """
        Closes all datasets and removes temporary files.

        """
        if isinstance(self.vectorContainer, dict):
            for key in self.vectorContainer:
                vector = self.vectorContainer[key]
                vector.cleanup()

        elif isinstance(self.vectorContainer, Vector):
            self.vectorContainer.cleanup()

        else:
            for vector in self.vectorContainer:
                vector.cleanup()
