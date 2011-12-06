
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
                    driveroptions=DEFAULTCREATIONOPTIONS):
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
            raise rioserros.ImageOpenError("Unable to open OGR dataset: %s" % filename)
        self.layer = self.ds.GetLayer(inputlayer)
        if self.layer is None:
            raise rioserrors.VectorLayerError("Unable to find layer: %s" % inputlayer)
        layerdefn = self.layer.GetLayerDefn()

        # check the attribute exists
        if attribute is not None:
            fieldidx = layerdefn.GetFieldIndex(attribute)
            if fieldidx == -1:
                raise rioserros.VectorAttributeError("Attribute does not exist in file: %s" % attribute)

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
            # create an output dataset of the right size
            # with the driver specified by vector.
            (blockxsize, blockysize) = info.getBlockSize()
            gdaldatatype = NumpyTypeToGDALType(vector.datatype)
            outds = vector.driver.Create(vector.temp_image, blockxsize, blockysize, 1, 
                                    gdaldatatype, vector.driveroptions)
            if outds is None:
                raise rioserrors.ImageOpenError("Unable to create temporary file %s" % vector.temp_image)

            # for some reason info.getTransform() 
            # returns the transform for the whole image
            # need to adjust it to be for the block
            transform = list(info.getTransform())
            blocktl,blockbr = info.getBlockBounds()
            transform[0] = blocktl.x
            transform[3] = blocktl.y
            outds.SetGeoTransform(transform)

            projection = info.getProjection()
            outds.SetProjection(projection)

            if info.isFirstBlock():
                # if processing the first block, we 
                # checks to ensure the projection
                # of the raster is 'equivalent enough' to
                # that of the vector. This is because:
                # 1) We can't do on-the-fly reprojection
                #    due to limitation of the GDAL bindings
                #    (can't create the pfnTransformer argument)
                #    so we need to stop if the projections aren't the same.
                # 2) Due to differences with the way projections
                #    are stored in different formats often they
                #    end up being slightly different when they
                #    should be the same.
                spatialref = vector.layer.GetSpatialRef()
                if not info.workingGrid.equivalentProjection(spatialref, pixtolerance):
                    raise rioserrors.VectorProjectionError("Raster and Vector projections do not match")


            # now perform the actual rasterisation
            err = gdal.RasterizeLayer(outds, [1], vector.layer, burn_values=[vector.burnvalue], 
                                        options=vector.options)

            if err != gdal.CE_None:
                raise rioserrors.VectorRasterizationError("Rasterization failed")

        finally:
            # ensure this is called if an exception
            # is raised to remove temp files etc.
            # commented out because it seems to be called all the time
            # any ideas?
            #vector.cleanup()
            pass

        # normally GDAL just returns a 2D array for single
        # layer datasets, but to fit with the RIOS paradigm
        # we need to convert to 3D
        datashape = (1,blockysize,blockxsize)
        data = numpy.empty(datashape, vector.datatype)
        data[0] = outds.ReadAsArray()
        del outds

        return data

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
            blockContainer = []
            for vector in self.vectorContainer:
                vector.cleanup()
