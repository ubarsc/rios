================
Applier Examples
================

Simple Example
--------------
::

    # Reads in two input files and adds them together. 
    # Assumes that they have the same number of bands. 
    from rios import applier
        
    # Set up input and output filenames. 
    infiles = applier.FilenameAssociations()
    infiles.image1 = "file1.img"
    infiles.image2 = "file2.img"
 
    outfiles = applier.FilenameAssociations()
    outfiles.outimage = "outfile.img"
 
    # Set up the function to be applied
    def addThem(info, inputs, outputs):
        # Function to be called by rios.
        # Adds image1 and image2 from the inputs, and
        # places the result in the outputs as outimage. 
        outputs.outimage = inputs.image1 + inputs.image2
 
    # Apply the function to the inputs, creating the outputs. 
    applier.apply(addThem, infiles, outfiles)


The program shown above is complete, and would work, assuming the two input files existed. 
It would create a file called outfile.img, whose pixel values would be the sum of the 
corresponding pixels in the two input files, file1.img and file2.img.

The user-supplied function addThem is passed to the :func:`rios.applier.apply` function, which applies it across the image. 
Inside the addThem function, we are given the info object (which we are not making use of in this case, but we are given it anyway), 
and the inputs and outputs objects, which contain the data from the raster files defined earlier. 
The data is presented as a set of numpy arrays, of the datatype corresponding to that in the raster files. 
It is the responsibility of the user to manage all conversions of datatypes.

All blocks of data are 3-d numpy arrays. The first dimension corresponds to the number of layers in the image file, 
and will be present even when there is only one layer.

The datatype of the output file(s) will be inferred from the datatype of the numpy arrays(s) given in the outputs object. 
So, to control the datatype of the output file, use the numpy astype() function to control the datatype of the output arrays.         

Passing Other Data Example
--------------------------

A mechanism is provided for passing other data to and from the user function, apart from the raster data itself. 
This is obviously useful for passing parameters into the processing. It can also be used to pass information out again, 
and to preserve data between calls to the function, since the otherargs object is preserved between blocks.

When invoking the :func:`rios.applier.apply` function, a fourth argument can be given, otherargs. 
This can be any python object, but will typically be an instance of the :class:`rios.applier.OtherInputs` class. 
If supplied, then the use function should also expect to take this as its fourth argument. It will be supplied to every call to the user function, 
and rios will do nothing to it between calls.

The OtherInputs class is simply a container, so that the application can attach arbitrary attributes to it, 
and they will be accessible from inside the user function.

A simple example, using it to pass in a single parameter, might be a program to multiply an input raster by a scale value and add an offset (showing only the relevant lines of code)::

    def rescale(info, inputs, outputs, otherargs):
        outputs.scaled = inputs.img * otherargs.scale + otherargs.offset

    otherargs = applier.OtherInputs()
    otherargs.scale = scaleval
    otherargs.offset = offsetval
    applier.apply(rescale, infiles, outfiles, otherargs)


An example of using the otherargs object to accumulate information across blocks might be a program to calculate some statistic 
(e.g. the mean) across the whole raster (showing only the relevant lines of code) ::
            
    def accum(info, inputs, outputs, otherargs):
        tot = float(inputs.img.sum())
        n = inputs.img.size
        otherargs.tot += tot
        otherargs.count += n

    otherargs = applier.OtherInputs()
    otherargs.tot = 0.0
    otherargs.count = 0
    applier.apply(accum, infiles, outfiles, otherargs)
    print 'Average value = ', otherargs.tot / otherargs.count

The *tot* and *count* values on otherargs are initialized before calling :func:`rios.applier.apply`, and are accumulated between blocks, 
as RIOS loops over all blocks in the image. After the call to :func:`rios.applier.apply`, these attributes have their final values, and we can calculate the final average.

Of course, there already exist superior ways of calculating the mean value of an image, but the point about using RIOS to do
something like this would be that: (a) opening the input rasters is taken care of; and (b) it takes up very little memory, as only small blocks are in memory at one time. The same mechanism can be used to do more specialized calculations across the image(s).

Note that there are no output rasters from the last example - this is perfectly valid.         

Examples Controlling Reading/Writing
------------------------------------

By default, if the input rasters are on different projections, or different
pixel sizes, or even just mis-aligned pixel grids, then RIOS will raise an
exception. However, if requested to do so, it will reproject
on-the-fly, and the arrays presented to the user function will be in this
resamped grid, as will the output rasters. This is enabled by telling it which
of the input rasters should be used as the reference, and all other inputs
will be reprojected onto the reference projection. This is done as follows
(showing only the relevant lines)::

    controls = applier.ApplierControls()
    controls.setReferenceImage(infiles.img2)
    applier.apply(userFunc, infiles, outfiles, controls=controls)

The method used for resampling is controlled by calling
controls.setResampleMethod().

Resampling can also be done onto a different reference than any of the
inputs, by using the setReferencePixgrid method on the controls object.
This requires understanding the :class:`rios.pixelgrid.PixelGridDefn` class.
Assuming we already know the desired projection and geotransform and so on,
this would look something like the following::

    controls = applier.ApplierControls()
    pixgrid = pixelgrid.PixelGridDefn(geotransform=gt, nrows=nrows,
        ncols=ncols, projection=outWKT)
    controls.setReferencePixgrid(pixgrid)

The extent of the output grid can be controlled using the footprint type. This
can be one of applier.INTERSECTION, applier.UNION, or
applier.BOUNDS_FROM_REFERENCE. The INTERSECTION
will be the largest extent which is still contained within all of the input
images, while the UNION will be the smallest extent which will contain all
the input images. If BOUNDS_FROM_REFERENCE is used, the output extent will be
the same as that for the selected reference image or pixgrid, regardless of the
extents of the other input images. For example::

    controls.setFootprintType(applier.BOUNDS_FROM_REFERENCE)

Some of the `set` methods on the controls object allow the parameter
to be specific to only one of the input/output files, by specifying
the name of the image to which it applies. For example::

    outfiles.outimg1 = 'mainoutput.tif'
    controls.setOutputDriverName('GTiff', imagename='outimg1')

which would make that output file in GTiff format, without affecting
other output files.

Most other aspects of reading and writing the inputs and output can
be controlled via the controls object. Please see the full documentation
for the :class:`rios.applier.ApplierControls` class for all options and
details.

        
Arbitrary Numbers of Input (and Output) Files
---------------------------------------------

Each name on the infiles or outfiles object can also be a list of filenames, 
instead of a single filename. This will cause the corresponding attribute on the 
inputs/outputs object to be a list of blocks, instead of a single block. 
This allows the function to process an arbitrary number of files, without having 
to give each one a separate name within the function. An example might be a function 
to average a number of raster files, which should work the same regardless of 
how many files are to be averaged. This could be written as follows::

    import sys
    from rios import applier

    def doAverage(info, inputs, outputs):
        """
        Called from RIOS. Average the input files.
        Assumes first image contains the totals for the input images.
        """
        tot = inputs.imgs[0].astype(numpy.float32)
        for img in inputs.imgs[1:]:
            tot = tot + img
        avg = tot / len(inputs.imgs)
        outputs.avg = avg.astype(img.dtype)

    infiles = applier.FilenameAssociations()
    # names of input images from command line
    infiles.imgs = sys.argv[1:-1]

    outfiles = applier.FilenameAssociations()
    # Last name given on the command line is the output
    outfiles.avg = sys.argv[-1]
    applier.apply(doAverage, infiles, outfiles)

Another example of this might be a function that takes one input file and 
creates a new file for each layer that works regardless of the number of layers.::

    import sys
    import numpy
    from rios import applier

    def doSplit(info, inputs, outputs):
        """
        Splits the input file into a separate file for each layer.
        """
        outputList = []
        for layer in range(inputs.img.shape[0]):
            output = inputs.img[0]
            output = numpy.expand_dims(output, axis=0) # convert single layer to 3d array
            outputList.append(output)

        outputs.outImages = outputList

    infiles = applier.FilenameAssociations()
    # name of input image is first on command line
    infiles.img = sys.argv[1]

    outfiles = applier.FilenameAssociations()
    # the other names on the command line are the output images (one for each layer of the input file)
    outfiles.outImages = sys.argv[2:]

    applier.apply(doSplit, infiles, outfiles)
        
If required, lists could be used for both input and output images.

Filters and Overlap
-------------------

Because RIOS operates on a per block basis, care must be taken to 
set the overlap correctly when working with filters::

    from rios import applier
    from scipy.ndimage import uniform_filter

    controls = applier.ApplierControls()
    # for a 3x3 the overlap is 1, 5x5 overlap is 2 etc
    controls.setOverlap(1)

    def doFilter(info, inputs, outputs):
        # does a 3x3 uniform filter. Select just one input band at a time
        # or you will get an unexpected result
        filtered = uniform_filter(inputs.indata[0], size=3)
        # create 3d image from 2d array
        outputs.result = numpy.expand_dims(filtered, axis=0) 

    applier.apply(doFilter, infiles, outfiles, controls=controls)

Many other `Scipy Filters <http://docs.scipy.org/doc/scipy/reference/ndimage.html>`_ are also available
and can be used in a similar way.

Vector Inputs
-------------

As of RIOS 1.1, it is possible for the input files to be vector files as well as raster files. 
Any polygon file which can be read using GDAL/OGR is acceptable. The polygons will be rasterized on the fly, 
and presented inside the user's function as numpy arrays, in exactly the same way as would normally happen with raster inputs.

Some attributes are added to the :class:`rios.applier.ApplierControls` object to manage the rasterizing process, 
setting such things as a burn value (i.e. the value in the array corresponding to pixels "inside" the polygons. 

Concurrency
-----------

As of RIOS 2.0, there is much stronger support for concurrency in both reading of image data
and computing the user function. The reader is referred to :doc:`concurrency` for a detailed
description of the concurrency model. It is activated by creating a ConcurrencyStyle object,
and giving this to the controls object::

    from rios import applier

    controls = applier.ApplierControls()
    concurrency = applier.ConcurrencyStyle(numReadWorkers=3)
    controls.setConcurrencyStyle(concurrency)

The earlier parallel processing paradigm (from version 1.4) is now disabled, and should not be used.

Other Examples
-----------------

The good people from the `Spectraldifferences blog <https://spectraldifferences.wordpress.com/tag/rios/>`_
have made available some other examples of their uses of RIOS.
