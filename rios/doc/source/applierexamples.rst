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

A mechanism is proved for passing other data to and from the user function, apart from the raster data itself. 
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

Of course, there already exist superior ways of calculating the mean value of an image, but the point about using rios to do 
something like this would be that: (a) opening the input rasters is taken care of; and (b) it takes up very little memory, as only small blocks are in memory at one time. The same mechanism can be used to do more specialized calculations across the image(s).

Note that there are no output rasters from the last example - this is perfectly valid.         

Controlling Reading/Writing Example
-----------------------------------

A simple example would be to allow resampling of input rasters. 
Normally, rios will raise an exception if the input rasters are on different projections, 
but if requested to do so, it will reproject on-the-fly. This is enabled by telling it 
which of the input rasters should be used as the reference (all other inputs will be 
reprojected onto the reference projection. This is done as follows (showing only the relevant lines)::

    controls = applier.ApplierControls()
    controls.setReferenceImage(infiles.img2)
    applier.apply(userFunc, infiles, outfiles, controls=controls)

Other controls which can be manipulated are detailed in the full python documentation for the :class:`rios.applier.ApplierControls` class.

        
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
        "Called from RIOS. Average the input files"
        tot = inputs.imgs[0].astype(numpy.float32)
        for img in inputs.imgs[1:]:
            tot = tot + img
        avg = tot / len(inputs.imgs)
        outputs.avg = avg.astype(img.dtype)

    infiles = applier.FilenameAssociations()
    # names of imput images
    infiles.imgs = sys.argv[1:-1]
    # Last name given is the output
    outfiles.avg = sys.argv[-1]
    applier.apply(doAverage, infiles, outfiles)

Vector Inputs
-------------

As of RIOS 1.1, it is possible for the input files to be vector files as well as raster files. 
Any polygon file which can be read using GDAL/OGR is acceptable. The polygons will be rasterized on the fly, 
and presented inside the user's function as numpy arrays, in exactly the same way as would normally happen with raster inputs.

Some attributes are added to the :class:`rios.applier.ApplierControls` object to manage the rasterizing process, 
setting such things as a burn value (i.e. the value in the array corresponding to pixels "inside" the polygons. 

Advanced Examples
-----------------

More advanced RIOS examples are available from the `Spectraldifferences site <https://spectraldifferences.wordpress.com/tag/rios/>`_.
