=======================
Map Projections In RIOS
=======================
RIOS is designed to work with whatever map projections are required, using the
excellent support in ``osgeo.osr``. It attempts to make this as transparent as
possible, and mostly the user does not really need to worry too much.

When working entirely within a single projection and pixel size & alignment, there is never
anything to worry about. RIOS will also (when requested) re-project inputs to match the working
grid, and in general this is quite seamless.

However, map projections are a complicated topic, and there are a few traps &
tricks the user may need to be aware of.

Mixing Map Projections
----------------------
If all the input files have the same projection, pixel size, and grid alignment,
RIOS will write the output files in the same grid.

If one or more inputs is different, then you will need to choose one as the reference
image, and others will be reprojected to match it::

    infiles.inimg = "someImage.tif"
    infiles.otherimage = "otherImage.tif"
    controls.setReferenceImage("inimg")

You can specify either the internal name (``inimg``) or the external file name (``someImage.tif``).
The data from ``otherImage.tif`` will be reprojected to match ``someImage.tif`` before it is
presented to the user. There is no requirement that the reference image be one of the input files,
although that is often the case.

The resampling method will default to nearest neighbour, but this can be changed
(controls.setResampleMethod), and the user should give some attention to what method
to use for each input file.

Controlling the Working Grid
----------------------------
In some cases, one may wish to have the working grid match that of another file which
is not involved in the current run. This is done by setting a reference image,
specifying the external file name::

    controls.setReferenceImage("someOtherFile.tif")

This will take the projection, pixel size, and grid alignment from the reference file,
but still calculate the extent from the input files. If you also wish the extent to come
from the reference image, you should also set the footprint to be ``BOUNDS_FROM_REFERENCE``::

    controls.setFootprintType(applier.BOUNDS_FROM_REFERENCE)

It is also possible to specify the grid programmatically, without reference to another file,
allowing outputs to be written into a specific grid, regardless of what other files
are available. This uses an internal structure called a PixelGridDefn, e.g.::

    from rios import pixelgrid

    pixGrid = pixelgrid.PixelGridDefn(projection=wkt, xRes=xRes, yRes=yRes,
        xMin=xMin, xMax=xMax, yMin=yMin, yMax=yMax)
    controls.setReferencePixgrid(pixGrid)

This is equivalent to having an external file with this projection and grid alignment and calling
``setReferenceImage``. As above, one can also use ``BOUNDS_FROM_REFERENCE`` to also take the extent
from this pixGrid object, otherwise it will be calculated from inputs, as normal.

See the PixelGridDefn class docstring for more details on its use.

Restricting Files For Calculating Extent
----------------------------------------
By default, RIOS will calculate the INTERSECTION or UNION extent using the extents of all of
the input files. In some special cases it may be necessary to restrict this to a specific
set of files. This can be specified on the controls::

    controls.setFilesForExtent(filelist)

This takes a list of external file names. The calculation will be done in the reference projection
and pixel size & alignment, as normal, but the extents used for INTERSECTION or UNION will
be those of the given files. There is no requirement that these also be input files, although
usually it would be some subset of the inputs.

Longitude +/-180 degrees
------------------------
If the working grid crosses the line of 180 degrees longitude, *and* some inputs
require re-projection, it is quite likely that something will not behave as
expected. There are a range of different ways that things might go wrong, depending
on the combination of projections and extents involved, but if you are working
in this area, be prepared for trouble. There is likely to be no easy solution,
and probably the easiest thing is to split the region of interest in two, on either
side of the offending meridian, and process them separately.

If no inputs require reprojection, there is unlikely to be any particular problem.

Similar comments to those above also apply to working over the north or south pole.
