Release Notes
=============

Version 1.4.8 (2019-03-21)
--------------------------

Bug Fixes:
  * Allow vector null value to be given as integer datatype without upsetting GDAL
  * Disable gdalwarp's use of overviews when resampling to lower resolution, by 
    giving it the '-ovr NONE' switch. This behaviour started with GDAL 2.0, and 
    should be considered unreliable, and therefore is not to be used by RIOS. 
    An option is provided for those who wish to live dangerously. 

Version 1.4.6 (2018-08-03)
--------------------------

Bug Fixes:
  * In certain circumstances, when multiple resample methods were in use on different inputs, 
    it was possible for these to get mixed and the wrong method used for some files. This
    is now fixed. 

Version 1.4.5 (2018-03-14)
--------------------------

New Features:
  * Added $RIOS_USE_VRT_FOR_RESAMPLING environment variable to allow the use of VRTs in the resampling code to be disabled if needed (i.e. buggy GDAL versions).
  
Bug Fixes:
  * Improve exception handling in calcstats.py
  * Improve code that runs gdalwarp and report errors better.
  * Update bundled cloudpickle code to latest version which fixes a problem with Python 3.6.
  * Fix a problem with testing the multiprocessing code under Windows.

Version 1.4.4 (2017-09-11)
--------------------------

New Features:
  * Added rioscalcstats.py utility and created a 'cmdline' module to handle command line code.
  * Added $RIOS_NOCMDLINE environment variable to suppress installation of command line scripts for users creating entry points.
  * Add ratapplier.copyRAT function.

Bug Fixes:
  * Check sys.stdout isatty() before using it for progress.
  * Use the GDAL SetLinearBinning function rather than setting metadata for versions of GDAL where this works properly.
  * Add progress support to ratapplier. 

Version 1.4.3 (2016-06-10)
--------------------------

Bug Fixes:
  * Many minor fixes to better support sites who use $RIOS_DFLT_DRIVER to configure their default output raster format to GTiff. Apologies - we usually use HFA, so often don't notice GTiff problems. 
  * Those sites (I am looking at you, LandcareNZ) who want the automatic random colour table added to thematic outputs will now have to turn this on using the $RIOS_DFLT_AUTOCOLORTABLETYPE environment variable. See doco for details. 
  * The default RIOS block size has been changed to 256 pixels, which is much more likely to align with a number of common formats. As before, other values can still be specified in the usual ways (via controls, or environment variables). 

Version 1.4.2 (2016-01-05)
--------------------------

Bug Fixes:
  * More robust scheme for handling deletion of pre-existing output files. This will now cope better with whatever driver ought to be used to perform the deletion. It will also not generate spurious warning messages to stderr. 
  * The test framework is now independent of scipy, so the whole installation can be done without scipy, should that be required.
  * Fixed a bug introduced in 1.4.1, in which the overviews were always generated using averaging, regardless of the LAYER_TYPE. 
  * Fixed some recently introduced inconsistencies in setup.py. 

Version 1.4.1 (2015-11-23)
--------------------------

New Features:
  * Added options to ApplierControls for manipulating the overviews (i.e. pyramid layers) of 
    output files. Also some environment variables for defaulting overview behaviour. 
  * Added code to check the creation options when the output driver is GTiff. If used with
    default settings, this would create huge output files, because of the GTiff driver's
    inability to re-use space within the file. The fix requires that the RIOS block size
    be a multiple of the GTiff block size. Violation of this will now raise an exception. 
    WARNING: This change will cause existing programs which write GTiff output files, 
    running with default settings to now raise an exception. The correct fix is to 
    change the RIOS block size. If you do a lot of GTiff output, it is strongly recommended
    to set RIOS_DFLT_BLOCKXSIZE and RIOS_DFLT_BLOCKYSIZE to 256 (which is probably a better 
    default anyway). 
  * Added environment variables to set driver-specific default creation options, instead of 
    the previous single default value. This allows programs to change their driver without having 
    to explicitly hard-wire the right set of creation options to use for each possible driver. 
    Instead, they are configured in the environment, per driver. 


Bug Fixes:
  * Fixed incorrect assignment of loggingstream in sub-jobs, when using parallel 
    job manager sub-system

Version 1.4.0 (2015-09-23)
--------------------------

New Features:
  * Added new, more flexible implementation of parallel processing. Has a number of drivers, allowing a number of different models of parallelism. Drivers for using mpi, multiprocessing module, simple sub-processes, batch queues with PBS or SLURM. See rios.parallel.jobmanager docstring for help. 
  * Added capacity for selecting which raster layers are read on input.
  * Docstrings formatted for Sphinx. This allows doc to be hosted on http://rioshome.org/, at the expense of looking stoopid when displayed with Python's own help() and pydoc utilities. 

Bug Fixes:
  * Prevent pre-RFC40 metadata access from clobbering the histogram.
  * Fixed vector test code to work with more recent versions of numpy
  * Cope with integer overflow in GDAL's GetHistogram() function.
  * Loop the stats test code over a number of different file formats
  * Added $RIOS_HISTOGRAM_IGNORE_RFC40 environment variable, as a way of ignoring RFC40 for histogram code. Useful for HFA files, and appears to be a bug still in RFC40 code for HFA driver. This is just a workaround,  not a complete fix. 

Removed Features:
  * Removed deprecated readerinfo functions getPixCoord() and 
    getBlockBounds()

Version 1.3.1 (2014-05-28)
--------------------------

New Features:
  * Standalone program riosprintstats.py, for printing the stats of a raster in a simple format. 
  * fileinfo.ImageLayerStats and calcstats now use the RFC40 RAT interface to read/write the histogram, if it is available (comes with GDAL 1.11.0). Purely for greater efficiency. 

Bug Fixes:
  * Some Python 3 compatability fixes in the test suite. 
  * Tidied up test suite so it counts errors correctly. 
  * Minor fixes in rios.parallel code. 

Version 1.3.0 (2014-03-26)
--------------------------

New Features:
  * Added rios.ratapplier module. This is designed for working with very large Raster Attribute Tables (millions of rows), and allows the user to apply a function block by block through the table, for memory efficiency. For best results, this relies on GDAL 1.11, which is expected to be released within a few weeks. 
  * Added 'outPROJ' as parameter to the getCorners() function - thanks to Markus. 
  * User can control the value used as null when rasterising a vector input, to avoid clashes with a valid column value (controls.setVectorNull()). 

Bug Fixes:
  * rat.writeColumn copes with unicode string arrays when using turbogdal assistance
  * Fixed bug in ReaderInfo.getPixRowColBlock(), in which it mixed up rows and columns.
  * Use GDAL to remove temporary raster file, so that auxiliary files also get removed
  * Fixed metadata representation of histogram, which was previously dropping the final count (which would commonly have been zero, but not necessarily)

Version 1.2.0 (2013-12-07)
--------------------------

New Features:
  * Added rios.fileinfo module. Contains utility classes for gathering information about raster files, outside of the methods given in the ReaderInfo class. The intention is that using fileinfo classes before calling applier.apply(), and passing information in, is simpler and neater than some of the ReaderInfo methods. 
  * Added rios.parallel, with functions to over-ride the normal applier.apply() function, to make parallel version of the main RIOS block loop. Currently contains a version using Python multiprocessing package, and a version using mpi4py. These are somewhat experimental - early days yet. 

Bug Fixes:
  * Precision fix on the on-the-fly reprojection. When using pixel sizes with many digits of precision, some precision was being lost, resulting in incorrect reprojection and consequent mis-alignment of the resulting raster relative to the reference image. 

Version 1.1.7 (2013-11-11)
--------------------------

Further bug fix on statistics calculation:
  * Histogram calculation for float datatypes would limit bin width to 1, regardless of range of data values. Now selects bin width sensibly. This results in much better estimates of median and mode in statistics calculation for float rasters with small values. 

Version 1.1.6 (2013-11-07)
--------------------------

Minor bug fixes and enhancements:
  * Added ReaderInfo.getPixRowColBlock() function, making it easier to run debugging of a single pixel
  * Notes in docstrings for getPixCoord(), getPixColRow() and getBlockBounds() to indicate that getBlockCoordArrays() is preferred. 
  * Fixed bug in median calculation in calcstats, and added to tests of statistics calculation in testrios.py, along with note that it requires the GDAL bug fixes in tickets `#4750 <http://trac.osgeo.org/gdal/ticket/4750>`_ and `#5289 <http://trac.osgeo.org/gdal/ticket/5289>`_ in order to get the median and mode correct in all cases. 

Version 1.1.5 (2013-10-23)
--------------------------

Minor bug-fixes and enhancements: 
  * Fixed bug with rounding of coordinates. Depending on exact values of grid coordinates, this could sometimes result in incorrect calculation of grid alignments, etc. Reported by Jane Whitcomb (many thanks!). 
  * Preparation for GDAL changes in GDAL's `RFC40 <http://trac.osgeo.org/gdal/wiki/rfc40_enhanced_rat_support>`_, for efficient raster attribute table handling
  * Some Python 3.3 string handling incompatibilities
  * Environment variables for some other default values - $RIOS_DFLT_FOOTPRINT, $RIOS_DFLT_BLOCKXSIZE, $RIOS_DFLT_BLOCKYSIZE, and $RIOS_DFLT_OVERLAP
  * Some improvements in handling of column usage and data types in the rios.rat module
  * Output layer names settable via ApplierControls
  * Fixed a few docstrings

Version 1.1.4 (2013-07-29)
--------------------------

  * Trap tests on thematic LAYER_TYPE on formats which don't support it
  * Fix info.getBlockCoordArrays() so it copes when there is an overlap set
  * More robust behaviour with $RIOS_DFLT_DRIVEROPTIONS
  * More robust testing of GDAL version, for avoiding GDAL bugs
  * Python-3 compatability fixes. Formatting of error message strings. Deal with change in behaviour of round() for -x.5 case. 
  * Maintain attribute filter on a vector, when the vector is reprojected

Version 1.1.3 (2013-01-10)
--------------------------

  * Some fixes for Python 3 compatability
  * Allow specification of column usage in rat.writeColumn() and rat.writeColumnToBand()
  * Added BOUNDS_FROM_REFERENCE as an alternative to INTERSECTION or UNION

Version 1.1.2 (2012-12-04)
--------------------------

This release is just small bug fixes:
  * Better handling of datatype of null values
  * Improvements to Raster Attribute Table handling, especially for very large tables. This includes the optional use of Sam's TurboRAT library, if it is available, for greatly improved speed on very large attribute tables. 
  * Improved docstrings for methods in readerinfo class
  * getBlockCoordArrays() method, for easier access to the coordinates of each pixel
  * Implemented Pete B's suggestions for calculation of stats in the more obscure datatypes
  * Trap GDAL's silly "error" message when calculating stats on a raster which is all null. 

Version 1.1.1 (2012-06-26)
--------------------------

  * Allow point and line vectors as inputs. Previously they were arbitrarily dis-allowed, which was good, because earlier versions of GDAL's rasterize routine (before GDAL 1.9.0) had a bug which meant that they were mis-registered. However, they are now allowed, with a check on the GDAL version number to ensure it has the bug fix
  * Better use of return code in on-the-fly reprojection of vectors
  * Cast result of getNoDataValueFor() to same type as dataset
  * Allow multi-band files to be thematic. Previously they were arbitrarily dis-allowed, possibly because of concerns about some format drivers. 

Version 1.1.0 (2012-01-23)
--------------------------

  * Added vector input capability

Version 1.0.1 (2011-12014)
--------------------------

  * Bug fixes. 
  * Added/finalized rat.py color table and raster attribute table access

Version 1.0 (2011-12-08)
--------------------------

  * First public release
