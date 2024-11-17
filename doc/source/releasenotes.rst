Release Notes
=============

Version 2.0.5 (2024-11-18)
--------------------------
Improvements
  * Output file statistics, histograms & overviews (i.e. pyramids) are now
    computed incrementally as each block is processed, rather than when the
    output files are closed. The old way required several extra passes through
    the output data, so this change gives useful speedups, with no change needed
    to the application script. Note that the GDAL KEA driver required a fix to
    support this, so for KEA outputs, best results require GDAL version 3.9.3.
    New methods are added to the controls object to fully control this new
    behaviour (https://github.com/ubarsc/rios/pull/116).
  * Better disk space management when using CW_AWSBATCH compute workers
    (https://github.com/ubarsc/rios/pull/112)
  * Added controls.setJobName(), to support better tracking of multiple
    jobs and compute workers. (https://github.com/ubarsc/rios/pull/113)

Bug Fixes
  * When using CW_AWSBATCH compute workers, ensure consistency between the
    install location of RIOS and the Dockerfile being used
    (https://github.com/ubarsc/rios/pull/109).

Version 2.0.4 (2024-07-16)
--------------------------
Bug Fixes
  * Ensure all GDAL files are closed before exit, to avoid Panasas disk array
    file deletion problems.
  * Exceptions from read workers in single compute worker case must also
    go into the exception queue.
  * Check consistency of per-image calls to methods of ApplierControls

Version 2.0.3 (2024-06-25)
--------------------------
Bug Fixes
  * Fix problems with vector inputs in different projection to working grid,
    introduced in 2.0.0. (https://github.com/ubarsc/rios/pull/94)

Version 2.0.2 (2024-06-19)
--------------------------

Bug Fixes
  * Cope with numpy-2.0 changes in scalar handling (https://github.com/ubarsc/rios/pull/90).
  * Fix the now-deprecated ImageReader class so it does not depend on the
    now-deprecated ReaderInfo.setBlockDataset (https://github.com/ubarsc/rios/pull/89).
    The ImageReader class should be avoided in future.

Version 2.0.1 (2024-06-12)
--------------------------

Bug Fixes
  * Updated one incompatibility with numpy-2
  * Test suite guards more gracefully against unavailable tests

Improvements
  * Setup is now fully controlled by pyproject.toml, with no setup.py

Version 2.0.0 (2024-05-23)
--------------------------

New Features
  * New options for concurrency, strongly supported by a new internal 
    architecture. The previous parallel processing had been tacked on to
    a non-parallel architecture, and was never very good.

    - New concurrency model is more efficient and more flexible and
      configurable. See :doc:`concurrency` and 
      :class:`rios.structures.ConcurrencyStyle` for further details.
    - Allows overlapping of read, compute, and write operations.
    - Supports a number of different parallel system configurations,
      including multiple threads within one process, compute workers
      as batch queue jobs with PBS or SLURM queues, or running on
      separate nodes in an AWS cloud configuration

  * The apply() function now returns a :class:`rios.structures.ApplierReturn`
    object, with the following attributes

    - timings. A Timers object, which allows reporting of the time spent
      in different parts of RIOS. This is very useful in tuning the best
      combination of concurrency parameters.
    - otherArgsList. This is a list of :class:`rios.structures.OtherInputs`
      objects which were given to individual compute workers, allowing them
      to be recombined in whatever way makes sense.

Disabled Features
  * The getGDALDatasetFor & getGDALBandFor methods of the ReaderInfo object
    (i.e. the first argument of the user function), gave access to the
    underlying GDAL Dataset and Band objects for each input file. However,
    these do not translate well to a multi-threaded context, since GDAL objects
    are not thread-safe. For this reason, these two methods are now disabled
    completely.

Deprecations
  * The old parallel computation facilities are no longer supported, but will
    be emulated using the new concurrency support (with a deprecation warning).
    Users should move to using the new style.
  * Many old classes for reading and writing imagery are now deprecated,
    and likely to be removed from the system in future releases. This includes
    ImageReader, ImageWriter, InputCollections, and a number of other components.
  * controls.setLoggingStream now does nothing. The old loggingstream was
    hardly used internally anyway, and is now not used at all.

Changed Behaviour
  * Most existing RIOS scripts should work as before. Deprecation warnings may
    be printed to stderr for certain situations.
  * Vector inputs are still handled as before, but if there is a reprojection
    involved, it now happens after rasterization instead of before. This means
    that polygon edges can now become curved lines in the working grid
    coordinate system. Neither the old or new approach is more correct, but
    the difference could lead to slightly different results.
  * controls.setReferenceImage will now accept either an external filename
    (the old behaviour) or an internal symbolic name (more consistent with
    everything else). The old behaviour is still perfectly valid, and will
    be kept into the future.

Bug Fixes
  * In earlier versions, if a reference pixel grid or image were given, and
    the footprint type was either INTERSECTION or UNION, the bounds of the
    reference grid were erroneously included in the intersection or union
    operation. If the reference bounds lay outside the correct footprint
    region, this would lead to an unexpected working grid and output extent.
    This was not the intended behaviour, and has now been fixed. The bounds
    of the reference are now only used in the BOUNDS_FROM_REFERENCE case.
  * Since version 1.4.1, a check was applied for GTiff format output files to
    ensure that the selected RIOS blocksize did not conflict with the blocksize
    of output files. The purpose was to avoid creating output GTiff files with
    lots of unreclaimed re-written blocks. However, this check then
    over-reached, and tried to fix the GTiff blocksize if they were
    incompatible. This was not well implemented, and the check now just
    raises an exception if an incompatibility is found.

Version 1.4.17 (2024-03-01)
---------------------------

Bug Fixes:
  * Workaround for float images that are all the same value https://github.com/ubarsc/rios/pull/72

Improvements:
  * add section about command line programs in the docs https://github.com/ubarsc/rios/pull/71
  * Improve doc for examples of controlling reading/writing https://github.com/ubarsc/rios/pull/73
  * add new controls method 'setWindowSize' which allows X and Y window sizes to be set at once https://github.com/ubarsc/rios/pull/74
  * use GDAL's type conversion functions instead https://github.com/ubarsc/rios/pull/75

Version 1.4.16 (2023-09-28)
---------------------------

Bug Fixes:
  * Add missing FlushCache call when doing statistics https://github.com/ubarsc/rios/pull/66
  * Suppress GDAL warning when running testsuite with recent GDAL. https://github.com/ubarsc/rios/pull/65

Improvements:
  * Remove old stats caching code from readerinfo and RAT writing code from imagewriter.
    These were both inherited from the original PyModeller code and are no longer
    needed. https://github.com/ubarsc/rios/pull/57 and https://github.com/ubarsc/rios/pull/58
  * Improvements to ReadTheDocs formating. https://github.com/ubarsc/rios/pull/59
    and https://github.com/ubarsc/rios/pull/60
  * Implement parallel processing with AWS Batch https://github.com/ubarsc/rios/pull/61
    and https://github.com/ubarsc/rios/pull/67
  * Remove vendored cloudpickle. This standalone package now must be present before
    using the parallel processing functionality. https://github.com/ubarsc/rios/pull/63,
    https://github.com/ubarsc/rios/pull/68 and https://github.com/ubarsc/rios/pull/69.
  * Add new entry points without extensions. This should help Windows users.
    https://github.com/ubarsc/rios/pull/64


Version 1.4.15 (2023-01-25)
---------------------------

Bug Fixes:
  * Disable the use of SetLinearBinning when writing stats & histogram to
    output HFA images. This re-enables an earlier disable mechanism
    which had been removed in version 1.4.11, and thus avoids a bug
    in GDAL's HFA driver which always sets the HFA binFunctionType to
    "direct". The GDAL bug should be fixed, but this prevents it from
    affecting RIOS. The bug only affected 16 & 32 bit athematic images,
    when displaying in ERDAS Imagine.
    https://github.com/ubarsc/rios/pull/54
  * Fix a minor problem with the final histogram bin of 16 and 32 bit
    athematic images. The last bin was being omitted. Has very little
    visible effect, as on athematic images that bin usually contains
    only a few pixels.
    https://github.com/ubarsc/rios/pull/55

Version 1.4.14 (2022-12-22)
---------------------------

Bug Fixes:
  * Numpy-1.24 release removes deprecated type symbols like numpy.bool. We
    still had some of these which needed to be updated.


Version 1.4.13 (2022-11-22)
---------------------------

Bug Fixes:
  * Use driver.Delete in test suite to ensure all temporary files deleted.
  * Ensure GDAL Exception state is maintained

Improvements:
  * Use gdal.Warp() rather than the command line gdalwarp program
  * Add CI run and support for flake8
  * Add support for 64 bit ints introduced in GDAL 3.5.0
  * Introduce VersionObj as our own version comparison class and use where needed.
  * Use setuptools for installation and update instructions to use "pip install".
  * Use GDAL gdal.GetDataTypeName() call for converting data type to a string 
    instead of our own version
  * Use gdal.ApplyGeoTransform in pix2wld and wld2pix instead of our own versions
  

Version 1.4.12 (2021-12-22)
---------------------------

Bug Fixes:
  * Remove dependency on the distutils module which is now deprecated
    in Python. Use numpy.distutils instead as we do in the other ubarsc
    projects.
  * Remove file system existence check in fileinfo.ImageInfo as this
    did not work for /vsi files.
  * Fix colortable module to work with GTiff and various other small fixes 
    and improvements.

New Features:
  * All colorbrewer2.org ramps are now available in the colortable module
    and other ramps can be added programmatically.
  * Allow the points to apply a color ramp at to specified to 
    colortable.genTable().


Version 1.4.11 (2021-02-16)
---------------------------

Bug Fixes:
  * When calculating stats and/or overviews (pyramid layers), set the 
    NoDataValue before both, and independently of them. Previously,
    it was set after calculating overviews, which meant that for continuous
    data (i.e. using averaging to calculate overviews), the overviews 
    would contain pixels contaminated by the null value. No impact on the 
    full-resolution data, but it meant that overviews were not as reliable
    as they should be. The NoDataValue can now also be set even when 
    statistics are not being calculated. 

New Features:
  * Use GDAL RFC40 attribute table methods for handling histogram
    and color tables. Much faster for very large number of entries. 
  * Removed support for GDAL versions < 2.2. 

Version 1.4.10 (2019-11-29)
---------------------------

Bug Fixes:
  * Cope with an API change in GDAL 3, in which latitude/longitude values are swapped
    in certain situations, compared with earlier GDAL versions. RIOS API does NOT change
    in this regard, the difference is handled internally. 

Version 1.4.9 (2019-11-28)
--------------------------

New Features:
  * Allow option for approximate stats calculation, to speed up on very large output files
    (thanks to Ben Jolly)

Bug Fixes:
  * Update cloudpickle inclusion, to cope with changes in Python 3.8
  * Cope with vagaries of newer OpenMPI clients

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
