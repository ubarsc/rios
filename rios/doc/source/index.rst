.. _contents:

Raster I/O Simplification
========================================================
A set of Python modules which makes it easy to write raster processing code in Python. Built on top of GDAL, it handles the details of opening and closing files, checking alignment of projection and raster grid, stepping through the raster in small blocks, etc., allowing the programmer to concentrate on the processing involved. RIOS was written for our own use, and comes without any warranty, or assurance that it might even be useful, but if anyone else is silly enough to want it, they are welcome to it. It is licensed under GPL 3.

For more information, and to download RIOS see: https://bitbucket.org/chchrsc/rios/

High level functions
---------------------

.. toctree::
    :maxdepth: 1

    rios_applier
    rios_vectorreader
    rios_ratapplier


Low level functions
--------------------
.. toctree::
    :maxdepth: 1

    rios_imagereader
    rios_imagewriter
    rios_parallel
    rios_rat
    rios_readerinfo

Utilities
--------------------
.. toctree::
    :maxdepth: 1

    rios_calcstats

Internal
--------------------
.. toctree::
    :maxdepth: 1

    rios_fileinfo
    rios_imageio
    rios_inputcollection
    rios_pixelgrid
    rios_rioserrors
    rios_riostests
    rios_cuiprogress

* :ref:`modindex`
* :ref:`search`

.. codeauthor:: Sam Gillingham & Neil Flood
