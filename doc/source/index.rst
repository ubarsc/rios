.. _contents:

Raster I/O Simplification
========================================================

Introduction
------------
A set of Python modules which makes it easy to write raster processing 
code in Python. Built on top of GDAL, it handles the details of 
opening and closing files, checking alignment of projection and 
raster grid, stepping through the raster in small blocks, etc., 
allowing the programmer to concentrate on the processing involved. 
It is licensed under GPL 3.

Example
-------

::

    """
    Reads in two input files and adds them together. 
    Assumes that they have the same number of bands. 
    """
    from rios import applier
 
    # Set up input and output filenames. 
    infiles = applier.FilenameAssociations()
    infiles.image1 = "file1.img"
    infiles.image2 = "file2.img"
 
    outfiles = applier.FilenameAssociations()
    outfiles.outimage = "outfile.img"
 
    # Set up the function to be applied
    def addThem(info, inputs, outputs):
        """
        Function to be called by rios.
        Adds image1 and image2 from the inputs, and
        places the result in the outputs as outimage. 
        """
        outputs.outimage = inputs.image1 + inputs.image2
 
    # Apply the function to the inputs, creating the outputs. 
    applier.apply(addThem, infiles, outfiles)

See :doc:`applierexamples` for more information.


Downloads
---------
From `BitBucket <https://bitbucket.org/chchrsc/rios/downloads>`_. 
Release notes by version can be viewed in :doc:`releasenotes`.

`Conda <http://conda.pydata.org/miniconda.html#miniconda>`_ packages are available under the 'conda-forge' channel.
Once you have installed `Conda <http://conda.pydata.org/miniconda.html#miniconda>`_, run the following commands on the command line to install rios: ::

    conda config --add channels conda-forge 
    conda create -n myenv rios
    conda activate myenv

High level functions
---------------------

.. toctree::
    :maxdepth: 1

    Processing Raster and Vector files with rios.applier <rios_applier>
    Processing Raster Attribute Tables with rios.ratapplier <rios_ratapplier>
    Obtaining information on files with rios.fileinfo <rios_fileinfo>
    RIOS Environment Variables <environmentvars>

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

    rios_cuiprogress
    rios_imageio
    rios_inputcollection
    rios_pixelgrid
    rios_rioserrors
    rios_riostests
    rios_vectorreader
    rios_cmdline

* :ref:`modindex`
* :ref:`search`

.. codeauthor:: Sam Gillingham & Neil Flood
