.. _contents:

Raster I/O Simplification (RIOS)
========================================================

Introduction
------------
A set of Python modules which makes it easy to write raster processing 
code in Python. Built on top of GDAL, it handles the details of 
opening and closing files, checking alignment of projection and 
raster grid, stepping through the raster in small blocks, etc., 
allowing the programmer to concentrate on the processing involved.

As of version 2.0, RIOS has strong support for parallel processing,
in both reading and computation, supporting a range of paradigms.
See :doc:`concurrency` for details.

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
Source code is available from
`GitHub <https://github.com/ubarsc/rios/releases>`_. Installation from source
is described in the INSTALL.txt file.

Release notes by version can be viewed in :doc:`releasenotes`.

Conda packages are available under the ``conda-forge`` channel. Once you
have installed
`Conda's installer <http://conda.pydata.org/miniconda.html#miniconda>`_,
run the following commands on the command line to install RIOS::

    conda config --add channels conda-forge 
    conda create -n myenv rios
    conda activate myenv

RIOS is also available with the 
`Spack package manager <https://spack.readthedocs.io/en/latest/>`_::

    spack install py-rios

RIOS is *not* available from the PyPI repository. This is because it depends
on the GDAL library, which is also not available there, and must be installed
by some other means, such as conda. If one is using conda for GDAL, one
may as well use it for RIOS, too. While it is technically possible to bundle
the GDAL binaries into a PyPI distribution, this carries grave risks of version
conflicts if any other package does the same thing, and is best avoided.

High level functions
---------------------

.. toctree::
    :maxdepth: 1

    RIOS Basic Examples <applierexamples>
    Processing Raster and Vector files with rios.applier <rios_applier>
    Understanding RIOS's concurrency model <concurrency>
    Processing Raster Attribute Tables with rios.ratapplier <rios_ratapplier>
    Obtaining information on files with rios.fileinfo <rios_fileinfo>
    RIOS Environment Variables <environmentvars>

Low level functions
--------------------
.. toctree::
    :maxdepth: 1

    rios_imagereader
    rios_imagewriter
    rios_computemanager
    rios_structures
    rios_rat
    rios_colortable
    rios_readerinfo

Deprecated (see version 2.0 notes)
----------------------------------
.. toctree::
    :maxdepth: 1

    rios_parallel
    rios_parallel_aws_batch
    

Command Line Programs
---------------------

RIOS comes with two command line programs:
  - `rioscalcstats` computes statistics and pyramid layers (overviews) on a file. 
  - `riosprintstats` prints statistics information previously calculated on the file. 
  
Refer to the helpstrings (run with `-h`) for usage of these programs. Note that 
by default, RIOS calculates pyramid layers and statistics on a file.


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
