# RIOS - Raster I/O Simplification
A set of Python modules which makes it easy to write raster processing code in Python. It is built on top of [GDAL](https://www.gdal.org).

Raster data is read from input files and presented to the user as numpy arrays for processing. Output raster data is taken as numpy arrays and written to output files. The user provides a Python function to perform the desired processing with these numpy arrays.

These [simple examples](https://www.rioshome.org/en/latest/applierexamples.html) demonstrate the main ideas.

## Features
- Handles the details of opening/closing raster files and reading/writing of raster data.
- Checks agreement of projection and raster grid alignment and extent across the set of input files, reprojecting and/or resampling automatically as required
- Steps through the raster in small blocks to reduce memory requirements, allowing processing of very large rasters
- Raster data is presented to the user as numpy arrays, passed to a user-supplied function which performs all processing computation
- Strong, flexible support for concurrency in reading/computation/writing for efficient use of available hardware (e.g. multiple CPU cores, AWS Fargate/ECS, and others).
- *and more...*

This allows the programmer to concentrate on the processing involved.

RIOS comes without any warranty or assurance that it might be useful. Anyone doing raster processing is welcome to use it and build on it.

## Download/Install
- For source download, click on the [Releases](https://github.com/ubarsc/rios/releases) link and select the latest tarball
- Direct install is available with conda (from conda-forge), and with the Spack package manager

See [Downloads](https://www.rioshome.org/en/latest/#downloads) for full details

## Documentation
Full Sphinx documentation is available at [www.rioshome.org](https://www.rioshome.org/)

## License
GPL 3, *see LICENSE.txt* 
