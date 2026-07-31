"""
This module holds a number of constant values which should be available
to the rest of the package. This file should not import any other
part of RIOS. 
"""
# This file is part of RIOS - Raster I/O Simplification
# Copyright (C) 2012  Sam Gillingham, Neil Flood
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import os


# Definitions for footprint types. These were originally in imageio.py
INTERSECTION = 0
UNION = 1
BOUNDS_FROM_REFERENCE = 2       # Bounds of working region are taken from given reference grid


# Per-driver dictionary of default creation options. We pre-load this with
# options for a few drivers we think are important (to us), but more can be
# loaded from environment variables $RIOS_DFLT_CREOPT_<drivername>
dfltDriverOptions = {
    'HFA': ['COMPRESSED=YES', 'IGNOREUTM=YES'],
    'GTiff': ['TILED=YES', 'COMPRESS=DEFLATE', 'INTERLEAVE=BAND',
              'BIGTIFF=IF_SAFER'],
    'Zarr': ['FORMAT=ZARR_V3', 'COMPRESS=BLOSC']
}

# N.B. For the default HFA creation options.
# The IGNOREUTM=YES is there to switch off a minor kludge in GDAL's
# HFA driver. By default, it will check any Transverse Mercator
# projection, and if its parameters match a standard UTM zone, it
# re-states the projection as literal UTM. This was originally because
# Imagine was not good at matching equivalent projections. This is no
# longer true, and we choose to disable that behaviour by default.


def setDefaultDriver():
    """
    Sets some default values into global variables, defining
    what defaults we should use for GDAL driver. On any given
    output file these can be over-ridden, and can be over-ridden globally
    using the environment variables

        * $RIOS_DFLT_DRIVER
        * $RIOS_DFLT_DRIVEROPTIONS (deprecated)
        * $RIOS_DFLT_CREOPT_<drivername>
    
    If RIOS_DFLT_DRIVER is set, then it should be a gdal short driver name. 
    If RIOS_DFLT_DRIVEROPTIONS is set, it should be a space-separated list
    of driver creation options, e.g. "COMPRESS=LZW TILED=YES", and should
    be appropriate for the selected GDAL driver. This can also be 'None'
    in which case an empty list of creation options is passed to the driver.
    
    The same rules apply to the driver-specific creation options given
    using $RIOS_DFLT_CREOPT_<driver>. These options are a later paradigm, and 
    are intended to supercede the previous generic driver defaults. 
    
    If not otherwise supplied, the default is to use the HFA driver, with compression. 

    """
    # The old behaviour just had a single pair of environment variables for
    # default driver and creation options. If these are set, load them as python
    # symbols, and store in the per-driver dictionary
    global DEFAULTDRIVERNAME, DEFAULTCREATIONOPTIONS
    DEFAULTDRIVERNAME = os.getenv('RIOS_DFLT_DRIVER', default='HFA')
    creationOptionsStr = os.getenv('RIOS_DFLT_DRIVEROPTIONS')
    if creationOptionsStr is not None:
        DEFAULTCREATIONOPTIONS = creationOptionsStr.split()
        dfltDriverOptions[DEFAULTDRIVERNAME] = DEFAULTCREATIONOPTIONS
    else:
        DEFAULTCREATIONOPTIONS = []

    # Now load any driver-specific creation options which are specified by
    # environment variables, of the form RIOS_DFLT_CREOPT_<drivername>
    driverOptVarPrefix = 'RIOS_DFLT_CREOPT_'
    for varname in os.environ:
        if varname.startswith(driverOptVarPrefix):
            drvrName = varname[len(driverOptVarPrefix):]
            optionsStr = os.getenv(varname)
            dfltDriverOptions[drvrName] = optionsStr.split()


setDefaultDriver()
