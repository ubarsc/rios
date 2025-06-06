"""
All exceptions used within rios. 

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

import sys
import inspect


class RiosError(Exception):
    """
    Base class for RIOS exceptions
    """


class FileOpenError(RiosError):
    "Failed to open an input or output file"


class ImageOpenError(FileOpenError):
    "Image wasn't able to be opened by GDAL"


class ParameterError(RiosError):
    "Incorrect parameters passed to function"


class GDALLayerNumberError(RiosError):
    "A GDAL layer number was given, but was out of range"


class ResampleNeededError(RiosError):
    "Images do not match - resample needs to be turned on"


class OutsideImageBoundsError(RiosError):
    "Requested Block is not available"


class GdalWarpError(RiosError):
    "Error while running gdalwarp"


class ThematicError(RiosError):
    "File unable to be set to thematic"


class ProcessCancelledError(RiosError):
    "Process was cancelled by user"


class KeysMismatch(RiosError):
    "Keys do not match expected"


class MismatchedListLengthsError(RiosError):
    "Two lists had different lengths, when they were supposed to be the same length"


class AttributeTableColumnError(RiosError):
    "Unable to find specified column"


class AttributeTableTypeError(RiosError):
    "Type does not match that expected"


class ArrayShapeError(RiosError):
    "Error in shape of an array"


class TypeConversionError(RiosError):
    "Unknown type conversion"


class VectorAttributeError(RiosError):
    "Unable to find specified index in vector file"


class VectorGeometryTypeError(RiosError):
    "Unexpected Geometry type"


class VectorProjectionError(RiosError):
    "Vector projection does not match raster projection"


class VectorRasterizationError(RiosError):
    "Rasterisation of Vector dataset failed"


class VectorLayerError(RiosError):
    "Unable to find the specified layer"


class WrongControlsObject(RiosError):
    "The wrong type of control object has been passed to apply"


class RatBlockLengthError(RiosError):
    "Error with RAT block length, in ratapplier"


class RatMismatchError(RiosError):
    "Inconsistent RATs on inputs to ratapplier"


class IntersectionError(RiosError):
    "Images don't have a common area"


class JobMgrError(RiosError):
    "Errors from Jobmanager class"


class ColorTableGenerationError(RiosError):
    "Error generating a color table"


class PermissionError(RiosError):
    "Error due to permissions on temp files"


class TimeoutError(RiosError):
    "Something timed out"


class UnavailableError(RiosError):
    "A dependency is unavailable"


class WorkerExceptionError(RiosError):
    "A worker thread or process has raised an exception"


class SinglePassActionsError(RiosError):
    "An error in processing single-pass actions"


class ECSError(RiosError):
    "Error arising from AWS ECS"


deprecationAlreadyWarned = set()


def deprecationWarning(msg, stacklevel=2):
    """
    Print a deprecation warning to stderr. Includes the filename
    and line number of the call to the function which called this.
    The stacklevel argument controls how many stack levels above this
    gives the line number.

    Implemented in mimcry of warnings.warn(), which seems very flaky.
    Sometimes it prints, and sometimes not, unless PYTHONWARNINGS is set
    (or -W is used). This function at least seems to work consistently.

    """
    frame = inspect.currentframe()
    for i in range(stacklevel):
        if frame is not None:
            frame = frame.f_back

    if frame is None:
        filename = "sys"
        lineno = 1
    else:
        filename = frame.f_code.co_filename
        lineno = frame.f_lineno

    key = (filename, lineno)
    if key not in deprecationAlreadyWarned:
        print("{} (line {}):\n    WARNING: {}".format(filename, lineno, msg),
            file=sys.stderr)
        deprecationAlreadyWarned.add(key)
