#!/usr/bin/env python
"""
All exceptions used within rios. 

"""

class RiosError(Exception): pass

# 
class ImageOpenError(RiosError):
    "Image wasn't able to be opened by GDAL"
    
class ParameterError(RiosError):
    "Incorrect parameters passed to function"

class ResampleNeededError(RiosError):
    "Images do not match - resample needs to be turned on"
    
class OutsideImageBoundsError(RiosError):
    "Requested Block is not available"
    
class GdalWarpNotFoundError(RiosError):
    "Unable to run gdalwarp"
    
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
