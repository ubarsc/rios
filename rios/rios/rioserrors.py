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
