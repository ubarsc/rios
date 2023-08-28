#!/usr/bin/env python
"""
Use rios.fileinfo to print the statistics for the given image(s). 

"""

from rios.cmdline import riosprintstats
import warnings
warnings.warn("Future versions of RIOS may remove the .py extension from this script name", DeprecationWarning)

riosprintstats.main()
