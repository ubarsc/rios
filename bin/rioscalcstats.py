#!/usr/bin/env python
"""
Use rios.calcstats to calculate statistics for the given image(s). 

"""

from rios.cmdline import rioscalcstats
import warnings
warnings.warn("Future versions of RIOS may remove the .py extension from this script name", DeprecationWarning)

rioscalcstats.main()
