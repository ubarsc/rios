#!/usr/bin/env python
"""
Main test harness for RIOS. 

Should be run as a main program. It then runs a selection 
of tests of some capabilities of RIOS. 

"""
import riostestutils

import testavg
testavg.run()

import testresample
testresample.run()

import testcolortable
testcolortable.run()

import testvector
testvector.run()



# After all tests
riostestutils.report("ALL TESTS", "Completed")
