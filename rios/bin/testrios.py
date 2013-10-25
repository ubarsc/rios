#!/usr/bin/env python
"""
Main test harness for RIOS. 

Should be run as a main program. It then runs a selection 
of tests of some capabilities of RIOS. 

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
import riostestutils

failureCount = 0

import testavg
ok = testavg.run()
if not ok: failureCount += 1

import testresample
ok = testresample.run()
if not ok: failureCount += 1

import testcolortable
ok = testcolortable.run()
if not ok: failureCount += 1

import testvector
ok = testvector.run()
if not ok: failureCount += 1

import testcoords
ok = testcoords.run()
if not ok: failureCount += 1

# After all tests
riostestutils.report("ALL TESTS", "Completed, with %d failure(s)" % failureCount)
