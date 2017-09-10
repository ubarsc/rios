"""
Sub-module for command line entry points

We have these functions that allow us to create 
a command line program easily. They are turned into 
entry points by Conda.

We use entry points wherever possible since Conda on Windows creates
a .exe file that calls the correct Python etc. Doing this isn't
hard for Unix operating systems since they understand the shebang
mechanism, but having good Windows support we think is worth 
the extra bother.
"""
