#!/usr/bin/env python
"""
Create a distribution of RIOS, with the given version number. 

Creates a tarball with everything required, named sensibly with 
the given version number. 

Checks that there is a tag in hg for this version. 

"""
import sys
import os
import optparse
import shutil


def main():
    """
    Main routine
    """
    cmdargs = CmdArgs()
    checkTag(cmdargs)
    tmpDir = copyAll(cmdargs)
    tarAll(cmdargs, tmpDir)
    shutil.rmtree(tmpDir)


def checkTag(cmdargs):
    """
    Checks that there is a tag corresponding to the 
    given version number. Doesn't check that this is the
    version we are actually tarring, just assumes that 
    the user has done this right. 
    
    Actually, currently it does nothing. 
    
    """
    pass


def copyAll(cmdargs):
    """
    Copy everything to a temporary directory, with the 
    appropriate name 
        rios-version
    
    """
    tmpDir = "%s/rios-%s" % (cmdargs.tmpdir, cmdargs.versionnumber)
    if os.path.exists(tmpDir):
        shutil.rmtree(tmpDir)
    shutil.copytree(cmdargs.directory, tmpDir)
    return tmpDir


def tarAll(cmdargs, tmpDir):
    """
    Tar the given tmp dir into a tarball
    """
    cwd = os.getcwd()
    tarfile = "%s/rios-%s.tar.gz" % (cwd, cmdargs.versionnumber)
    
    newcwd = os.path.dirname(tmpDir)
    cmd = "cd %s; tar cfz %s %s" % (newcwd, tarfile, tmpDir)
    os.system(cmd)

class CmdArgs:
    def __init__(self):
        p = optparse.OptionParser()
        p.add_option("-v", "--versionnumber", dest="versionnumber", 
            help="Version number string")
        p.add_option("-d", "--directory", dest="directory", default="./rios", 
            help="Directory underneath which to find rios (default=%default)")
        p.add_option("--tmpdir", dest="tmpdir", default=".",
            help="Temporary working directory")
        (options, args) = p.parse_args()
        self.__dict__.update(options.__dict__)
        
        if self.versionnumber is None:
            p.print_help()
            sys.exit()


if __name__ == "__main__":
    main()
