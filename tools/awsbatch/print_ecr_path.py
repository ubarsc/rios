#!/usr/bin/env python3

"""
Prints the ECR Path so it can be used in a Makefile.
"""

import argparse
from rios.parallel.aws.batch import getStackOutputs


def getCmdArgs():
    p = argparse.ArgumentParser()
    p.add_argument('--base', action='store_true',
        help="Just extract the server part of the URL")

    cmdargs = p.parse_args()
    return cmdargs


def main():
    cmdargs = getCmdArgs()
    
    outputs = getStackOutputs()
    result = outputs['BatchECR']
    if cmdargs.base:
        result = result.split('/')[0]
        
    print(result)


if __name__ == '__main__':
    main()
