#!/usr/bin/env python3

"""
Helper script to delete the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.
"""

import time
import argparse
import boto3
import botocore

from ..jobmanager import DFLT_STACK_NAME
from ..jobmanager import DFLT_REGION


def getCmdArgs():
    """
    Get Command Line Args
    """
    p = argparse.ArgumentParser()
    p.add_argument('--stackname', default=DFLT_STACK_NAME,
        help="Name of CloudFormation Stack to delete. (default=%(default)s)")
    p.add_argument('--region', default=DFLT_REGION,
        help="AWS Region to use. (default=%(default)s)")
    p.add_argument('--wait', action="store_true",
        help="Wait until CloudFormation is complete before exiting")
        
    cmdargs = p.parse_args()
        
    return cmdargs


def main():
    """
    Main function for this script
    """
    cmdargs = getCmdArgs()

    client = boto3.client('cloudformation', region_name=cmdargs.region)
    client.delete_stack(StackName=cmdargs.stackname)
    
    if cmdargs.wait:
        while True:
            time.sleep(30)
            try:
                client.describe_stacks(StackName=cmdargs.stackname)
            except botocore.exceptions.ClientError:
                break


if __name__ == '__main__':
    main()
