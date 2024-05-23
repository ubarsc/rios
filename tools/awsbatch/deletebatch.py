#!/usr/bin/env python3

"""
Helper script to delete the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.
"""

import time
import argparse
import boto3
import botocore

from rios.parallel.aws.batch import STACK_NAME
from rios.parallel.aws.batch import REGION
from rios.parallel.aws.batch import getStackOutputs


def getCmdArgs():
    """
    Get Command Line Args
    """
    p = argparse.ArgumentParser()
    p.add_argument('--wait', action="store_true",
        help="Wait until CloudFormation is deleted before exiting")
        
    cmdargs = p.parse_args()
        
    return cmdargs


def deleteAllECRImages(outputs):
    """
    See https://stackoverflow.com/questions/58843927/boto3-script-to-delete-all-images-which-are-untagged

    Needed before you can delete the repos
    """
    client = boto3.client('ecr', region_name=REGION)
    for name in ['riosecr', 'riosecrmain']:
        try:
            response = client.list_images(repositoryName=name)
            imageList = [image for image in response['imageIds']]
            if len(imageList) > 0:
                client.batch_delete_image(repositoryName=name, 
                        imageIds=imageList)
        except client.exceptions.RepositoryNotFoundException:
            pass


def main():
    """
    Main function for this script
    """
    cmdargs = getCmdArgs()

    outputs = getStackOutputs()

    deleteAllECRImages(outputs)

    client = boto3.client('cloudformation', region_name=REGION)
    client.delete_stack(StackName=STACK_NAME)
    
    if cmdargs.wait:
        while True:
            time.sleep(30)
            try:
                client.describe_stacks(StackName=STACK_NAME)
            except botocore.exceptions.ClientError:
                break


if __name__ == '__main__':
    main()
