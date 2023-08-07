#!/usr/bin/env python3

"""
Helper script to delete the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.
"""

import time
import argparse
import boto3
import botocore

from rios.parallel.aws.batch import DFLT_STACK_NAME
from rios.parallel.aws.batch import DFLT_REGION
from rios.parallel.aws.batch import getStackOutputs


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
        help="Wait until CloudFormation is deleted before exiting")
        
    cmdargs = p.parse_args()
        
    return cmdargs


def deleteAllS3Files(outputs):
    """
    See https://stackoverflow.com/questions/43326493/what-is-the-fastest-way-to-empty-s3-bucket-using-boto3

    Needed before you can delete the bucket
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(outputs['BatchBucket'])
    bucket.objects.all().delete()


def deleteAllECRImages(outputs):
    """
    See https://stackoverflow.com/questions/58843927/boto3-script-to-delete-all-images-which-are-untagged

    Needed before you can delete the repo
    """
    client = boto3.client('ecr')
    response = client.list_images(repositoryName='riosecr')
    imageList = [image for image in response['imageIds']]
    client.batch_delete_image(repositoryName='riosecr', imageIds=imageList)


def main():
    """
    Main function for this script
    """
    cmdargs = getCmdArgs()

    outputs = getStackOutputs(cmdargs.stackname, cmdargs.region)

    deleteAllS3Files(outputs)
    deleteAllECRImages(outputs)

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
