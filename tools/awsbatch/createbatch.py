#!/usr/bin/env python3

"""
Helper script to create the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.

The --modify command line argument allows modification of 
an existing stack.

All the other parameters for the CloudFormation stack can
be altered with the command line params.
"""

import time
import argparse
import boto3

from rios.parallel.aws.batch import STACK_NAME
from rios.parallel.aws.batch import REGION


def getCmdArgs():
    """
    Get Command Line Args
    """
    p = argparse.ArgumentParser()
    p.add_argument('--stackname', default=STACK_NAME,
        help="Name of CloudFormation Stack to create. (default=%(default)s)")
    p.add_argument('--region', default=REGION,
        help="AWS Region to use. (default=%(default)s)")
    p.add_argument('--ecrname',
        help="Name of ECR Repository to create")
    p.add_argument('--vcpus', type=int,
        help="Number of CPUs that each job will require")
    p.add_argument('--mem', type=int,
        help="Amount of Kb that each job will require")
    p.add_argument('--maxvcpus', type=int,
        help="Maximum number of jobs to run at once. " + 
            "Ideally the same as controls.setNumThreads(). " +
            "May need to be adjusted if --vcpus > 1.")
    p.add_argument('--instancetype', 
        help="Override the instance type for the jobs. This " +
            "needs to be the same architecture as the Docker images")
    p.add_argument('--mainvolumesize', type=int, 
        help="Override the main job volume size in GB")
    p.add_argument('--wait', action="store_true",
        help="Wait until CloudFormation is complete before exiting")
    p.add_argument('--modify', action="store_true",
        help="Instead of creating the stack, modify it.")
    p.add_argument('--tag', default='RIOS', 
        help="Tag to use when creating resources. (default=%(default)s)")
        
    cmdargs = p.parse_args()
        
    return cmdargs
    
    
def main():
    """
    Main function for this script
    """
    cmdargs = getCmdArgs()
    
    stackId, status = createBatch(cmdargs.stackname, cmdargs.region,
        cmdargs.ecrname, cmdargs.vcpus, cmdargs.mem, cmdargs.maxvcpus, 
        cmdargs.instancetype, cmdargs.mainvolumesize, cmdargs.modify, 
        cmdargs.wait, cmdargs.tag)
            
    print('stackId: {}'.format(stackId))
    if status is not None:
        print('status: {}'.format(status))


def addParam(params, key, value):
    """
    Helper function for adding keys/values in the format
    boto3 expects.
    """
    params.append({'ParameterKey': key,
        'ParameterValue': value})

    
def createBatch(stackname, region, ecrName, vCPUs, maxMem, maxvCPUs, 
        instanceType, mainVolumeSize, modify, wait, tag):
    """
    Do the work of creating the CloudFormation Stack
    """        
    
    # set overridden stack parameters
    params = []
        
    if ecrName is not None:
        addParam(params, 'ECR_Name', ecrName)
        
    if vCPUs is not None:
        addParam(params, 'VCPUS', str(vCPUs))
        
    if maxMem is not None:
        addParam(params, 'MaxMemory', str(maxMem))
        
    if maxvCPUs is not None:
        addParam(params, 'MaxVCPUS', str(maxvCPUs))

    if instanceType is not None:
        addParam(params, 'InstanceType', instanceType)

    if mainVolumeSize is not None:
        addParam(params, 'MainVolumeSize', str(mainVolumeSize))
        
    body = open('batch.yaml').read()
        
    client = boto3.client('cloudformation', region_name=region)

    if modify:
        # modify stack
        resp = client.update_stack(StackName=stackname,
            TemplateBody=body, Capabilities=['CAPABILITY_IAM'], 
            Parameters=params, Tags=[{'Key': tag, 'Value': '1'}])
        inProgressStatus = 'UPDATE_IN_PROGRESS'

    else:
        # create stack
        resp = client.create_stack(StackName=stackname,
            TemplateBody=body, Capabilities=['CAPABILITY_IAM'], 
            Parameters=params, Tags=[{'Key': tag, 'Value': '1'}])
        inProgressStatus = 'CREATE_IN_PROGRESS'
        
    stackId = resp['StackId']
    print('StackId: {}'.format(stackId))
    
    status = None
    # If they asked to wait, loop calling describe_stacks
    if wait:
        while True:
            time.sleep(30)
            resp = client.describe_stacks(StackName=stackId)
            status = resp['Stacks'][0]['StackStatus']
            if status != inProgressStatus:
                break
            print('Status: {}'.format(status))
                
    return stackId, status
    

if __name__ == '__main__':
    main()
