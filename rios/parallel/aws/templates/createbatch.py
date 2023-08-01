#!/usr/bin/env python3

"""
Helper script to create the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.
"""

import time
import argparse
import boto3

N_AZS = 3
DFLT_STACK_NAME = 'RIOS'
DFLT_REGION = 'ap-southeast-2'


def getCmdArgs():
    """
    Get Command Line Args
    """
    p = argparse.ArgumentParser()
    p.add_argument('--stackname', default=DFLT_STACK_NAME,
        help="Name of CloudFormation Stack to create. (default=%(default)s)")
    p.add_argument('--region', default=DFLT_REGION,
        help="AWS Region to use. (default=%(default)s)")
    p.add_argument('--az', action='append',
        help="Availability zones to use. Specify {} times".format(N_AZS))
    p.add_argument('--ecrname',
        help="Name of ECR Repository to create")
    p.add_argument('--vcpus', type=int,
        help="Number of CPUs that each job will require")
    p.add_argument('--mem', type=int,
        help="Amount of Kb that each job will require")
    p.add_argument('--maxjobs', type=int,
        help="Maximum number of jobs to run at once. " + 
            "Ideally the same as controls.setNumThreads()")
    p.add_argument('--wait', action="store_true",
        help="Wait until CloudFormation is complete before exiting")
        
    cmdargs = p.parse_args()
    if cmdargs.az is not None and len(cmdargs.az) != 3:
        raise SystemExit("--az must be specified {} times".format(N_AZS))
        
    return cmdargs
    
    
def main():
    """
    Main function for this script
    """
    cmdargs = getCmdArgs()
    
    stackId, status = createBatch(cmdargs.stackname, cmdargs.region,
        cmdargs.az, cmdargs.ecrname, cmdargs.vcpus, 
        cmdargs.mem, cmdargs.maxjobs, cmdargs.wait)
            
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

    
def createBatch(stackname, region, azs, ecrName, vCPUs, maxMem, maxJobs, wait):
    """
    Do the work of creating the CloudFormation Stack
    """        
    
    params = []
    if azs is not None:
        addParam(params, 'AZ1', azs[0])
        addParam(params, 'AZ2', azs[1])
        addParam(params, 'AZ3', azs[2])
        
    if ecrName is not None:
        addParam(params, 'ECR_Name', ecrName)
        
    if vCPUs is not None:
        addParam(params, 'VCPUS', vCPUs)
        
    if maxMem is not None:
        addParam(params, 'MaxMemory', maxMem)
        
    if maxJobs is not None:
        addParam(params, 'MaxJobs', maxJobs)
        
    body = open('batch.yaml').read()
        
    client = boto3.client('cloudformation', region_name=region)
    resp = client.create_stack(StackName=stackname,
        TemplateBody=body, Capabilities=['CAPABILITY_IAM'], 
        Parameters=params)
        
    stackId = resp['StackId']
    print('StackId: {}'.format(stackId))
    
    status = None
    if wait:
        while True:
            time.sleep(30)
            resp = client.describe_stacks(StackName=stackId)
            status = resp['Stacks'][0]['StackStatus']
            if status != 'CREATE_IN_PROGRESS':
                break
            print('Status: {}'.format(status))
                
    return stackId, status
    

if __name__ == '__main__':
    main()
