#!/usr/bin/env python3

"""
Helper script to create the RIOS Batch CloudFormation. 
Optionally wait until stack is created before returning.
"""

import time
import argparse
import boto3

N_AZS = 3

def getCmdArgs():
    """
    Get Command Line Args
    """
    p = argparse.ArgumentParser()
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
    if cmdargs.az is not None and len(az) != 3:
        raise SystemExit("--az must be specified {3} times".format(N_AZS))
        
    return cmdargs
    
    
def main():
    cmdargs = getCmdArgs()
    
    stackId, status = createBatch(cmdargs.az, cmdargs.ecrname, cmdargs.vcpus, 
        cmdargs.mem, cmdargs.maxjobs, cmdargs.wait):
            
    print('stackId: {}'.format(stackId)})
    if status is not None:
        print('status: {}'.format(status))


def addParam(params, key, value):
    params.append({'ParameterKey': key,
        'ParameterValue': value})

    
def createBatch(azs, ecrName, vCPUs, maxMem, maxJobs, wait):
    
    params = []
    if azs is not None:
        addParam(params, 'AZ_1', azs[0])
        addParam(params, 'AZ_2', azs[1])
        addParam(params, 'AZ_3', azs[2])
        
    if cmdargs.ecrname is not None:
        addParam(params, 'ECR_Name', ecrName)
        
    if cmdargs.vcpus is not None:
        addParam(params, 'VCPUS', vCPUs)
        
    if cmdargs.mem is not None:
        addParam(params, 'MaxMemory', maxMem)
        
    if cmdargs.maxjobs is not None:
        addParam(params, 'MaxJobs', maxJobs)
    
    client = boto3.client('cloutformation')
    resp = client.create_stack(StackName='RIOS',
        TemplateURL='file://batch.yaml',
        Parameters=[params])
        
    stackId = resp['StackId']
    print('StackId: {}'.format(stackId))
    
    status = None
    if wait:
        while True:
            resp = client.descripe_stacks(StackName=stackId)
            status = resp['Stacks'][0]['StackStatus']
            if status != 'CREATE_IN_PROGRESS':
                break
            time.sleep(30)
                
    return stackId, status
    

if __name__ == '__main__':
    main()
    