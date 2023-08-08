"""
Module for implementing parallel with AWS Batch. 

See the class AWSBatch for the implemenation.
"""

import io
import pickle
import boto3

from .. import jobmanager

# Import a pickler which can pickle functions, with their dependencies, as well
# as data. Either use the version installed with cloudpickle
# (https://github.com/cloudpipe/cloudpickle) or the bundled versin
try:
    from cloudpickle import cloudpickle
except ImportError:
    # Import from our own local copy. This is what will usually happen. 
    from .. import cloudpickle

DFLT_STACK_NAME = 'RIOS'
DFLT_REGION = 'ap-southeast-2'


class AWSBatchException(Exception):
    pass


def getStackOutputs(stackName=DFLT_STACK_NAME, region=DFLT_REGION):
    """
    Helper function to query the CloudFormation stack for outputs
    """
    client = boto3.client('cloudformation', region_name=region)
    resp = client.describe_stacks(StackName=stackName)
    if len(resp['Stacks']) == 0:
        raise AWSBatchException("Stack not created")
    outputsRaw = resp['Stacks'][0]['Outputs']
    # convert to a normal dictionary
    outputs = {}
    for out in outputsRaw:
        key = out['OutputKey']
        value = out['OutputValue']
        outputs[key] = value
    return outputs


class AWSBatch(jobmanager.JobManager):
    """
    Implementation of parallelisation via AWS Batch.
    This uses 2 SQS queues for communication between the
    'main' RIOS script and the subprocesses (which run on Batch)
    and an S3 bucket to hold the pickled data (which the SQS
    messages refer to).
    """
    jobMgrType = 'AWSBatch'

    def __init__(self, numSubJobs, stackName=DFLT_STACK_NAME, 
            region=DFLT_REGION):
        super().__init__(numSubJobs)
        # get the output of the CloudFormation so we know what 
        # the resources are called.
        self.stackOutputs = getStackOutputs(stackName, region)
        self.batchClient = boto3.client('batch', region_name=region)
        self.s3Client = boto3.client('s3', region_name=region)
        self.sqsClient = boto3.client('sqs', region_name=region)

        # check they haven't asked for more jobs than we have batch instances
        # minus one as one job is always done in this thread
        if numSubJobs - 1 > int(self.stackOutputs['BatchMaxJobs']):
            print('Number of threads greater than number of MaxJobs input to ' +
                'CloudFormation. Consider increasing this number.')

        # start the required number of batch jobs running now
        # minus one as one job is always done in this thread
        for n in range(numSubJobs - 1):
            self.batchClient.submit_job(jobName='RIOS_{}'.format(n),
                jobQueue=self.stackOutputs['BatchProcessingJobQueueName'],
                jobDefinition=self.stackOutputs['BatchProcessingJobDefinitionName'])
        
    def startOneJob(self, userFunc, jobInfo):
        """
        Start one sub job
        """
        jobInfo = jobInfo.prepareForPickling()

        allInputs = (userFunc, jobInfo)
        allInputsPickled = cloudpickle.dumps(allInputs)
        # save pickled data in file like BytesIO
        fileObj = io.BytesIO(allInputsPickled)
        
        # create a unique filename based on the coords of the
        # current block.
        s3Key = 'block_{}_{}_in.pkl'.format(jobInfo.info.xblock, jobInfo.info.yblock)
        
        # upload this file to S3
        self.s3Client.upload_fileobj(fileObj, 
            self.stackOutputs['BatchBucket'], s3Key)
            
        # send the filename as a message to the Batch workers
        self.sqsClient.send_message(QueueUrl=self.stackOutputs['BatchInQueue'],
            MessageBody=s3Key)

        # return the block coords so we can look for this message later
        return (jobInfo.info.xblock, jobInfo.info.yblock)
            
    def waitOnJobs(self, jobIDlist):
        """
        Wait on all the jobs. Do nothing.
        """
        pass
        
    def gatherAllOutputs(self, jobIDlist):
        """
        Gather all the results. Checks the output SQS Queue
        """
        # first one that is done in this thread
        outputBlocksList = [jobIDlist[0]]
        # convert to a set so we can easily search for which blocks 
        # out 'ours'. They should all be, but I'm just being paranoid
        inBlocks = set()
        for xblock, yblock in jobIDlist[1:]:
            inBlocks.add((xblock, yblock))
        outputBlocksDict = {}
        
        # look for all the blocks
        while len(outputBlocksDict) < len(jobIDlist) - 1:
            resp = self.sqsClient.receive_message(
                QueueUrl=self.stackOutputs['BatchOutQueue'],
                WaitTimeSeconds=20)  # 20 appears to be the max

            if 'Messages' not in resp:
                continue                
            for msg in resp['Messages']:
                body = msg['Body']
                receiptHandle = msg['ReceiptHandle']
                # get the info out of the filename
                bl, x, y, o = body.split('_')
                x = int(x)
                y = int(y)
                # one of ours?
                if (x, y) in inBlocks:
                    # delete it so we don't see it again
                    self.sqsClient.delete_message(
                        QueueUrl=self.stackOutputs['BatchOutQueue'], 
                        ReceiptHandle=receiptHandle)
                        
                    # download
                    pickledOutput = io.BytesIO()
                    self.s3Client.download_fileobj(
                        self.stackOutputs['BatchBucket'], body, pickledOutput)
                    pickledOutput.seek(0)
                    outputObj = pickle.load(pickledOutput)
                    # save
                    outputBlocksDict[(x, y)] = outputObj
                    
                    # delete pkl
                    self.s3Client.delete_object(
                        Bucket=self.stackOutputs['BatchBucket'], Key=body)

        # now convert dict back to list so in same order requested
        for xblock, yblock in jobIDlist[1:]:
            obj = outputBlocksDict[(xblock, yblock)]
            outputBlocksList.append(obj)

        return outputBlocksList        
        
    def finalise(self):
        """
        Stop our AWS Batch jobs by sending a special message to the queue
        """
        for n in range(self.numSubJobs):
            self.sqsClient.send_message(
                QueueUrl=self.stackOutputs['BatchInQueue'],
                MessageBody='Stop')


