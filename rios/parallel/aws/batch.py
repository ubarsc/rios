"""
Module for implementing parallel with AWS Batch
"""

import io
import boto3

from .. import jobmanager

# Import a pickler which can pickle functions, with their dependencies, as well
# as data. Either use the version installed with cloudpickle
# (https://github.com/cloudpipe/cloudpickle) or the bundled versin
try:
    from cloudpickle import cloudpickle
except ImportError:
    # Import from our own local copy. This is what will usually happen. 
    from . import cloudpickle

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
    if (len(resp['Stacks']) == 0 
            or resp['Stacks'][0]['StackStatus'] != 'CREATE_COMPLETE'):
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
    jobMgrType = 'AWSBatch'

    def __init__(self, numSubJobs, stackName=DFLT_STACK_NAME, 
            region=DFLT_REGION):
        super().__init__(self, numSubJobs)
        self.stackOutputs = getStackOutputs(stackName, region)
        self.batchClient = boto3.client('cloudformation', region_name=region)
        self.s3Client = boto3.client('s3', region_name=region)
        self.sqsClient = boto3.client('sqs', region_name=region)
        # start the required number of batch jobs running now
        for n in range(numSubJobs):
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
        fileObj = io.BytesIO(allInputsPickled)
        
        s3Key = 'block_{}_{}_in.pkl'.format(jobInfo.info.xblock, jobInfo.info.yblock)
        
        self.s3Client.upload_fileobj(fileObj, 
            self.stackOutputs['BatchBucket'], s3Key)
            
        self.sqsClient.send_message(QueueUrl=self.stackOutputs['BatchInQueue'],
            MessageBody=s3Key)
            
        return (jobInfo.info.xblock, jobInfo.info.yblock)
            
    def waitOnJobs(self, jobIDlist):
        """
        Wait on all the jobs. Do nothing.
        """
        
    def gatherAllOutputs(self, jobIDlist):
        outputBlocksList = [jobIDlist[0]]
        # convert to a set so we can easily search for which blocks 
        # out 'ours'. They should all be, but I'm just being paranoid
        inBlocks = set()
        for xblock, yblock in jobIDlist[1:]:
            inBlocks.add((xblock, yblock))
        outputBlocksDict = {}
        
        while len(outputBlocksDict) < len(jobIDlist) - 1:
            resp = self.sqsClient.receive_message(
                QueueUrl=self.stackOutputs['BatchOutQueue'])
                
            for msg in resp['Messages']:
                body = msg['Body']
                receiptHandle = msg['ReceiptHandle']
                bl, x, y, o = body.split('_')
                x = int(x)
                y = int(y)
                # one of ours?
                if (x, y) in inBlocks:
                    self.sqsClient.delete_message(
                        QueueUrl=self.stackOutputs['BatchOutQueue'], 
                        ReceiptHandle=receiptHandle)
                        
                    # download
                    pickledOutput = io.BytesIO()
                    self.s3Client.download_fileobj(
                        self.stackOutputs['BatchBucket'], body, fileObj)
                    outputObj = pickle.loads(pickledOutput)
                    outputBlocksDict[(x, y)] = outputObj
                    
                    self.s3Client.delete_object(
                        Bucket=self.stackOutputs['BatchBucket'], Key=body)

        # now convert dict back to list so in same order requested
        for xblock, yblock in jobIDlist[1:]:
            obj = outputBlocksDict[(xblock, yblock)]
            outputBlocksList.append(obj)
        
        
    def finalise(self):
        """
        Stop our AWS Batch jobs by sending a special message to the queue
        """
        for n in range(self.numSubJobs):
            self.sqsClient.send_message(
                QueueUrl=self.stackOutputs['BatchInQueue'],
                MessageBody='Stop')


