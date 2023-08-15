"""
Module for implementing parallel with AWS Batch. 

See the class AWSBatchJobManager for the implementation.

Using AWS Services for parallel processing in RIOS
==================================================

This directory holds implementations of per tile parallel processing
using AWS services. Currently only AWS Batch is supported but it is
intended that other services will be added in future.

Refer to jobmanager.py for an overview of how RIOS handles parallel processing.

AWS Batch
=========

Creating the infrastructure
---------------------------

This implementation comes with a CloudFormation script (``templates/batch.yaml``)
to create a separate VPC with all the infrastructure required. It is recommended
to use the script `templates/createbatch.py` for the creation or modification (via the ``--modify``
command line option) of this CloudFormation stack. There are also options for
overriding some of the input parameters - see the output of `createbatch.py --help`
for more information.

When you have completed processing you can run ``templates/deletebatch.py`` to delete
all resources so you aren't paying for it. Note that you specify the region and stack
name for this script via the RIOS_BATCH_REGION and RIOS_BATCH_STACK environment variables.

Note that both ``createbatch.py`` and ``deletebatch.py`` have a ``--wait`` option that causes the
script to keep running until creation/deletion is complete. 

Creating the Docker image
-------------------------

AWS Batch requires you to provide a Docker image with the required software installed. 
A `Dockerfile` is provided for this, but it it recommended that you use the `Makefile`
to build the image as this handles the details of pulling the names out of the CloudFormation
stack and creating a tar file of RIOS for copying into the Docker image. To build and push to 
ECR simply run::

    make

By default this image includes GDAL, boto3 and RIOS. 

Normally your script will need extra packages to run. You can specify the names of Ubuntu packages
to also install with the environment variable `EXTRA_PACKAGES` like this::

    EXTRA_PACKAGES="python3-sklearn python3-skimage" make


You can also use the `PIP_PACKAGES` environment variable to set the name of any pip packages like this::

    PIP_PACKAGES="pydantic python-dateutil" make

You can also specify both if needed::

    EXTRA_PACKAGES="python3-sklearn python3-skimage" PIP_PACKAGES="pydantic python-dateutil" make

Setting up your main script
---------------------------

To enable parallel processing using AWS Batch in your RIOS script you must import the batch module::

    from rios.parallel.aws import batch

Secondly, you must set up an :class:`rios.applier.ApplierControls`
object and pass it to :func:`rios.applier.apply`. On this
object, you must make the following calls::

    controls.setNumThreads(4) # or whatever number you want
    controls.setJobManagerType('AWSBatch')

Note that the number of AWS Batch jobs started will be (numThreads - 1) as one job is done by the main RIOS script.

It is recommended that you run this main script within a container based on the one above. This reduces the likelihood
of problems introduced by different versions of Python or other packages your script needs between the main RIOS
script and the AWS Batch workers.

To do this, create a `Dockerfile` like the one below (replacing `myscript.py` with the name of your script)::

    # Created by make command above
    FROM rios:latest

    COPY myscript.py /usr/local/bin
    RUN chmod +x /usr/local/bin/myscript.py

    ENTRYPOINT ["/usr/bin/python3", "/usr/local/bin/myscript.py"]

Don't forget to pass in your ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment variables to this
container when it runs (these variables are automatically set if running as a AWS Batch job but you'll
need to set them otherwise).

Also a good idea to pass in your RIOS_BATCH_REGION and RIOS_BATCH_STACK environment variables if the
defaults have been overridden so that RIOS can find the CloudFormation stack.

Needless to say the account that this "main" script run as should have sufficient permissions on the resources 
created by CloudFormation. 
 
"""

import io
import os
import pickle
import boto3

from .. import jobmanager

from cloudpickle import cloudpickle

STACK_NAME = os.getenv('RIOS_BATCH_STACK', default='RIOS')
REGION = os.getenv('RIOS_BATCH_REGION', default='ap-southeast-2')


class AWSBatchException(Exception):
    pass


def getStackOutputs():
    """
    Helper function to query the CloudFormation stack for outputs.
    
    Uses the RIOS_BATCH_STACK and RIOS_BATCH_REGION env vars to 
    determine which stack and region to query.
    """
    client = boto3.client('cloudformation', region_name=REGION)
    resp = client.describe_stacks(StackName=STACK_NAME)
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


class AWSBatchJobManager(jobmanager.JobManager):
    """
    Implementation of parallelisation via AWS Batch.
    This uses 2 SQS queues for communication between the
    'main' RIOS script and the subprocesses (which run on Batch)
    and an S3 bucket to hold the pickled data (which the SQS
    messages refer to).
    """
    jobMgrType = 'AWSBatch'

    def __init__(self, numSubJobs):
        super().__init__(numSubJobs)
        # get the output of the CloudFormation so we know what 
        # the resources are called.
        self.stackOutputs = getStackOutputs()
        self.batchClient = boto3.client('batch', region_name=REGION)
        self.s3Client = boto3.client('s3', region_name=REGION)
        self.sqsClient = boto3.client('sqs', region_name=REGION)

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


