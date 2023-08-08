#!/usr/bin/env python
"""
Main program for RIOS subprocesses invoked via AWS Batch. 

"""
# This file is part of RIOS - Raster I/O Simplification
# Copyright (C) 2012  Sam Gillingham, Neil Flood
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import print_function

import os
import io
import time
import boto3

from rios.parallel import subproc

# These vars are set in the container environment
# by CloudFormation
BUCKET = os.getenv("RIOSBucket")
INQUEUE = os.getenv("RIOSInQueue")
OUTQUEUE = os.getenv("RIOSOutQueue")
DFLT_NOMSG_TIMEOUT_SECS = 60 * 60  # 1 hour
NOMSG_TIMEOUT_SECS = int(os.getenv('RIOS_NOMSG_TIMEOUT', 
    default=DFLT_NOMSG_TIMEOUT_SECS))

# keep a track of the last time we got a message
# - if too long we can assume the main script exited
# and exit ourselves
LAST_MESSAGE_TIME = time.time()


def main():

    global LAST_MESSAGE_TIME
    
    s3Client = boto3.client('s3')
    # SQS client needs a region - should be same as s3 bucket
    response = s3Client.get_bucket_location(Bucket=BUCKET)
    region = response['LocationConstraint']
    sqsClient = boto3.client('sqs', region_name=region)
    
    while True:
        # get a message from the queue
        resp = sqsClient.receive_message(QueueUrl=INQUEUE,
            MaxNumberOfMessages=1, WaitTimeSeconds=20)  # must be <= 20
        if 'Messages' in resp and len(resp['Messages']) > 0:
            # we got something
            LAST_MESSAGE_TIME = time.time()

            # just look at the first one (just asked for 1)
            msg = resp['Messages'][0]
            body = msg['Body']
            receiptHandle = msg['ReceiptHandle']
            sqsClient.delete_message(
                QueueUrl=INQUEUE, ReceiptHandle=receiptHandle)
                 
            # message from the main script to exit
            if body == 'Stop':
                print('Job Exiting')
                break

            print('Started', body)
                 
            # get the info out of the pkl filename
            bl, x, y, o = body.split('_')
            outfile = 'block_{}_{}_out.pkl'.format(x, y)
             
            # read the input pkl
            inPkl = io.BytesIO()
            s3Client.download_fileobj(BUCKET, body, inPkl)
            inPkl.seek(0)
             
            # delete it
            s3Client.delete_object(Bucket=BUCKET, Key=body)
             
            # run the job
            outPkl = io.BytesIO()
            subproc.runJob(inPkl, outPkl)

            # upload the result
            outPkl.seek(0)             
            s3Client.upload_fileobj(outPkl, BUCKET, outfile)
             
            # send message back to main script
            sqsClient.send_message(QueueUrl=OUTQUEUE,
                MessageBody=outfile)

            print('finished', body)
        elif ((time.time() - LAST_MESSAGE_TIME) > 
                NOMSG_TIMEOUT_SECS):
            print('No message received within timeout. Exiting')
            break
        else:
            # sleep for a bit before checking again
            time.sleep(30)


if __name__ == '__main__':
    main()
