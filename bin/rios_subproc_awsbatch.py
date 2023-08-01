#!/usr/bin/env python
"""
Main program for RIOS subprocesses invocked via AWS Batch. 

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

BUCKET = os.getenv("RIOSBucket")
INQUEUE = os.getenv("RIOSInQueue")
OUTQUEUE = os.getenv("RIOSOutQueue")


def main():
    
    s3Client = boto3.client('s3')
    sqsClient = boto3.client('sqs')
    
    while True:
         resp = sqsClient.receive_message(QueueUrl=INQUEUE,
            MaxNumberOfMessages=1)
         if len(resp['Messages']) > 0:
             # just look at the first one
             msg = resp['Messages'][0]
             body = msg['Body']
             receiptHandle = msg['ReceiptHandle']
             self.sqsClient.delete_message(
                 QueueUrl=INQUEUE, ReceiptHandle=receiptHandle)
                 
             if body == 'Stop':
                 print('Job Exiting')
                 break
                 
             bl, x, y, o = body.split('_')
             outfile = 'block_{}_{}_out.pkl'.format(x, y)
             
             inPlk = io.Bytes()
             s3Client.download_fileobj(BUCKET, body, inPlk)
             
             s3Client.delete_object(Bucket=BUCKET, Key=body)
             
             outPkl = io.Bytes()
             subproc.runJob(inPlk, outPkl)
             
             s3Client.upload_fileobj(outPkl, BUCKET, outfile)
             
             self.sqsClient.send_message(QueueUrl=OUTQUEUE,
                MessageBody=outfile)
                
         else:
            time.sleep(30)


if __name__ == '__main__':
    main()
