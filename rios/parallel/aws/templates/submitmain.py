#!/usr/bin/env python3

import boto3

def main():
    batch = boto3.client('batch', region_name='us-west-2')
    batch.submit_job(jobName='conc_test',
        jobQueue='riosJobQueue',
        jobDefinition='riosJobDefinitionMain',
        containerOverrides={
            'command': ['-k', 'awsbatch', '-c', '2', '-r', '2']
        })


if __name__ == '__main__':
    main()
