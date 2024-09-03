===================================
Parallel Processing using AWS Batch
===================================

This directory contains an example of using RIOS with AWS Batch.

It is assumed that you already have a reasonable understanding of the
AWS Batch system. Please ensure you understand the concepts used and the
changes (and costs!) associated with these examples before you use them.

The helper scripts below assume RIOS is installed on the machine you are using
them on. The CW_AWSBATCH compute worker kind is currently very entangled
with these scripts, and depends on the AWS Batch environment having been created
using these methods.

Files
-----

#. ``batch.yaml``. CloudFormation script used by ``createbatch.py`` to create and 
   modify the AWS assets needed. See the contents of this file for more information
#. ``Dockerfile``. An example Docker file for creating the 'worker' jobs. Built using Makefile.
#. ``Makefile``. Used to build the Dockerfile and push it to ECR. The CloudFormation 
   needs to run first. The ``RIOS_AWSBATCH_REGION`` environment variable must be set
   to the current AWS Region first. Install optional packages that your function may need
   using the ``EXTRA_PACKAGES`` and ``PIP_PACKAGES`` environment variables.
#. ``createbatch.py``. Used to create the AWS assets using the CloudFormation script ``batch.yaml``.
   There is a ``--modify`` option to update the existing stack. See the output of ``createbatch.py -h``
   for more information. The ``RIOS_AWSBATCH_REGION`` environment variable must be set
   to the current AWS Region first.
   Note also the ``--instancetype`` parameter. Specify ``optimal`` for x86 or one of the 
   Gravitron types (eg ``m7g``) for Gravitron. This must match the architecture of the Docker
   containers you are creating. By the default this is the architecture of the machine you
   are building, but can be altered by using the ``docker buildx`` command to perform cross
   compilation. See the Docker documentation for more information.
#. ``deletebatch.py``. Deletes the AWS Stack. Ensure you have removed all images from the ECR
   repositories first.

Main Script
-----------

While the Makefile (above) creates the worker Docker image in ECR, you will also need
a Dockerfile for your "main" script (ie. the script that calls RIOS). Skip this step if
you intend to run this script interactively, or through some other method that isn't AWS Batch.

Once you have created the "worker" Docker image (via ``Makefile``, above) we recommend a Dockerfile
that starts with ``FROM rios``. This will mean that your Docker image will already have RIOS and 
any optional packages you need. Then install any other packages your main script needs (but the workers
do not) with the Docker ``RUN`` command. Lastly copy your script in (plus any other data you need) using the
Docker ``COPY`` command and set your script as the default to run with the ``ENTRYPOINT`` command. This an example
``Dockerfile.main`` may look like this ::

        FROM rios
        RUN pip install pystac-client
        RUN mkdir /usr/local/data
        COPY mydata.gpkg /usr/local/data
        COPY myscript.py /usr/local/bin
        WORKDIR /usr/local/data
        ENTRYPOINT ["/usr/bin/python3", "/usr/local/bin/myscript.py"]

Once successfully build you will need to push this Docker image to the ``riosecrmain`` repository
already created by Cloudformation (above). An example shell script is below ::

        docker build -f Dockerfile.main -t riosmain .
        ECR_URL=${AWS_Account}.dkr.ecr.${AWS_Region}.amazonaws.com
        aws ecr get-login-password --region ${AWS_Region} | docker login --username AWS --password-stdin ${ECR_URL}
        docker tag riosmain ${ECR_URL}/riosecrmain:latest
        docker push ${ECR_URL}/riosecrmain:latest

You will need to set the ``AWS_Account`` and ``AWS_Region`` environment variables as needed. Lastly, submit this
main script to AWS Batch using either the AWS CLI or ``boto3`` with Python. Unless you've made any modifications
to the CloudFormation, the ``jobQueue`` will be ``riosJobQueue`` and the ``jobDefinition`` will be ``riosJobDefinitionMain``.

This main script should then spawn other AWS Batch jobs that will stay running until the processing is
finished.


Adapting to your own needs
--------------------------

The above is a simple way to get started with the AWS Batch support in RIOS. However it is likely
that most users will need to run RIOS inside their own VPC. Doing so should be straightforward as long as the 
following steps are followed:

#. To reduce confusion, create separate ECR repositories for the 'main' and worker jobs
#. Ensure that your jobs have internet access - this is needed for Batch jobs to start. It is recommended
to have your jobs running in a private subnet as shown in the example stack. You will need a NAT for internet
access in this situation.
#. Create an S3 endpoint so that S3 access is free for your jobs.
#. Ensure that your stack has the following outputs: BatchProcessingJobQueueName, BatchProcessingJobDefinitionName,
BatchVCPUS and BatchMaxVCPUS. These should refer to the queue and job definition for the workers.
#. Ensure the `RIOS_AWSBATCH_STACK` and `RIOS_AWSBATCH_REGION` environment variables are set
in the main script so that RIOS can start the worker jobs using the above stack outputs. 
#. Ensure that the security group that your jobs run as (both worker and main) allows TCP traffic
in the port range 30000-50000 from itself. Note this is not enabled in AWS by default.
#. Ensure your main jobs have enough storage attached if writing large output files.
#. Make sure you experiment with different EC2 instance types for your job. Performance and 
price will differ between these types.
