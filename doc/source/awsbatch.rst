===================================
Parallel Processing using AWS Batch
===================================

This directory contains an example of using RIOS with AWS Batch.

Please ensure you understand the concepts used and the changes (and costs!)
associated with these examples before you use them.

The helper scripts below assume RIOS is installed on the machine you are using them on.

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
