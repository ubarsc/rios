# Using AWS Services for parallel processing in RIOS

This directory holds implementations of per tile parallel processing
using AWS services. Currently only AWS Batch is supported but it is
intended that other services will be added in future.

Refer to jobmanager.py for an overview of how RIOS handles parallel processing.

## AWS Batch

### Creating the infrastructure

This implementation comes with a CloudFormation script (`templates/batch.yaml`)
to create a separate VPC with all the infrastructure required. It is recommended
to use the script `templates/createbatch.py` for the creation or modification (via the `--modify`
command line option) of this CloudFormation stack. There are also options for
overriding some of the input parameters - see the output of `createbatch.py --help`
for more information.

When you have completed processing you can run `templates/deletebatch.py` to delete
all resources so you aren't paying for it. 

Note that both  `createbatch.py` and `deletebatch.py` have a `--wait` option that causes the
script to keep running until creation/deletion is complete. 

### Creating the Docker image

AWS Batch requires you to provide a Docker image with the required software installed. 
A `Dockerfile` is provided for this, but it it recommended that you use the `Makefile`
to build the image as this handles the details of pulling the names out of the CloudFormation
stack and creating a tar file of RIOS for copying into the Docker image. To build and push to 
ECR simply run:
```
make
```

By default this image includes GDAL, boto3 and RIOS. 

Normally your script will need extra packages to run. You can specify the names of Ubuntu packages
to also install with the environment variable `EXTRA_PACKAGES` like this:
```
EXTRA_PACKAGES="python3-sklearn python3-skimage" make
```

You can also use the `PIP_PACKAGES` environment variable to set the name of any pip packages like this:
```
PIP_PACKAGES="pydantic python-dateutil" make
```

You can also specify both if needed:
```
EXTRA_PACKAGES="python3-sklearn python3-skimage" PIP_PACKAGES="pydantic python-dateutil" make
```

### Setting up your main script

To enable parallel processing using AWS Batch in your RIOS script you must import the batch module:
```
from rios.parallel.aws import batch
```

Secondly, you must set up an (Applier Controls)[https://www.rioshome.org/en/latest/rios_applier.html#rios.applier.ApplierControls]
object and pass it to (apply)[https://www.rioshome.org/en/latest/rios_applier.html#rios.applier.apply]. On this
object, you must make the following calls:
```
controls.setNumThreads(4) # or whatever number you want
controls.setJobManagerType('AWSBatch')
```

Note that the number of AWS Batch jobs started will be (numThreads - 1) as one job is done by the main RIOS script.

It is recommended that you run this main script within a container based on the one above. This reduces the likelihood
of problems introduced by different versions of Python or other packages your script needs between the main RIOS
script and the AWS Batch workers.

To do this, create a `Dockerfile` like the one below (replacing `myscript.py` with the name of your script):

```
# Created by make command above
FROM rios:latest

COPY myscript.py /usr/local/bin
RUN chmod +x /usr/local/bin/myscript.py

ENTRYPOINT ["/usr/bin/python3", "/usr/local/bin/myscript.py"]
```

Don't forget to pass in your `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables to this
container when it runs (these variables are automatically set if running as a AWS Batch job but you'll
need to set them otherwise).

Needless to say the account that this "main" script run as should have sufficient permissions on the resources 
created by CloudFormation. 
 
