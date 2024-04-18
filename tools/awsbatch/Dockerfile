
# Based off Ubuntu
FROM public.ecr.aws/ubuntu/ubuntu:22.04

# These need to be passed in as --build-args
ARG EXTRA_PACKAGES
ARG PIP_PACKAGES
ARG RIOS_VER
ARG AWS_REGION=ap-southeast-2
ENV AWS_REGION_ENV=$AWS_REGION

ARG DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# use local mirror
RUN sed -i "s/http:\/\/ports./http:\/\/${AWS_REGION_ENV}.ec2.ports./g" /etc/apt/sources.list
RUN sed -i "s/http:\/\/archive./http:\/\/${AWS_REGION_ENV}.ec2.archive./g" /etc/apt/sources.list
RUN apt-get update
RUN apt-get upgrade -y

# install our prereqs, plus anything else the user has asked for
RUN apt-get install -y python3-gdal python3-boto3 python3-cloudpickle python3-pip $EXTRA_PACKAGES

COPY rios-$RIOS_VER.tar.gz /tmp
# install RIOS
RUN cd /tmp && tar xf rios-$RIOS_VER.tar.gz \
    && cd rios-$RIOS_VER \
    && pip install . \
    && cd .. && rm -rf rios-$RIOS_VER rios-$RIOS_VER.tar.gz

# Set our subproc script for AWS Batch as the entrypoint.
ENTRYPOINT ["/usr/bin/python3", "/usr/local/bin/rios_computeworker"]

# any pip packages?
RUN if [ -n "$PIP_PACKAGES" ] ; then pip install $PIP_PACKAGES; fi
