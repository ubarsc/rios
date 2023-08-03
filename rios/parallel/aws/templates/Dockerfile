
FROM ubuntu:22.04

ARG EXTRA_PACKAGES
ARG PIP_PACKAGES
ARG RIOS_VER

ARG DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Use Aussie mirrors
RUN sed -i 's/http:\/\/archive./http:\/\/ap-southeast-2.ec2.archive./g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get upgrade -y

RUN apt-get install -y python3-gdal python3-boto3 python3-pip $EXTRA_PACKAGES

COPY rios-$RIOS_VER.tar.gz /tmp
RUN cd /tmp && tar xf rios-$RIOS_VER.tar.gz \
    && cd rios-$RIOS_VER \
    && pip install . \
    && cd .. && rm -rf rios-$RIOS_VER rios-$RIOS_VER.tar.gz

ENTRYPOINT ["rios_subproc_awsbatch.py"]

# any pip packages?
RUN if [ -n "$PIP_PACKAGES" ] ; then pip install $PIP_PACKAGES; fi
