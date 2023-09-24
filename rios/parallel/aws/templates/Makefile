# A make file to create and push a docker image with RIOS and any other required packages
# to ECR.
# set the RIOS_BATCH_REGION environment variable to the name of the AWS region you wish to use
# To request other packages be installed into this docker either or both of:
# EXTRA_PACKAGES environment variable (for Ubuntu pakackes)
# PIP_PACKAGES environment variable (for pip pakackes)
# These accept a space seperated list of package names

ifndef RIOS_BATCH_REGION
$(error RIOS_BATCH_REGION is not set)
endif

ECR_URL := $(shell ./print_ecr_path.py --base)
REPO := $(shell ./print_ecr_path.py):latest
RIOS_VER := $(shell python3 -c 'import rios;print(rios.RIOS_VERSION)')

default: all

# grab the current RIOS source tree and make it available to the 
# docker COPY command
dist:
	cd ../../../..;python3 setup.py sdist --formats=gztar
	cp ../../../../dist/rios-$(RIOS_VER).tar.gz .

# Login to ECR, build package and push to ECR
all: dist
	aws ecr get-login-password --region ${RIOS_BATCH_REGION} | docker login --username AWS --password-stdin $(ECR_URL)
	docker build --build-arg AWS_REGION=${RIOS_BATCH_REGION} --build-arg EXTRA_PACKAGES=${EXTRA_PACKAGES} --build-arg=PIP_PACKAGES=${PIP_PACKAGES} --build-arg RIOS_VER=$(RIOS_VER) -t rios .
	docker tag rios $(REPO)
	docker push $(REPO)
