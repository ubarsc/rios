

ECR_URL := $(shell ./print_ecr_path.py --base)
REPO := $(shell ./print_ecr_path.py):latest
RIOS_VER := $(shell python3 -c 'import rios;print(rios.RIOS_VERSION)')

default: all

dist:
	cd ../../../..;python3 setup.py sdist --formats=gztar
	cp ../../../../dist/rios-$(RIOS_VER).tar.gz .

all: dist
	aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin $(ECR_URL)
	docker build --build-arg EXTRA_PACKAGES=${EXTRA_PACKAGES} --build-arg=PIP_PACKAGES=${PIP_PACKAGES} --build-arg RIOS_VER=$(RIOS_VER) -t rios .
	docker tag rios $(REPO)
	docker push $(REPO)
