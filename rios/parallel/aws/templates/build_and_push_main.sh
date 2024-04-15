#!/bin/bash

set -ex

docker build -f Dockerfile.main -t riosmain .

ECR_URL=158798577683.dkr.ecr.us-west-2.amazonaws.com

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin ${ECR_URL}

docker tag riosmain ${ECR_URL}/riosecrmain:latest

docker push ${ECR_URL}/riosecrmain:latest
