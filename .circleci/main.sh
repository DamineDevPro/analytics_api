#!/bin/bash

# image name
IMAGE=ecom-spark-analytics

# login docker
echo "$DOCKER_PASS" | docker login --username $DOCKER_USER --password-stdin

# bump version
version=`cat MAINVERSION`
echo "version: $version"

# run build
docker build -t $DOCKER_USER/$IMAGE .

# tag it
docker tag $DOCKER_USER/$IMAGE:latest $DOCKER_USER/$IMAGE:$version

# push it
docker push $DOCKER_USER/$IMAGE:latest
docker push $DOCKER_USER/$IMAGE:$version