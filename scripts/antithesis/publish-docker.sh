#!/bin/sh

set -e

if [ -z "$DOCKER_REPO_URL" ]; then
    echo "Error: DOCKER_REPO_URL is not set."
    exit 1
fi

if [ -z "$IMAGE_NAME" ]; then
    echo "Error: IMAGE_NAME is not set."
    exit 1
fi

if [ -z "$DOCKER_IMAGE_VERSION" ]; then
    DOCKER_IMAGE_VERSION=$(git rev-parse HEAD)
fi

DOCKER_IMAGE=$DOCKER_REPO_URL/$IMAGE_NAME:$DOCKER_IMAGE_VERSION

docker build --platform linux/amd64 -f $DOCKERFILE -t $DOCKER_IMAGE $DOCKER_BUILD_ARGS $DOCKER_DIR

docker push $DOCKER_IMAGE
