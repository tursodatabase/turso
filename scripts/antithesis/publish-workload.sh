#!/bin/sh

set -e

export DOCKER_REPO_URL=$ANTITHESIS_DOCKER_REPO

export IMAGE_NAME=limbo-workload

export DOCKER_IMAGE_VERSION=antithesis-latest

export DOCKER_BUILD_ARGS="--build-arg antithesis=true"

export DOCKERFILE=Dockerfile.antithesis

export DOCKER_DIR=.

docker login -u _json_key https://$ANTITHESIS_DOCKER_HOST --password "$ANTITHESIS_REGISTRY_KEY"

${BASH_SOURCE%/*}/publish-docker.sh
