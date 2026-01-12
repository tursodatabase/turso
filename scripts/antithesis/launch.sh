#!/bin/sh

BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)

curl --fail -u "$ANTITHESIS_USER:$ANTITHESIS_PASSWD" \
  -X POST https://$ANTITHESIS_TENANT.antithesis.com/api/v1/launch/limbo \
  -d "{\"params\": { \"antithesis.description\":\"basic_test on $BRANCH @ $COMMIT\",
      \"custom.duration\":\"4\",
      \"antithesis.config_image\":\"$ANTITHESIS_DOCKER_REPO/limbo-config:antithesis-latest\",
      \"antithesis.images\":\"$ANTITHESIS_DOCKER_REPO/limbo-workload:antithesis-latest\",
      \"antithesis.report.recipients\":\"$ANTITHESIS_EMAIL\"
      } }"
