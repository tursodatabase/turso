#!/bin/sh

BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)

if [ "$ANTITHESIS_TRIGGER" = "workflow_dispatch" ]; then
  TEST_TYPE="manual test"
else
  TEST_TYPE="scheduled test"
fi

curl --fail -u "$ANTITHESIS_USER:$ANTITHESIS_PASSWD" \
  -X POST https://$ANTITHESIS_TENANT.antithesis.com/api/v1/launch/limbo \
  -d "{\"params\": { \"antithesis.description\":\"$TEST_TYPE on $BRANCH @ $COMMIT\",
      \"custom.duration\":\"0.5\",
      \"antithesis.config_image\":\"$ANTITHESIS_DOCKER_REPO/limbo-config:antithesis-latest\",
      \"antithesis.images\":\"$ANTITHESIS_DOCKER_REPO/limbo-workload:antithesis-latest\",
      \"antithesis.report.recipients\":\"$ANTITHESIS_EMAIL\"
      } }"
