#!/bin/sh

curl --fail -u "$ANTITHESIS_USER:$ANTITHESIS_PASSWD" \
  -X POST https://$ANTITHESIS_TENANT.antithesis.com/api/v1/launch/basic_test \
  -d "{\"params\": { \"antithesis.description\":\"basic_test on main\",
      \"antithesis.duration\":\"60\",
      \"antithesis.config_image\":\"$ANTITHESIS_DOCKER_REPO/limbo-config:antithesis-latest\",
      \"antithesis.images\":\"$ANTITHESIS_DOCKER_REPO/limbo-workload:antithesis-latest\",
      \"antithesis.report.recipients\":\"$ANTITHESIS_EMAIL\"
      } }"
