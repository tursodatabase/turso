#!/bin/sh

BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT=$(git rev-parse --short HEAD)

if [ "$ANTITHESIS_TRIGGER" = "workflow_dispatch" ]; then
  TEST_TYPE="manual test"
else
  TEST_TYPE="scheduled test"
fi

# Decide who receives the test report. NOTIFY_EVERYONE adds the shared recipient
# list and NOTIFY_EMAIL adds a single custom address; when both are set the two
# are combined into the semicolon-delimited list the API expects. When neither is
# set the report is not emailed, so ad-hoc runs stay quiet by default.
RECIPIENTS=""
if [ "$NOTIFY_EVERYONE" = "true" ] && [ -n "$ANTITHESIS_EMAIL" ]; then
  RECIPIENTS="$ANTITHESIS_EMAIL"
fi
if [ -n "$NOTIFY_EMAIL" ]; then
  RECIPIENTS="${RECIPIENTS:+$RECIPIENTS;}$NOTIFY_EMAIL"
fi

REPORT_RECIPIENTS=""
if [ -n "$RECIPIENTS" ]; then
  REPORT_RECIPIENTS=",
      \"antithesis.report.recipients\":\"$RECIPIENTS\""
fi

curl --fail -u "$ANTITHESIS_USER:$ANTITHESIS_PASSWD" \
  -X POST https://$ANTITHESIS_TENANT.antithesis.com/api/v1/launch/limbo \
  -d "{\"params\": { \"antithesis.description\":\"$TEST_TYPE on $BRANCH @ $COMMIT\",
      \"custom.duration\":\"4\",
      \"antithesis.config_image\":\"$ANTITHESIS_DOCKER_REPO/limbo-config:antithesis-latest\",
      \"antithesis.images\":\"$ANTITHESIS_DOCKER_REPO/limbo-workload:antithesis-latest\"$REPORT_RECIPIENTS
      } }"
