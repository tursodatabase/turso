#!/bin/bash

echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl

set -Eeuo pipefail

if [ -s /opt/antithesis/targeted_coverage.json ]; then
  cp /opt/antithesis/targeted_coverage.json "$ANTITHESIS_OUTPUT_DIR/targeted_coverage.json"
fi

exec "$@"
