#!/usr/bin/env bash
#

set -euo pipefail

# Run cromwell
mkdir -p logs
java -Dconfig.file=configs/cromwell.config \
     -jar /Users/em21/software/cromwell/cromwell-36.jar run workflows/coloc.wdl \
     --inputs configs/workflow.config.json \
     > cromwell_log.txt

echo COMPLETE
