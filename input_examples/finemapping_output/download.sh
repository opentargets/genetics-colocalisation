#!/usr/bin/env bash
#

set -euo pipefail

scp "ubuntu@172.27.17.55:/home/ubuntu/finemapping/output/*.parquet" .

echo COMPLETE
