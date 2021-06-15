#!/usr/bin/env bash
#

set -euo pipefail

# Args
outf=/output/overlap_table

mkdir -p configs

# Run
python scripts/generate_overlap_table.py \
  --in_credset /data/finemapping/credset \
  --which_set 95 \
  --which_method all \
  --max_dist 500000 \
  --outf $outf

echo COMPLETE
