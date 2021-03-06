#!/usr/bin/env bash
#

set -euo pipefail

# Args
in_credset=/home/ubuntu/results/finemapping/results/credset
outf=/home/ubuntu/results/coloc/overlap_table

mkdir -p configs

# Run
python scripts/generate_overlap_table.py \
  --in_credset $in_credset \
  --which_set 95 \
  --which_method all \
  --max_dist 500000 \
  --outf $outf

echo COMPLETE
