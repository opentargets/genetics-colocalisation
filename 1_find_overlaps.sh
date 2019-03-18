#!/usr/bin/env bash
#

set -euo pipefail

# Args
# in_credset=/Users/em21/Projects/genetics-finemapping/results/credset
in_credset=/home/em21/genetics-finemapping/results/credset
outf=configs/overlap_table

mkdir -p configs

# Run
python scripts/generate_overlap_table.py \
  --in_credset $in_credset \
  --which_set 95 \
  --which_method all \
  --outf $outf

echo COMPLETE
