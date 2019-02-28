#!/usr/bin/env bash
#

set -euo pipefail

# Args
in_credset=/Users/em21/Projects/genetics-finemapping/output/credible_set.json.gz
outf=tmp/overlap_table.json

# Create output dirs
mkdir -p tmp
mkdir -p output

# Input input credsets
credset_unzip=tmp/credible_set.json
if [ ! -f $credset_unzip ]; then
    zcat < $in_credset > $credset_unzip
fi

# Run
python scripts/generate_overlap_table.py \
  --in_credset $credset_unzip \
  --which_set 95 \
  --which_method all \
  --outf $outf

# Run toy example
# python scripts/generate_overlap_table.py \
#   --in_credset tmp/credible_set.toy.json \
#   --which_set 95 \
#   --which_method all \
#   --outf $outf

echo COMPLETE
