#!/usr/bin/env bash
#

set -euo pipefail

python scripts/generate_overlap_table.py \
  --in_credset input_examples/finemapping_output/credible_sets.parquet \
  --window 2 \
  --which_set 95 \
  --which_method all \
  --outf output/overlap_table.tsv.gz

echo COMPLETE
