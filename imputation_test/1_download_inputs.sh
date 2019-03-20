#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p input_data
cd input_data

gsutil cp gs://genetics-portal-staging/coloc/190320/coloc.json.gz .
gsutil cp gs://genetics-portal-staging/finemapping/190320/top_loci.json.gz .

# Create list of lead variants
zcat < coloc.json.gz | jq -r '[.left_chrom, .left_pos, .left_ref, .left_alt] | @tsv' | uniq | sort | uniq > var_list.tsv

echo COMPLETE
