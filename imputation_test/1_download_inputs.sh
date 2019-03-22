#!/usr/bin/env bash
#

set -euo pipefail

mkdir -p input_data
cd input_data

gsutil cp gs://genetics-portal-staging/coloc/190320/coloc.json.gz .
gsutil cp gs://genetics-portal-staging/finemapping/190320/top_loci.json.gz .
gsutil cp gs://genetics-portal-staging/coloc/190320/overlap_table/part-00000-4001a5cb-f44e-4599-ad1f-5461220edc40-c000.json.gz overlap_table.json.gz

# Create list of lead variants
zcat < coloc.json.gz | jq -r '[.left_chrom, .left_pos, .left_ref, .left_alt] | @tsv' | uniq | sort | uniq > var_list.tsv

echo COMPLETE
