#!/usr/bin/env bash
#

set -euo pipefail

# Args
in_ld='plink.placeholder'
in_varlist='input_data/var_list.tsv'
out_dir='ld_output'

mkdir -p out_dir

# Construct commands
cat $in_varlist | while read line; do
    
    # Make varid
    arr=($line)
    var_id=${arr[0]}_${arr[1]}_${arr[2]}_${arr[3]}

    # Make command
    outf="$out_dir/$var_id.ld.tsv.gz"
    echo python calc_ld_1000G.py \
      --varid $var_id \
      --bfile $in_ld \
      --ld_window 1000000 \
      --min_r2 0.7 \
      --outf $outf

done | parallel -j 4

echo COMPLETE
