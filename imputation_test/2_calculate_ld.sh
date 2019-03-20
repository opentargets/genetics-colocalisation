#!/usr/bin/env bash
#

set -euo pipefail

# Args
script='scripts/calc_ld_1000G.py'
in_ld='/home/em21/genetics-v2d-data/tmp/190315/ld/1000Genomep3/EUR/EUR.CHROM.1000Gp3.20130502'
in_varlist='input_data/var_list.tsv'
out_dir='ld_output'

mkdir -p $out_dir

# Construct commands
cat $in_varlist | while read line; do
    
    # Make varid
    arr=($line)
    var_id=${arr[0]}_${arr[1]}_${arr[2]}_${arr[3]}

    # Make command
    bfile=${in_ld/CHROM/${arr[0]}}
    outf="$out_dir/$var_id.ld.tsv.gz"
    echo python $script \
      --varid $var_id \
      --bfile $bfile \
      --ld_window 1000000 \
      --min_r2 0.7 \
      --outf $outf

done #| parallel -j 4

echo COMPLETE
