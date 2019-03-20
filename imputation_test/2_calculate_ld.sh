#!/usr/bin/env bash
#

set -euo pipefail

# Args
cores=32
instance_name="em-ld"
instance_zone="europe-west1-d"
script='scripts/calc_ld_1000G.py'
in_ld='/home/em21/genetics-colocalisation/imputation_test/input_data/uk10k/CHROM.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101'
in_varlist='input_data/var_list.tsv'
out_dir='ld_output'

mkdir -p $out_dir

# Construct commands
cat $in_varlist | while read line; do
    
    # Make varid
    arr=($line)
    var_id=${arr[0]}_${arr[1]}_${arr[2]}_${arr[3]}

    # Make command
    outf="$out_dir/$var_id.ld.tsv.gz"
    echo python $script \
      --varid $var_id \
      --bfile $in_ld \
      --ld_window 1000000 \
      --min_r2 0.7 \
      --outf $outf

done | parallel -j $cores

echo COMPLETE

gcloud compute instances stop $instance_name --zone=$instance_zone