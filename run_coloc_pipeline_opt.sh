#!/usr/bin/env bash

NCORES=$1
export PYSPARK_SUBMIT_ARGS=$2
#export PYSPARK_SUBMIT_ARGS="--driver-memory 100g pyspark-shell"

echo "Running on $NCORES cores"
echo "PYSPARK_SUBMIT_ARGS: $PYSPARK_SUBMIT_ARGS"

#python partition_top_loci_by_chrom.py # Script from fine-mapping pipeline

#echo -e "\n1_find_overlaps.sh"
# This step requires a lot of memory
time /bin/bash 1_find_overlaps.sh # 10 min last run

# Generate the manifest from the overlap table
echo -e "\n2_generate_manifest.py"
time python 2_generate_manifest.py # ~24 min last run

#cp /configs/manifest_unfiltered.json.gz /configs/manifest_unfiltered.all.json.gz

# Subset to chr21 for testing
#zcat /configs/manifest_unfiltered.all.json.gz | grep 'ukb_v3_chr21.downsampled10k' | head -n 10000 | gzip > /configs/manifest_unfiltered.json.gz

# Remove lines which are already in output from previous coloc runs
# (If no previous coloc, this just renames the file)
echo -e "\n2b_filter_manifest.py"
time python 2b_filter_manifest.py

# The script below generates commands to compute conditionally independent
# sumstats with GCTA. It creates two files `commands_todo.cond.txt.gz` and
# `commands_done.cond.txt.gz` showing which analyses have not yet/already
# been done. This step can be stopped at any time and restarted without
# repeating any completed analyses. You can safely regenerate the `commands_*.txt.gz`
# commands while the pipeline is running using `python 3_make_commands.py --quiet`.

# Note that it takes some time just to make the commands, e.g. >30 min.
# This gets longer as more output files have been written... so if it's piped
# to shuf then nothing will start before 30 min.
echo -e "\n3a_make_conditioning_commands.py"
time python 3a_make_conditioning_commands.py --quiet

# Creates `commands_todo_coloc_opt.txt`. Each command operates on a chunk of
# `coloc_manifest_opt.todo.txt.gz` written in the configs/commands_split directory.
echo -e "\n3b_make_coloc_commands_opt.sh"
time bash 3b_make_coloc_commands_opt.sh

############### Main conditioning step
# Took 26 hrs last run (222 cores, 400 Gb)
echo -e "\nRunning conditioning commands in parallel"
time zcat /configs/commands_todo.cond.txt.gz | shuf | parallel -j $NCORES --joblog /output/parallel.jobs.cond.log | tee /output/run_gcta_cond.out.txt
# Note: "--bar" can make things slower if there are millions of commands


############### Main coloc step
# Took ~ 2 hrs last run (222 cores), 6.8 M commands
echo -e "\nRunning coloc commands in parallel"
time cat /configs/commands_todo_coloc_opt.txt | parallel -j $NCORES --joblog /output/parallel.jobs.coloc.log --bar | tee /output/run_coloc_opt.out.txt
# Note: "--bar" can make things slower if there are millions of commands

# Combine the results of all the individual analyses
# This step can be slow/inefficient due to Hadoop many small files problem
echo -e "\n5_combine_results.py"
time python 5_combine_results.py # Took ~3 hrs last run (222 cores, 400 Gb)

# Process the results for exporting. Renames or computes a few columns,
# e.g. coloc_h4_h3 ratio, filters based on number of overlapping vars,
# makes symmetric coloc result matrix.
echo -e "\n6_process_results.py"
time python 6_process_results.py # Takes just a minute or two

# Run this step if merging with previous coloc results
#export PYSPARK_SUBMIT_ARGS="--driver-memory 25g pyspark-shell"
#time python 7_merge_previous_results.py | tee /output/merge_previous_results.log

echo -e "\nDONE"
date
