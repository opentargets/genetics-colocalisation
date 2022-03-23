#!/usr/bin/env bash

# OPTIMIZATION: split manifest into chunks, so that each R script can run
# multiple coloc commands. When colocs were run one at a time, we found
# previously that 90% of the time was spent starting the script & loading
# libraries.
COLOC_CHUNK_SIZE=1000
OUT_CMD_FILE=/configs/commands_todo_coloc_opt.txt

# Remove existing split commands if they exist
if [ -d /configs/commands_split ]; then
    rm /configs/commands_split/coloc_todo*
fi
mkdir -p /configs/commands_split

# Split the manifest into chunks, keeping the header line in each
# chunk. First we split without the header.
zcat /configs/coloc_manifest.tsv.gz | sed '1d' | split -l $COLOC_CHUNK_SIZE -d -a 4 - /configs/commands_split/coloc_todo.split.

# Now add the header back into each chunk
zcat /configs/coloc_manifest.tsv.gz | head -n 1 > /configs/coloc_manifest_header.tsv
for fname in /configs/commands_split/coloc_todo.split.*; do
    cat /configs/coloc_manifest_header.tsv > tmp_file
    cat "$fname" >> tmp_file
    mv -f tmp_file "$fname"
done

# Remove OUT_CMD_FILE if it exists
if [ -f $OUT_CMD_FILE ]; then
    rm $OUT_CMD_FILE
fi

# Make an R command for each chunk, including arguments for where the output
# and logs should go.
mkdir -p /output/coloc
mkdir -p /output/logs/coloc
for fname in /configs/commands_split/coloc_todo.split.*; do
    echo "Processing $fname"
    # Get the chunk number from the filename
    chunk_num=`echo $fname | sed 's/.*\.split\.\([0-9]*\).*/\1/'`
    echo "Rscript /coloc/scripts/coloc_opt.R  $fname  /output/coloc/coloc_res.$chunk_num.csv  >  /output/logs/coloc/coloc_log.$chunk_num.txt 2>&1" >> $OUT_CMD_FILE
done

