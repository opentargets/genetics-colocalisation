#!/usr/bin/env bash

set -euo pipefail

# OPTIMIZATION: split commands into chunks, so that each R script can run
# multiple coloc commands. When colocs were run one at a time, we found
# previously that 90% of the time was spent starting the script & loading
# libraries.
COLOC_CHUNK_SIZE=1000
OUT_CMD_FILE=/configs/commands_todo_coloc_opt.txt

# Creates `coloc_manifest_opt.txt.gz`.
# Note that this script also makes `commands_todo.coloc.txt.gz` and `commands_done.coloc.txt.gz`,
# but these are no longer used. This should probably be fixed sometime.
python 3b_make_coloc_commands.py --quiet

mkdir -p /configs/commands_split
#rm -r /configs/commands_split/coloc_todo*
#zcat /configs/coloc_manifest_opt.txt.gz | sed 's/coloc_res.json.gz/coloc_res.csv/g' | split -l $COLOC_CHUNK_SIZE -d -a 4 - /configs/commands_split/coloc_todo.split.
zcat /configs/coloc_manifest_opt.txt.gz | split -l $COLOC_CHUNK_SIZE -d -a 4 - /configs/commands_split/coloc_todo.split.

#rm $OUT_CMD_FILE
for fname in `ls /configs/commands_split/coloc_todo.split.*`; do
    echo "Processing $fname"
    echo "Rscript /coloc/scripts/coloc_opt.R $fname" >> $OUT_CMD_FILE
done

