#!/usr/bin/env bash

bash 1_find_overlaps.sh
python 2_generate_manifest.py

python 3a_make_conditioning_commands.py --quiet
python 3a_make_conditioning_commands.py | shuf | parallel -j 40

python 3b_make_coloc_commands.py --quiet
python 3b_make_coloc_commands.py | shuf | parallel -j 40

python 5_combine_results.py
python 6_process_results.py
