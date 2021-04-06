#!/usr/bin/env bash

bash 1_find_overlaps.sh
python 2_generate_manifest.py
python 3_make_commands.py --quiet
bash 4_run_commands.sh
python 5_combine_results.py
python 6_process_results.py