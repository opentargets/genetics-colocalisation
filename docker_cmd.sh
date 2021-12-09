# May need to fix some config issues with Docker
sudo groupadd docker
sudo usermod -aG docker ${USER}
sudo systemctl restart docker
# Exit VM and re-login

cd ~/genetics-colocalisation
docker build --tag otg-coloc .

mkdir -p ~/coloc

# docker run -it --rm \
#     --ulimit nofile=1024000:1024000 \
#     -v /home/js29/data/top_loci_by_chrom:/data/top_loci_by_chrom \
#     -v /home/js29/data/finemapping/credset:/data/credset \
#     -v /home/js29/data/filtered/significant_window_2mb:/data/significant_window_2mb \
#     -v /home/js29/data/ukb_v3_downsampled10k:/data/ukb_v3_downsampled10k_plink \
#     -v /home/js29/output:/data \
#     otg-coloc /bin/bash run_coloc_pipeline_opt.sh

tmux

docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v ~/data:/data \
    -v ~/configs:/configs \
    -v ~/output:/output \
    otg-coloc /bin/bash

NCORES=31

export PYSPARK_SUBMIT_ARGS="--driver-memory 150g pyspark-shell"
time /bin/bash 1_find_overlaps.sh # 10 min last run
time python 2_generate_manifest.py # 
time python 2b_filter_manifest.py

# Note that it takes some time just to make the commands, e.g. >30 min.
# This gest longer as more output files have been written... so if it's piped
# to shuf then nothing will start before 30 min.
# Last run took about 5 hrs to generate the commands.
#time python 3a_make_conditioning_commands.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.2.log
time python 3a_make_conditioning_commands.py | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.log

# Generating commands took 7.5 hrs for the last run (3.8 million manifest lines).
# However, once many files have been created, then it would take > 60 hrs just to
# determine the remaining commands to complete, by my estimate.

# Note that using --bar with parallel seems to slow it down massively, at least
# when piping in 3.8 M commands, though it worked well with only thousands of
# commands.
cat /output/coloc_completed_files.tsv | cut -f 27 | sed 's/file:\/\//g' | sed 's/file://g'> /configs/coloc_res_done.txt

#time python 3b_make_coloc_commands.py --quiet
#time python 3b_make_coloc_commands.fix.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --joblog /output/parallel.jobs.coloc.run3.log
time python 3b_make_coloc_commands.py | shuf | parallel -j $NCORES --joblog /output/parallel.jobs.coloc.log

time python 5_combine_results.py # Took 14 hrs last run

# Fix that was needed when preparing R6 - shouldn't generally need to be run
#cp /output/coloc_raw.parquet /output/coloc_raw_needs_fix.parquet
#export PYSPARK_SUBMIT_ARGS="--driver-memory 10g pyspark-shell"
#time python other/5b_filter_for_top_loci.py # Takes just a minute or two

time python 6_process_results.py # Takes just a minute or two
time python 7_merge_previous_results.py
