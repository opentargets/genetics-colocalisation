# May need to fix some config issues with Docker
sudo groupadd docker
sudo usermod -aG docker ${USER}
sudo systemctl restart docker
# Exit VM and re-login

docker build --tag otg-coloc .

mkdir -p /home/js29/coloc

# docker run -it --rm \
#     --ulimit nofile=1024000:1024000 \
#     -v /home/js29/data/top_loci_by_chrom:/data/top_loci_by_chrom \
#     -v /home/js29/data/finemapping/credset:/data/credset \
#     -v /home/js29/data/filtered/significant_window_2mb:/data/significant_window_2mb \
#     -v /home/js29/data/ukb_v3_downsampled10k:/data/ukb_v3_downsampled10k_plink \
#     -v /home/js29/output:/data \
#     otg-coloc /bin/bash run_coloc_pipeline_opt.sh


docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v /home/js29/data:/data \
    -v /home/js29/configs:/configs \
    -v /home/js29/output:/output \
    otg-coloc /bin/bash

time /bin/bash 1_find_overlaps.sh
time python 2_generate_manifest.py
time python 2b_filter_manifest.py

# Note that it takes ~30 min just to make the commands, so if it's piped
# to shuf then nothing will start before 30 min...
#time python 3a_make_conditioning_commands.py --quiet
time python 3a_make_conditioning_commands.py | shuf | parallel -j 76
#time python 3b_make_coloc_commands.py --quiet
time python 3b_make_coloc_commands.py | shuf | parallel -j 76

export PYSPARK_SUBMIT_ARGS="--driver-memory 50g pyspark-shell"
time python 5_combine_results.py
time python 6_process_results.py
time python 8_merge_previous_results.py
