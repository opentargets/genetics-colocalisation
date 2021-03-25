#!/usr/bin/env bash
#

version_date=`date +%y%m%d`

# Copy results
gsutil -m rsync -r /home/js29/genetics-colocalisation/results/coloc/results gs://genetics-portal-staging/coloc/$version_date

# Copy overlap table
gsutil -m rsync /home/ubuntu/results/coloc/overlap_table gs://genetics-portal-staging/coloc/$version_date/overlap_table

# Tar the logs and copy over
tar -zcvf logs.tar.gz /home/ubuntu/results/coloc/logs
gsutil -m cp logs.tar.gz gs://genetics-portal-staging/coloc/$version_date/logs.tar.gz

# # Tar the plots and copy over
# tar -zcvf plots.tar.gz plots
# gsutil -m cp plots.tar.gz gs://genetics-portal-staging/coloc/$version_date/plots.tar.gz
