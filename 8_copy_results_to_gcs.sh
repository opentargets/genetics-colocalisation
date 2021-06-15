#!/usr/bin/env bash
#

version_date=`date +%y%m%d`

# Copy current results
#gsutil -m rsync -r ~/genetics-colocalisation/results/coloc/results gs://genetics-portal-dev-staging/coloc/$version_date
gsutil -m cp -r ~/output/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/$version_date/coloc_raw.parquet
gsutil -m cp -r ~/output/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/$version_date/coloc_processed.parquet

# Copy overlap table
#gsutil -m cp -r ~/genetics-colocalisation/results/coloc/overlap_table gs://genetics-portal-dev-staging/coloc/$version_date/overlap_table
gsutil -m cp -r ~/output/overlap_table gs://genetics-portal-dev-staging/coloc/$version_date/overlap_table

# Tar the logs and copy over
tar -zcvf logs.tar.gz ~/output/logs
gsutil -m cp logs.tar.gz gs://genetics-portal-dev-staging/coloc/$version_date/logs.tar.gz

# # Tar the plots and copy over
# tar -zcvf plots.tar.gz plots
# gsutil -m cp plots.tar.gz gs://genetics-portal-staging/coloc/$version_date/plots.tar.gz

# Make a note as to what this coloc run contained. E.g.:
echo "V4 OT Genetics release: 213 GWAS catalog + 4 Covid R4 GWAS + T1D prepublished + FinnGen R5" > README.txt
gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/$version_date/README.txt

# Copy merged results to GCS
gsutil -m cp -r ~/output/merged/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_raw.parquet
gsutil -m cp -r ~/output/merged/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_processed.parquet

echo "V4 OT Genetics release: merged 190612 release + new coloc results (213 GWAS catalog + 4 Covid R4 GWAS + T1D prepublished + FinnGen R5)" > README.txt
gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/${version_date}_merged/README.txt
