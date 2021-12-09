#!/usr/bin/env bash
#

version_date=`date +%y%m%d`

# Copy current results
gsutil -m cp -r ~/output/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/$version_date/
gsutil -m cp -r ~/output/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/$version_date/

# Copy overlap table
gsutil -m cp -r ~/output/overlap_table gs://genetics-portal-dev-staging/coloc/$version_date/overlap_table

# Tar the logs and copy over
tar -zcvf logs.tar.gz ~/output/logs
gsutil -m cp logs.tar.gz gs://genetics-portal-dev-staging/coloc/$version_date/logs.tar.gz

# # Tar the plots and copy over
# tar -zcvf plots.tar.gz plots
# gsutil -m cp plots.tar.gz gs://genetics-portal-staging/coloc/$version_date/plots.tar.gz

# Make a note as to what this coloc run contained. E.g.:
echo "V6 OT Genetics release: updated coloc results for all eQTL catalogue + recomputed for Sun et al and eQTLGen vs. all previous GWAS plus 450 new GWAS catalog studies" > README.txt
gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/$version_date/README.txt

if [ -d "~/output/merged" ]; then
    # Remove unneeded .part*.crc files that cause problems for downstream pipeline steps
    sudo rm -f ~/output/merged/coloc_raw.parquet/.*.crc
    sudo rm -f  ~/output/merged/coloc_processed.parquet/.*.crc

    # Copy merged results to GCS
    gsutil -m cp -r ~/output/merged/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_raw.parquet
    gsutil -m cp -r ~/output/merged/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_processed.parquet

    echo "V6 OT Genetics release: updated coloc results for all eQTL catalogue + recomputed for Sun et al and eQTLGen vs. all previous GWAS plus 450 new GWAS catalog studies" > README.txt
    gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/${version_date}_merged/README.txt
fi
