#!/usr/bin/env bash
#

version_date=`date +%y%m%d`

# Copy current results
gsutil -m cp -r $HOME/output/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/$version_date/
gsutil -m cp -r $HOME/output/coloc_raw.csv.gz gs://genetics-portal-dev-staging/coloc/$version_date/
gsutil -m cp -r $HOME/output/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/$version_date/

# Copy overlap table
gsutil -m cp -r $HOME/output/overlap_table gs://genetics-portal-dev-staging/coloc/$version_date/overlap_table

# Make a note as to what this coloc run contained. E.g.:
echo "Recomputed from scratch: Data for Genetics portal R8, with GTEx-sQTL added and FinnGen R6" > README.txt
gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/$version_date/README.txt

if [ -d "$HOME/output/merged" ]; then
    # Remove unneeded .part*.crc files that cause problems for downstream pipeline steps
    sudo rm -f $HOME/output/merged/coloc_raw.parquet/.*.crc
    sudo rm -f  $HOME/output/merged/coloc_processed.parquet/.*.crc

    # Copy merged results to GCS
    gsutil -m cp -r $HOME/output/merged/coloc_raw.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_raw.parquet
    gsutil -m cp -r $HOME/output/merged/coloc_processed.parquet gs://genetics-portal-dev-staging/coloc/${version_date}_merged/coloc_processed.parquet

    echo "Merged colocs from Genetics portal R6, with 6 pQTL studies added and 653 new GWAS to make Genetics portal R7" > README.txt
    gsutil cp README.txt gs://genetics-portal-dev-staging/coloc/${version_date}_merged/README.txt
fi

# Tar the logs and copy over
# (NOTE: For a large run this takes too long to be worthwhile - e.g. days
# just to tar the files.)
#tar -zcvf logs.tar.gz ~/output/logs
#gsutil -m cp logs.tar.gz gs://genetics-portal-dev-staging/coloc/$version_date/logs.tar.gz

# # Tar the plots and copy over
# tar -zcvf plots.tar.gz plots
# gsutil -m cp plots.tar.gz gs://genetics-portal-staging/coloc/$version_date/plots.tar.gz
