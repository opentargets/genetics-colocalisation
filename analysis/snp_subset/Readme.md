Evaluation of running coloc using a subset of SNPs
==================================================

The coloc pipeline has historically been the slowest step in producing a genetics portal release. The most recent paper from Chris Wallace on her coloc package investigated the performance of coloc when including only a subset of SNPs, filtered based on the Bayes Factor (BF) in each study. That paper used SuSIE for fine-mapping, and the results were better, if anything, using filtered SNPs.

I suspect that the same is true for a standard coloc analysis. Here, I will investigate pre-filtering SNPs to improve the running speed of coloc. We will compare with the same loci computed using full sumstats.

### Filter sumstat significant windows

We will compare a standard run of the coloc pipeline with one where we provide significant windows that are pre-filtered to remove SNPs with p > 0.05.

```
# Get list of significant window files to filter
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/molecular_trait/*.parquet/_SUCCESS" > gcs_input_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/*.parquet/_SUCCESS" >> gcs_input_paths.txt

# Get list of completed files
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_p_gt_0.05/molecular_trait/*.parquet/_SUCCESS" > gcs_completed_paths.txt
gsutil -m ls "gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_p_gt_0.05/gwas/*.parquet/_SUCCESS" >> gcs_completed_paths.txt


# Start a dataproc cluster
# Note that I had this fail multiple times, and had to try adjusting the number
# of executors, memory, cores, etc. to get it to work.
gcloud beta dataproc clusters create \
    js-sumstat-filter \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=25g,spark:spark.executor.memory=76g,spark:spark.executor.cores=8,spark:spark.executor.instances=6 \
    --master-machine-type=n2-highmem-64 \
    --master-boot-disk-size=2TB \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=10m
# Cluster will automatically shutdown after 10 minutes idle


# Queue all, specifying output directory
# (You may want to tmux first if there are thousands of GWAS to run,
# in case it stops submitting part way and you don't know which are
# already queued.)
python queue_all.py --pval 0.05 --out_dir 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_p_gt_0.05'


```

### To monitor job
```
gcloud compute ssh js-sumstat-filter-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-sumstat-filter-m" http://js-sumstat-filter-m:8088
```

### Prepare fine-mapping input data

We will run the coloc pipeline on a subset of coloc tests, using either the full set of significant windows, or filtered significant windows.

```
DATADIR=$HOME/data # for Docker pipeline
mkdir $DATADIR

mkdir -p $DATADIR/ukb_v3_downsampled10k
mkdir -p $DATADIR/filtered/significant_window_2mb/gwas
mkdir -p $DATADIR/filtered/significant_window_2mb/molecular_trait

# Note, need to delete files named "_SUCCESS" from within all parquet folders,
# since the dask dataframe seems to choke on this when reading the parquet.
# Edit: In the latest run this doesn't seem to be necessary?!
find $DATADIR -name "*_SUCCESS" | wc -l
find $DATADIR -name "*_SUCCESS" -delete

mkdir -p $DATADIR/finemapping
gsutil -m cp gs://genetics-portal-dev-staging/finemapping/210923/top_loci.json.gz $DATADIR/finemapping/top_loci.json.gz
gsutil -m cp -r gs://genetics-portal-dev-staging/finemapping/210923/credset $DATADIR/finemapping/

python partition_top_loci_by_chrom.py # Script from fine-mapping pipeline
```

### Run coloc pipeline with FILTERED sumstats

First copy filtered data.

```
gsutil -m rsync gs://open-targets-ukbb/genotypes/ukb_v3_downsampled10k/ $DATADIR/ukb_v3_downsampled10k/
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_p_gt_0.05/molecular_trait/*.parquet $DATADIR/filtered/significant_window_2mb/molecular_trait/
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_p_gt_0.05/gwas/*.parquet $DATADIR/filtered/significant_window_2mb/gwas/
```

Prepare run.

```
cd ~/genetics-colocalisation
docker build --tag otg-coloc .

mkdir -p ~/coloc
mv configs $HOME/

tmux

docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v ~/data:/data \
    -v ~/configs:/configs \
    -v ~/output:/output \
    otg-coloc /bin/bash

NCORES=31

export PYSPARK_SUBMIT_ARGS="--driver-memory 110g pyspark-shell"
time /bin/bash 1_find_overlaps.sh # 10 min last run
time python 2_generate_manifest.py # 

# Take the first 1,000 coloc tests for our comparison.
cp /configs/manifest_unfiltered.json.gz /configs/manifest_unfiltered.all.json.gz
zcat /configs/manifest_unfiltered.all.json.gz | head -n 1000 | gzip > /configs/manifest_unfiltered.json.gz

time python 2b_filter_manifest.py
```

Run main coloc commands. Record the total time taken by each command.

```
#time python 3a_make_conditioning_commands.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.log
time python 3a_make_conditioning_commands.py | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.log

#time python 3b_make_coloc_commands.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.coloc.log
time python 3b_make_coloc_commands.py | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.coloc.log

# IMPORTANT - Check that the output is complete and correct!
# Grep all log files for errors
find /output/logs -name 'log_file.txt' -print0 | xargs -r0 grep -H 'ERROR' | tee errors.txt | wc -l
# Cat all log files
find /output/logs -name "log_file.txt" -exec cat {} \; | less

find /output/logs/coloc -name "log_file.txt" -exec cat {} \; | less
find /output/filtered/output/logs/coloc -name "log_file.txt" -exec cat {} \; | less

# Combine results to make coloc_raw.parquet
time python 5_combine_results.py

mv /output/coloc_raw.parquet /output/coloc_raw_filtered.parquet

# Test is done - remove completed colocs
rm -r /output/cache

mkdir -p /output/filtered
tar -zcf /output/filtered/logs_unfiltered.tar.gz /output/logs

rm -r /output/cache
rm -r /output/logs
rm -r /output/data
rm -r /output/tmp

# Exit docker
exit
```

We're not interested in downstream steps to combine all the results together, since those don't change. (Though they could be optimised in future as well.)

### Run coloc pipeline with FULL sumstats

First copy unfiltered data.

```
# Backup filtered data
mv $HOME/data/filtered/significant_window_2mb $HOME/data/filtered/significant_window_2mb_p_lt_0.05

mkdir -p $HOME/data/filtered/significant_window_2mb/gwas
mkdir -p $HOME/data/filtered/significant_window_2mb/molecular_trait
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/molecular_trait/*.parquet $HOME/data/filtered/significant_window_2mb/molecular_trait/
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/*.parquet $HOME/data/filtered/significant_window_2mb/gwas/
```

Prepare run.

```
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

#cp /configs/manifest_unfiltered.json.gz /configs/manifest_unfiltered.all.json.gz
#zcat /configs/manifest_unfiltered.all.json.gz | head -n 1000 | gzip > /configs/manifest_unfiltered.json.gz

#time python 2b_filter_manifest.py
```

Run main coloc commands. Record the total time taken by each command.

```
#time python 3a_make_conditioning_commands.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.log
time python 3a_make_conditioning_commands.py | shuf | parallel -j $NCORES --bar --joblog /output/parallel.jobs.prep.log

cat /output/coloc_completed_files.tsv | cut -f 27 | sed 's/file:\/\//g' | sed 's/file://g'> /configs/coloc_res_done.txt

#time python 3b_make_coloc_commands.py --quiet
#time python 3b_make_coloc_commands.fix.py --quiet
#time zcat /configs/commands_todo.txt.gz | shuf | parallel -j $NCORES --joblog /output/parallel.jobs.coloc.log
time python 3b_make_coloc_commands.py | shuf | parallel -j $NCORES --joblog /output/parallel.jobs.coloc.log

find /output/filtered/output/logs/coloc -name "log_file.txt" -exec cat {} \; | less

# Combine results to make coloc_raw.parquet
time python 5_combine_results.py

mv /output/coloc_raw.parquet /output/coloc_raw_unfiltered.parquet

# Exit docker
exit
```

### Compare coloc values between runs

```
Rscript compare_coloc_values.R --in_fitered /output/coloc_raw_filtered.parquet --in_unfiltered /output/coloc_raw_unfiltered.parquet

```

