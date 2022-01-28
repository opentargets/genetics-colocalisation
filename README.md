Open Targets Genetics colocalisation pipeline
=============================================

Colocalisation pipeline for Open Targets Genetics. In brief:
1. Identify overlaps between credible sets output from [finemapping pipeline](https://github.com/opentargets/genetics-finemapping)
2. Optionally, run conditional analysis
3. Perform colocalisation analysis using *coloc*

This pipeline runs using Docker.

### Requirements
- R
- Spark v2.4.0
- GCTA (>= v1.91.3) must be available in `$PATH`
- [conda](https://conda.io/docs/)
- GNU parallel


### Running the pipeline

#### Step 1: Prepare docker
```bash
# If docker is not installed, then install (on Ubuntu) following steps here.
# (Simplest is to install using the convenience script):
# https://docs.docker.com/engine/install/ubuntu/#install-using-the-convenience-script

# May need to update some permissions to get docker running, e.g.:
# https://stackoverflow.com/questions/56305613/cant-add-user-to-docker-group

# May need to fix some config issues with Docker
sudo groupadd docker
sudo usermod -aG docker ${USER}
sudo systemctl restart docker
# Exit VM and re-login

# Build the docker environment (takes ~10 min)
docker build --tag otg-coloc .

# Pipeline accesses config files in an external location so that we can monitor
# it from outside the docker instance. Output is also written to ~/output.
mv configs $HOME/
```


#### Step 2: Prepare input data

Requires the [same input data as the fine-mapping pipeline](https://github.com/opentargets/genetics-finemapping#step-1-prepare-input-data).

Additionally, it takes the `toploci` and `credibleset` outputs from the finemapping pipeline.
To avoid re-running coloc tests that were computed previously, it takes the "raw" coloc output file from previous runs.
```
DATADIR=$HOME/data # for Docker pipeline
mkdir -p $DATADIR
#DATADIR=$HOME/genetics-colocalisation/data # for original pipeline

mkdir -p $DATADIR/ukb_v3_downsampled10k
mkdir -p $DATADIR/filtered/significant_window_2mb/gwas
mkdir -p $DATADIR/filtered/significant_window_2mb/molecular_trait

# If on same machine as fine-mapping pipeline, then the following:
mv $HOME/genetics-finemapping/data/ukb_v3_downsampled10k $DATADIR/ukb_v3_downsampled10k

for f in ~/genetics-finemapping/data/filtered/significant_window_2mb/*/gwas/*.parquet; do
  fname=`basename $f`
  echo $DATADIR/filtered/significant_window_2mb/gwas/$fname
  mv $f $DATADIR/filtered/significant_window_2mb/gwas/$fname
done

for f in ~/genetics-finemapping/data/filtered/significant_window_2mb/*/molecular_trait/*.parquet; do
  fname=`basename $f`
  echo $DATADIR/filtered/significant_window_2mb/molecular_trait/$fname
  mv $f $DATADIR/filtered/significant_window_2mb/molecular_trait/$fname
done


# Otherwise, this:
gsutil -m rsync gs://open-targets-ukbb/genotypes/ukb_v3_downsampled10k/ $DATADIR/ukb_v3_downsampled10k/
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/gwas/*.parquet $DATADIR/filtered/significant_window_2mb/gwas/
gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/molecular_trait/*.parquet $DATADIR/filtered/significant_window_2mb/molecular_trait/

# Note, need to delete files named "_SUCCESS" from within all parquet folders,
# since the dask dataframe seems to choke on this when reading the parquet.
# Edit: In the latest run this doesn't seem to be necessary?!
find $DATADIR -name "*_SUCCESS" | wc -l
find $DATADIR -name "*_SUCCESS" -delete

# Update to the latest fine-mapping path before running
mkdir -p $DATADIR/finemapping
gsutil -m cp gs://genetics-portal-dev-staging/finemapping/211221_merged/top_loci.json.gz $DATADIR/finemapping/top_loci.json.gz
gsutil -m cp -r gs://genetics-portal-dev-staging/finemapping/211221_merged/credset $DATADIR/finemapping/

# Update to the path to the previous coloc release before running
# The previous coloc file is used to avoid repeating coloc tests that were already done.
# The "raw" file is best for this, since the "processed" one could have had some tests removed already.
# But either would work.
gsutil -m cp -r gs://genetics-portal-dev-staging/coloc/211215_merged/coloc_raw.parquet $DATADIR/
gsutil -m cp -r gs://genetics-portal-dev-staging/coloc/211215_merged/coloc_processed.parquet $DATADIR/

# If you are filtering out coloc tests that were previously done (not re-running them)
# then make sure that the path to the coloc_raw.parquet file is specified in config.yaml.
```

After downloading the data, we need to split the top loci by chromosome, and subset the reference panels for optimising performance. These are done within the docker instance.

```
tmux

#docker build --tag otg-coloc .

# Run docker container. Can then run steps in run_coloc_pipeline_opt.sh individually.
docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v $HOME/data:/data \
    -v $HOME/configs:/configs \
    -v $HOME/output:/output \
    otg-coloc /bin/bash

python partition_top_loci_by_chrom.py # Script from fine-mapping pipeline

# To optimise the GCTA conditioning step, it helps to split the UKB reference panel
# into smaller, overlapping "sub-panels". This is because (I believe) GCTA loads
# in the index for the whole chromosome (*.bim file) to determine which SNPs match.
time python 0_split_ld_reference.py --path /data/ukb_v3_downsampled10k/ukb_v3_chr{chrom}.downsampled10k
```

#### Step 3: Run pipeline

Start docker as above, and then either manually run individual commands in run_coloc_pipeline_opt.sh, or run the complete script.

```
# Set number of cores available to use, and pyspark args
# Last run on full dataset (~4 M colocs) took 28 hrs on a 224-core machine.
NCORES=222
PYSPARK_SUBMIT_ARGS="--driver-memory 400g pyspark-shell"

# Run the full pipeline (or alternatively, run individual commands from this script)
dt=`date '+%Y_%m_%d.%H_%M'`
time bash run_coloc_pipeline_opt.sh $NCORES "$PYSPARK_SUBMIT_ARGS" | tee /output/pipeline_run.$dt.txt

# Exit tmux with Ctrl+b then d
```

#### Step 4: Monitor running pipeline

```
# Check how many jobs have completed
wc -l /output/parallel.jobs.cond.log
wc -l /output/parallel.jobs.coloc.log

# Estimate GCTA conditioning time remaining
cat $HOME/output/parallel.jobs.cond.log | wc -l
JOBS_DONE=`cat $HOME/output/parallel.jobs.cond.log | wc -l`
JOBS_TOTAL=`zcat $HOME/configs/commands_todo.cond.txt.gz | wc -l`
TIME_START=`head -n 2 $HOME/output/parallel.jobs.cond.log | cut -f 3 | tail -n 1`
TIME_END=`tail -n 1 $HOME/output/parallel.jobs.cond.log | cut -f 3`
PCT_DONE=`echo "scale=3; 100 * $JOBS_DONE / $JOBS_TOTAL" | bc`
echo "$PCT_DONE% done"
MIN_LEFT=`echo "scale=3; ($TIME_END - $TIME_START) * ((100.0 / $PCT_DONE) - 1) / 60" | bc`
echo "$MIN_LEFT min left"

# Check how many coloc jobs have completed
cat $HOME/output/parallel.jobs.coloc.log | wc -l
JOBS_DONE=`cat $HOME/output/parallel.jobs.coloc_opt.log | wc -l`
JOBS_TOTAL=`zcat $HOME/configs/commands_todo.txt.gz | wc -l`
TIME_START=`head -n 2 $HOME/output/parallel.jobs.coloc_opt.log | cut -f 3 | tail -n 1`
TIME_END=`tail -n 1 $HOME/output/parallel.jobs.coloc_opt.log | cut -f 3`
PCT_DONE=`echo "scale=3; 100 * 1000 * $JOBS_DONE / $JOBS_TOTAL" | bc`
echo "$PCT_DONE% done"
MIN_LEFT=`echo "scale=3; ($TIME_END - $TIME_START) * ((100.0 / $PCT_DONE) - 1) / 60" | bc`
echo "$MIN_LEFT min left"

```

#### Step 5: Merge previous results

```
python 7_merge_previous_results.py

```

#### Step 6: Copy to GCS
```
bash 8_copy_results_to_gcs.sh
```


#### Step 7: Join the summary stats onto the coloc table

This step was added at a later date to join the summary stats onto the coloc results table. This is done in the pipeline rather than in the database as the sumstat database lives on a different machine from the coloc table.

This step requires the summary stats to be concatenated into a single parquet dataset and partitioned by chrom, pos. Script to do this is available [here](https://github.com/opentargets/genetics-sumstat-data/blob/master/filters/significant_window_extraction/union_and_repartition_into_single_dataset.py).

To run on google dataproc: (last run took XX hrs)

```
# Open join_results_with_betas.py and specify file arguments

# Start a dataproc cluster
# Note that I had this fail multiple times, and had to try adjusting the number
# of executors, memory, cores, etc. to get it to work. More memory seems to be key.
# Took ~30 min on last run, n2-highmem-64
gcloud beta dataproc clusters create \
    js-coloc-beta-join \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100,spark:spark.driver.memory=25g,spark:spark.executor.memory=76g,spark:spark.executor.cores=8,spark:spark.executor.instances=6 \
    --master-machine-type=n2-highmem-64 \
    --master-boot-disk-size=2TB \
    --num-master-local-ssds=8 \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=10m
# Cluster will automatically shutdown after 10 minutes idle

# Submit to dataproc
gcloud dataproc jobs submit pyspark \
    --cluster=js-coloc-beta-join \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    join_results_with_betas.py
```

# To monitor job
```
gcloud compute ssh js-coloc-beta-join-m \
  --project=open-targets-genetics-dev \
  --zone=europe-west1-d -- -D 1080 -N

"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --user-data-dir="/tmp/js-coloc-beta-join-m" http://js-coloc-beta-join-m:8088
```

### Other

##### Useful commands

```
# Count finished
find output/data -name "*.coloc_res.csv" | wc -l

# Check how many conditional sumstats had no variants to condition on
find $HOME/output/logs/extract_sumstats -name "log_file.txt" -exec grep "No variants to condition on" {} \;

# Parse time taken for each run
find $HOME/output/logs -name "log_file.txt" -exec grep "Time taken" {} \;

# Parse time taken to load right sumstats
find $HOME/output/logs -name "log_file.txt" -exec "Loading right" -A 1 {} \;

# Check how many commands still "to do"
time python 3a_make_conditioning_commands.py --quiet
zcat $HOME/configs/commands_todo.txt.gz | wc -l

# Grep all log files
find $HOME/output/logs/coloc -name 'log_file.txt' -print0 | xargs -r0 grep -H 'ERROR' | tee errors.txt | wc -l
find $HOME/output/logs -name 'log_file.txt' -print0 | xargs -r0 grep -H 'ERROR' | grep -v 'no intersection'

# (OLD) Grep all log files
ls -rt logs/left_study\=*/left_phenotype\=*/left_bio_feature\=*/left_variant\=*/right_study\=*/right_phenotype\=*/right_bio_feature\=*/right_variant\=*/log_file.txt | xargs grep "Time taken"

# Cat all log files
find $HOME/output/chr21_run3/logs/extract_sumstats -name "log_file.txt" -exec cat {} \; | less
find $HOME/output/chr21_run3/logs/extract_sumstats -name "log_file.txt" -exec cat {} \; | grep '/output/cache/gwas/GCST90002334/None/None/21/38479032/C/A/sumstat.tsv' | less

find $HOMEoutput/cache -name "sumstat.tsv.gz" -exec ls -l {} \; | less

find $HOME/output/logs/extract_sumstats -name "log_file.txt" -exec cat {} \; | less
find $HOME/output/logs/coloc -name "log_file.txt" -exec cat {} \; | grep -i "error" | less
```

##### Miscellaneous

```
gcloud beta dataproc clusters create \
    js-coloc-beta-join \
    --image-version=preview \
    --properties=spark:spark.debug.maxToStringFields=100 \
    --master-machine-type=n2-highmem-8 \
    --master-boot-disk-size=1TB \
    --zone=europe-west1-d \
    --initialization-action-timeout=20m \
    --single-node \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    --max-idle=10m

gcloud dataproc jobs submit pyspark \
    --cluster=js-coloc-beta-join \
    --async \
    --properties spark.submit.deployMode=cluster \
    --project=open-targets-genetics-dev \
    --region=europe-west1 \
    other/count_coloc_rows.py
```

### OLD stuff
### Setup environment

#### Local
```
git clone https://github.com/opentargets/genetics-colocalisation.git
cd genetics-colocalisation
bash setup.sh
# The last time I ran this it would hang at "solving environment..."
# I got around this by creating the env and then manually installing
# each package with conda install <name>
# Offending packages is probably r-coloc. Workaround is to open R
# and manually install coloc.
conda env create -n coloc --file environment.yaml
```

#### Run a single study (OLD)

```
# Activate environment
source activate coloc

# View args
$ python scripts/coloc_wrapper.py --help
usage: coloc_wrapper.py [-h] --left_sumstat <file> --left_type <str>
                        --left_study <str> [--left_phenotype <str>]
                        [--left_bio_feature <str>] --left_chrom <str>
                        --left_pos <int> [--left_ref <str>] [--left_alt <str>]
                        [--left_ld <str>] --right_sumstat <file> --right_type
                        <str> --right_study <str> [--right_phenotype <str>]
                        [--right_bio_feature <str>] --right_chrom <str>
                        --right_pos <int> [--right_ref <str>]
                        [--right_alt <str>] [--right_ld <str>] --method
                        {conditional,distance} --window_coloc <int>
                        --window_cond <int> [--min_maf <float>]
                        --r_coloc_script <str> [--delete_tmpdir]
                        [--top_loci <str>] --out <file> [--plot <file>] --log
                        <file> --tmpdir <file>

optional arguments:
  -h, --help            show this help message and exit
  --left_sumstat <file>
                        Input: left summary stats parquet file
  --left_type <str>     Left type
  --left_study <str>    Left study_id
  --left_phenotype <str>
                        Left phenotype_id
  --left_bio_feature <str>
                        Left bio_feature
  --left_chrom <str>    Left chromomsome
  --left_pos <int>      Left position
  --left_ref <str>      Left ref allele
  --left_alt <str>      Left alt allele
  --left_ld <str>       Left LD plink reference
  --right_sumstat <file>
                        Input: Right summary stats parquet file
  --right_type <str>    Right type
  --right_study <str>   Right study_id
  --right_phenotype <str>
                        Right phenotype_id
  --right_bio_feature <str>
                        Right bio_feature
  --right_chrom <str>   Right chromomsome
  --right_pos <int>     Right position
  --right_ref <str>     Right ref allele
  --right_alt <str>     Right alt allele
  --right_ld <str>      Right LD plink reference
  --method {conditional,distance}
                        Which method to run (i) conditional analysis or (ii)
                        distance based without conditional
  --window_coloc <int>  Plus/minus window (kb) to perform coloc on
  --window_cond <int>   Plus/minus window (kb) to perform conditional analysis
                        on
  --min_maf <float>     Minimum minor allele frequency to be included
  --r_coloc_script <str>
                        R script that implements coloc
  --delete_tmpdir       Remove temp dir when complete
  --top_loci <str>      Input: Top loci table (required for conditional
                        analysis)
  --out <file>          Output: Coloc results
  --plot <file>         Output: Plot of colocalisation
  --log <file>          Output: log file
  --tmpdir <file>       Output: temp dir
```

#### Step 2 (OLD): Prepare environment

```
# Activate environment
source activate coloc

# Set spark paths
export PYSPARK_SUBMIT_ARGS="--driver-memory 80g pyspark-shell"
export SPARK_HOME=/home/ubuntu/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
```

#### Manifest file details

The manifest file specifies all analyses to be run. The manifest is a JSON lines file with each line containing the following fields:

```json
{
  "left_sumstats": "/home/ubuntu/data/sumstats/filtered/significant_window_2mb/gwas/NEALE2_30530_raw.parquet",
  "left_ld": "/home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k",
  "left_study_id": "NEALE2_30530_raw",
  "left_type": "gwas",
  "left_phenotype_id": null,
  "left_bio_feature": null,
  "left_lead_chrom": "7",
  "left_lead_pos": 73627972,
  "left_lead_ref": "GCTTT",
  "left_lead_alt": "G",
  "right_sumstats": "/home/ubuntu/data/sumstats/filtered/significant_window_2mb/molecular_trait/ALASOO_2018.parquet",
  "right_ld": "/home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k",
  "right_study_id": "ALASOO_2018",
  "right_type": "eqtl",
  "right_phenotype_id": "ENSG00000071462",
  "right_bio_feature": "MACROPHAGE_SALMONELLA",
  "right_lead_chrom": "7",
  "right_lead_pos": 73676792,
  "right_lead_ref": "C",
  "right_lead_alt": "T",
  "method": "conditional",
  "out": "/home/ubuntu/results/coloc/output/left_study=NEALE2_30530_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_73627972_GCTTT_G/right_study=ALASOO_2018/right_phenotype=ENSG00000071462/right_bio_feature=MACROPHAGE_SALMONELLA/right_variant=7_73676792_C_T/coloc_res.json.gz",
  "log": "/home/ubuntu/results/coloc/logs/left_study=NEALE2_30530_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_73627972_GCTTT_G/right_study=ALASOO_2018/right_phenotype=ENSG00000071462/right_bio_feature=MACROPHAGE_SALMONELLA/right_variant=7_73676792_C_T/log_file.txt",
  "tmpdir": "/home/ubuntu/results/coloc/tmp/left_study=NEALE2_30530_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_73627972_GCTTT_G/right_study=ALASOO_2018/right_phenotype=ENSG00000071462/right_bio_feature=MACROPHAGE_SALMONELLA/right_variant=7_73676792_C_T",
  "plot": "/home/ubuntu/results/coloc/plot/NEALE2_30530_raw_None_None_7_73627972_GCTTT_G_ALASOO_2018_ENSG00000071462_MACROPHAGE_SALMONELLA_7_73676792_C_T.png"
}
```