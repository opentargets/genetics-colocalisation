Open Targets Genetics colocalisation pipeline
=============================================

Colocalisation pipeline for Open Targets Genetics. In brief:
1. Identify overlaps between credible sets output from [finemapping pipeline](https://github.com/opentargets/genetics-finemapping)
2. Optionally, run conditional analysis
3. Perform colocalisation analysis using *coloc*

Warning: I have not yet implemented a way to remove old results from the manifest file in order to skip them (prevent duplicating the computation), or a way to merge new and old results. This will affect steps 3 and 5 below.

### Requirements
- R
- Spark v2.4.0
- GCTA (>= v1.91.3) must be available in `$PATH`
- [conda](https://conda.io/docs/)
- GNU parallel

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

#### Docker
```bash
# If docker is not installed, then install (on Ubuntu) following steps here.
# (Simplest is to install using the convenience script):
# https://docs.docker.com/engine/install/ubuntu/#install-using-the-convenience-script

# May need to update some permissions to get docker running, e.g.:
# https://stackoverflow.com/questions/56305613/cant-add-user-to-docker-group

# Build the docker environment (takes ~10 min)
docker build --tag otg-coloc .

mv configs $HOME/

# Run in docker container. Can then run steps individually.
docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v $HOME/data:/data \
    -v $HOME/configs:/configs \
    -v $HOME/output:/output \
    otg-coloc /bin/bash

# Run full pipeline in Docker container.
docker run -it --rm \
    --ulimit nofile=1024000:1024000 \
    -v $HOME/data:/data \
    -v $HOME/configs:/configs \
    -v $HOME/output:/output \
    otg-coloc /bin/bash run_coloc_pipeline_opt.sh
```

### Run a single study

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

### Running the pipeline

#### Step 1: Prepare input data

Requires the [same input data as the fine-mapping pipeline](https://github.com/opentargets/genetics-finemapping#step-1-prepare-input-data).

Additionally, it takes the `toploci` and `credibleset` outputs from the finemapping pipeline.
To avoid re-running coloc tests that were computed previously, it takes the "raw" coloc output file from previous runs.
```
DATADIR=$HOME/data # for Docker pipeline
mkdir $DATADIR
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
#gsutil -m rsync gs://open-targets-ukbb/genotypes/ukb_v3_downsampled10k/ $DATADIR/ukb_v3_downsampled10k/
#gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/*/gwas/*.parquet $DATADIR/filtered/significant_window_2mb/gwas/
#gsutil -m cp -r gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb/*/molecular_trait/*.parquet $DATADIR/filtered/significant_window_2mb/molecular_trait/

# Note, need to delete files named "_SUCCESS" from within all parquet folders,
# since the dask dataframe seems to choke on this when reading the parquet.
# Edit: In the latest run this doesn't seem to be necessary?!
find $DATADIR -name "*_SUCCESS" | wc -l
find $DATADIR -name "*_SUCCESS" -delete

mkdir -p $DATADIR/finemapping
gsutil -m cp gs://genetics-portal-dev-staging/finemapping/210923/top_loci.json.gz $DATADIR/finemapping/top_loci.json.gz
gsutil -m cp -r gs://genetics-portal-dev-staging/finemapping/210923/credset $DATADIR/finemapping/

# The previous coloc file is used to avoid repeating coloc tests that were already done.
# The "raw" file is best for this, since the "processed" one could have had some tests removed already.
# But either would work.
gsutil -m cp -r gs://genetics-portal-staging/coloc/190601/coloc_raw.parquet $DATADIR/
gsutil -m cp -r gs://genetics-portal-staging/coloc/190601/coloc_processed.parquet $DATADIR/

python partition_top_loci_by_chrom.py # Script from fine-mapping pipeline
```

#### Step 2: Prepare environment

```
# Activate environment
source activate coloc

# Set spark paths
export PYSPARK_SUBMIT_ARGS="--driver-memory 80g pyspark-shell"
export SPARK_HOME=/home/ubuntu/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
```

#### Step 3: Make manifest file

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

The manifest file can be automatically generated based on the overlapping credible sets using:

```
# If running Docker, then run commands in file docker_cmd.sh, else do the below

# Edit args in `1_find_overlaps.sh`, then
bash 1_find_overlaps.sh

# Generate the manifest from the overlap table
python 2_generate_manifest.py

# Remove lines which are already in output from previous coloc runs
python 2b_filter_manifest.py
```
#### Step 4: Run pipeline

```
# Edit args in `4_run_commands.sh` then
tmux
time bash 4_run_commands.sh

# Exit tmux with Ctrl+b then d
```

The above command will run all analyses specified in the manifest using GNU parallel. It will create two files `commands_todo.txt.gz` and `commands_done.txt.gz` showing which analyses have not yet/already been done. The pipeline can be stopped at any time and restarted without repeating any completed analyses. You can safely regenerate the `commands_*.txt.gz` commands whilst the pipeline is running using `python 3_make_commands.py --quiet`.

Warning: When I ran this for 1.7 million comparisons I ran out of inodes towards the end, causing the remaining comparisons to fail. Need to think about how to remedy this.

Each comparison took on average ~70 seconds. On 60 cores, this completes in about 1 month. This pipeline will need scaling out, or optimising in the future if the scale of the data drastically increases.

#### Step 5: Process the results

```
# Combine the results of all the individual analyses
# This step can be slow/inefficient due to Hadoop many small files problem
export PYSPARK_SUBMIT_ARGS="--driver-memory 50g pyspark-shell"
time python 5_combine_results.py

# Process the results for exporting. Renames or computes a few columns,
# e.g. coloc_h4_h3 ratio, filters based on number of overlapping vars,
# makes symmetric coloc result matrix.
python 6_process_results.py

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
# of executors, memory, cores, etc. to get it to work.
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
find output -name "*.json" | wc -l

# Parse time taken for each run
find /output/logs -name "log_file.txt" -exec grep "Time taken" {} \;

# Parse time taken to load right sumstats
find /output/logs -name "log_file.txt" -exec "Loading right" -A 1 {} \;

# Check how many commands still "to do"
time python 3a_make_conditioning_commands.py --quiet
zcat /configs/commands_todo.txt.gz | wc -l

# Grep all log files
find /output/logs -name 'log_file.txt' -print0 | xargs -r0 grep -H 'ERROR' | tee errors.txt | wc -l
find /output/logs -name 'log_file.txt' -print0 | xargs -r0 grep -H 'ERROR' | grep -v 'no intersection'

# (OLD) Grep all log files
ls -rt logs/left_study\=*/left_phenotype\=*/left_bio_feature\=*/left_variant\=*/right_study\=*/right_phenotype\=*/right_bio_feature\=*/right_variant\=*/log_file.txt | xargs grep "Time taken"

# Cat all log files
find /output/logs -name "log_file.txt" -exec cat {} \; | less
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
    othercount_coloc_rows.py
```
