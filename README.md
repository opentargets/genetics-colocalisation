Open Targets Genetics colocalisation pipeline
=============================================

Colocalisation pipeline for Open Targets Genetics. In brief:
1. Identify overlaps between credible sets output from [finemapping pipeline](https://github.com/opentargets/genetics-finemapping)
2. Optionally, run conditional analysis
3. Perform colocalisation analysis using *coloc*

### Requirements
- R
- Spark v2.4.0
- GCTA (>= v1.91.3) must be available in `$PATH`
- [conda](https://conda.io/docs/)
- GNU parallel

### Setup environment

```
git clone https://github.com/opentargets/colocalisation.git
cd colocalisation
bash setup.sh
conda env create -n coloc --file environment.yaml

# Open R and install coloc and tidyverse packages
source("https://bioconductor.org/biocLite.R")
biocLite("snpStats")
install.packages('coloc')
install.packages("tidyverse")
```

### Run a single study

```
# Activate environment
source activate finemap

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
                        distance based with conditional
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

#### Step 2: Prepare environment

```
# Activate environment
source activate finemap

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
# Edit args in `1_find_overlaps.sh`, then
bash 1_find_overlaps.sh

# Generate the manifest from the overlap table
python 2_generate_manifest.py
```
#### Step 4: Run pipeline

```
# Edit args in `4_run_commands.sh` then
tmux
bash 4_run_commands.sh

# Exit tmux with Ctrl+b then d
```

The above command will run all analyses specified in the manifest using GNU parallel. It will create two files `commands_todo.txt.gz` and `commands_done.txt.gz` showing which analyses have not yet/already been done. The pipeline can be stopped at any time and restarted without repeating any completed analyses. You can safely regenerate the `commands_*.txt.gz` commands whilst the pipeline is running using `python 3_make_commands.py --quiet`.

#### Step 5: Process the results

```
# Combine the results of all the individual analyses
# This step can be slow/inefficient due to Hadoop many small files problem
python 5_combine_results.py

# Process the results for exporting
python 6_process_results.py

# Copy results to GCS
bash 7_copy_results_to_gcs.sh
```

### Other

##### Useful commands

```
# Count finished
find output -name "*.json" | wc -l

# Parse time taken for each run
find logs -name "log_file.txt" -exec grep "Time taken" {} \;

# Parse time taken to load right sumstats
find logs -name "log_file.txt" -exec "Loading right" -A 1 {} \;

# Grep all log files
ls -rt logs/left_study\=*/left_phenotype\=*/left_bio_feature\=*/left_variant\=*/right_study\=*/right_phenotype\=*/right_bio_feature\=*/right_variant\=*/log_file.txt | xargs grep "Time taken"
```