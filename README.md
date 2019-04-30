Colocalisation pipeline
=======================

Work in progress. Preliminary scripts are [here](https://github.com/edm1/coloc_interim).

Note pyarrow rowGroup filter is not due until version [0.13.0](https://issues.apache.org/jira/browse/ARROW-1796). Until then will have to use Dask/fastparquet + glob to read the Spark written parquet files.

Todo:
- Save the overlap table
- Use example data to calculate manifest params to reduce number of tests in final dataset
- Restrict to comparisons within 500kb

### Requirements
- [conda](https://conda.io/docs/)

### Setup environment

```
git clone https://github.com/opentargets/colocalisation.git
cd colocalisation
conda env create -n coloc --file environment.yaml
```

### Edit config files

todo

### Usage

```
# Activate environment
source activate finemap

# Set spark paths
export PYSPARK_SUBMIT_ARGS="--driver-memory 80g pyspark-shell"
export SPARK_HOME=/home/ubuntu/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH

# Find overlaps
todo
```

### Useful commands

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

```
to investigate

{ 'delete_tmpdir': True,
  'left_alt': 'C',
  'left_bio_feature': None,
  'left_chrom': '7',
  'left_ld': '/home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k',
  'left_phenotype': None,
  'left_pos': 75420803,
  'left_ref': 'T',
  'left_study': 'NEALE2_23124_raw',
  'left_sumstat': '/home/ubuntu/data/sumstats/filtered/significant_window_2mb/gwas/NEALE2_23124_raw.parquet',
  'left_type': 'gwas',
  'log': '/home/ubuntu/results/coloc/logs/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_bi
o_feature=UBERON_0000178/right_variant=7_75415552_G_A/log_file.txt',
  'method': 'conditional',
  'min_maf': 0.01,
  'out': '/home/ubuntu/results/coloc/output/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_
bio_feature=UBERON_0000178/right_variant=7_75415552_G_A/coloc_res.json.gz',
  'plot': None,
  'r_coloc_script': '/home/ubuntu/genetics-colocalisation/scripts/coloc.R',
  'right_alt': 'A',
  'right_bio_feature': 'UBERON_0000178',
  'right_chrom': '7',
  'right_ld': '/home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k',
  'right_phenotype': 'ENSG00000135213',
  'right_pos': 75415552,
  'right_ref': 'G',
  'right_study': 'GTEX_v7',
  'right_sumstat': '/home/ubuntu/data/sumstats/filtered/significant_window_2mb/molecular_trait/GTEX_v7.parquet',
  'right_type': 'eqtl',
  'tmpdir': '/home/ubuntu/results/coloc/tmp/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_
bio_feature=UBERON_0000178/right_variant=7_75415552_G_A',
  'top_loci': '/home/ubuntu/results/finemapping/results/top_loci_by_chrom/CHROM.json',
  'window_coloc': 500,
  'window_cond': 2000}
2019-04-30 15:32:52,235 __main__     INFO     Loading left sumstats for 2000kb conditional window
2019-04-30 15:32:55,806 __main__     INFO     Loaded 10140 left variants
2019-04-30 15:32:55,807 __main__     INFO     Loading right sumstats for 2000kb conditional window
/home/ubuntu/miniconda3/envs/finemap/lib/python3.7/site-packages/fastparquet/api.py:214: UserWarning: Partition names coerce to values of different types, e.g. [1, 'X']
  warnings.warn("Partition names coerce to values of different types, e.g. %s" % examples)
2019-04-30 15:34:09,111 __main__     INFO     Loaded 3599 right variants
2019-04-30 15:34:09,111 __main__     INFO     Starting conditional analysis
2019-04-30 15:34:09,112 __main__     INFO      Loading top loci table
2019-04-30 15:34:09,659 __main__     INFO      Left, conditioning 10140 variants on 1 variants
2019-04-30 15:35:15,409 __main__     INFO      Left, finished conditioning, 10139 variants remain
2019-04-30 15:35:15,412 __main__     INFO      Right, no variants to condition on
2019-04-30 15:35:15,412 __main__     INFO     Extracting coloc window (500kb)
2019-04-30 15:35:15,415 __main__     INFO      Left, 2115 variants remain
2019-04-30 15:35:15,420 __main__     INFO      Right, 1848 variants remain
2019-04-30 15:35:15,420 __main__     INFO     Harmonising left and right dfs
2019-04-30 15:35:15,429 __main__     INFO     Left-right intersection contains 1603 variants
2019-04-30 15:35:15,429 __main__     INFO     Running colocalisation
2019-04-30 15:35:19,980 __main__     INFO      H4=0.990 and H3=0.010
2019-04-30 15:35:19,982 __main__     INFO     Time taken: 0:02:27.749830
2019-04-30 15:35:19,982 __main__     INFO     Finished!


Command

python /home/ubuntu/genetics-colocalisation/scripts/coloc_wrapper.py --left_sumstat /home/ubuntu/data/sumstats/filtered/significant_window_2mb/gwas/NEALE2_23124_raw.parquet --left_ld /home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k --left_type gwas --left_study NEALE2_23124_raw --left_phenotype None --left_bio_feature None --left_chrom 7 --left_pos 75420803 --left_ref T --left_alt C --right_sumstat /home/ubuntu/data/sumstats/filtered/significant_window_2mb/molecular_trait/GTEX_v7.parquet --right_ld /home/ubuntu/data/genotypes/ukb_v3_downsampled10k_plink/ukb_v3_chr7.downsampled10k --right_type eqtl --right_study GTEX_v7 --right_phenotype ENSG00000135213 --right_bio_feature UBERON_0000178 --right_chrom 7 --right_pos 75415552 --right_ref G --right_alt A --r_coloc_script /home/ubuntu/genetics-colocalisation/scripts/coloc.R --method conditional --top_loci /home/ubuntu/results/finemapping/results/top_loci_by_chrom/CHROM.json --window_coloc 500 --window_cond 2000 --min_maf 0.01 --out /home/ubuntu/results/coloc/output/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_bio_feature=UBERON_0000178/right_variant=7_75415552_G_A/coloc_res.json.gz --log /home/ubuntu/results/coloc/logs/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_bio_feature=UBERON_0000178/right_variant=7_75415552_G_A/log_file.txt --tmpdir /home/ubuntu/results/coloc/tmp/left_study=NEALE2_23124_raw/left_phenotype=None/left_bio_feature=None/left_variant=7_75420803_T_C/right_study=GTEX_v7/right_phenotype=ENSG00000135213/right_bio_feature=UBERON_0000178/right_variant=7_75415552_G_A
```