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

```
