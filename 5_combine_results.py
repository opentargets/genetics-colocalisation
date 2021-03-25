#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Combines the outputs from the coloc pipeline into a single file
#

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
# from shutil import copyfile
# from glob import glob

def main():

    # Make spark session
    # Using `ignoreCorruptFiles` will skip empty files
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.master", "local[*]")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # # Set logging level
    # sc = spark.sparkContext
    # sc.setLogLevel('INFO')

    # Args
    in_res_pattern = '/home/js29/genetics-colocalisation/results/coloc/output/left_study=*/left_phenotype=*/left_bio_feature=*/left_variant=*/right_study=*/right_phenotype=*/right_bio_feature=*/right_variant=*/coloc_res.json.gz'
    # in_res_pattern = '/home/js29/genetics-colocalisation/results/coloc/output/left_study=GCST*/left_phenotype=*/left_bio_feature=*/left_variant=*/right_study=AL*/right_phenotype=*/right_bio_feature=*/right_variant=*/coloc_res.json.gz'
    out_coloc = '/home/js29/genetics-colocalisation/results/coloc/results/coloc_raw.parquet'

    # Load
    df = spark.read.json(in_res_pattern)

    # Repartition
    # df = (
    #     df.repartitionByRange('left_chrom', 'left_pos')
    #     .sortWithinPartitions('left_chrom', 'left_pos')
    # )

    # Coalesce
    df = df.coalesce(2000)

    # Write
    (
        df
        .write
        .parquet(
            out_coloc,
            compression='snappy',
            mode='overwrite'
        )
    )

    return 0

if __name__ == '__main__':

    main()
