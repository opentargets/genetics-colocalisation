#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
'''
Merges coloc results from previous release(s) with new results.
This should be done prior to joining to get betas from sumstats.
'''

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''
import gzip
from glob import glob

import pyspark.sql
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():

    # Make spark session
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.master", "local[*]")
        .getOrCreate()
    )
    # sc = spark.sparkContext
    print('Spark version: ', spark.version)

    # File args
    in_raw_previous = '/data/coloc_raw.parquet'
    in_processed_previous = '/data/coloc_processed.parquet'
    in_raw_new = '/output/coloc_raw.parquet'
    in_processed_new = '/output/coloc_processed.parquet'

    out_raw = '/output/merged/coloc_raw.parquet'
    out_processed = '/output/merged/coloc_processed.parquet'

    # Load
    raw_prev = spark.read.parquet(in_raw_previous) #.limit(100)
    print('Loaded {} previous coloc tests'.format( raw_prev.count() ))

    raw_new = spark.read.parquet(in_raw_new) #.limit(100)
    print('Loaded {} new coloc tests'.format( raw_new.count() ))

    raw_merged = raw_prev.unionByName(raw_new, allowMissingColumns=True)
    print('Writing {} total coloc tests'.format( raw_merged.count() ))

    prev_only_cols = [x for x in raw_prev.columns if x not in raw_new.columns]
    if len(prev_only_cols) > 0:
        print("WARNING: columns present only in previous coloc dataset:")
        print(prev_only_cols)
    new_only_cols = [x for x in raw_new.columns if x not in raw_prev.columns]
    if len(new_only_cols) > 0:
        print("WARNING: columns present only in new coloc dataset:")
        print(new_only_cols)

    # Repartition
    raw_merged = (
        raw_merged.repartitionByRange('left_chrom', 'left_pos')
        .sortWithinPartitions('left_chrom', 'left_pos')
    )

    # Write
    (
        raw_merged
        .write.parquet(
            out_raw,
            mode='overwrite'
        )
    )

    # Do the same for processed coloc results
    processed_prev = spark.read.parquet(in_processed_previous) #.limit(100)
    print('Loaded {} previous coloc tests'.format( processed_prev.count() ))

    processed_new = spark.read.parquet(in_processed_new) #.limit(100)
    print('Loaded {} new coloc tests'.format( processed_new.count() ))

    processed_merged = processed_prev.unionByName(processed_new)
    print('Writing {} total coloc tests'.format( processed_merged.count() ))

    # Repartition
    processed_merged = (
        processed_merged.repartitionByRange('left_chrom', 'left_pos')
        .sortWithinPartitions('left_chrom', 'left_pos')
    )

    # Write
    (
        processed_merged
        .write.parquet(
            out_processed,
            mode='overwrite'
        )
    )

    return 0


if __name__ == '__main__':

    main()
