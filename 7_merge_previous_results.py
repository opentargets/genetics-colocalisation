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

    # For removing duplicates below
    col_subset_for_duplicates = ['left_study', 'left_type', 'left_chrom', 'left_pos', 'left_ref', 'left_alt',
                                 'right_study', 'right_type', 'right_phenotype', 'right_bio_feature',
                                 'right_chrom', 'right_pos', 'right_ref', 'right_alt']
    # Load
    raw_prev = spark.read.parquet(in_raw_previous)
    prev_count = raw_prev.count()
    print('Loaded {} previous coloc tests'.format(prev_count))
    raw_prev = raw_prev.dropDuplicates(subset=col_subset_for_duplicates)
    print('{} records dropped as duplicates'.format(prev_count - raw_prev.count()))
    prev_count = raw_prev.count()

    raw_new = spark.read.parquet(in_raw_new)
    new_count = raw_new.count()
    print('Loaded {} new coloc tests'.format(new_count))
    raw_new = raw_new.dropDuplicates(subset=col_subset_for_duplicates)
    print('{} records dropped as duplicates'.format(new_count - raw_new.count()))
    new_count = raw_new.count()

    missing = raw_new.join(raw_prev, on=col_subset_for_duplicates, how="leftanti")
    missing.toPandas().to_csv('/output/coloc_raw.missing.csv')

    present = raw_prev.join(raw_new, on=col_subset_for_duplicates, how="leftsemi")
    present.toPandas().to_csv('/output/coloc_raw.present.csv')

    overlap = (
        raw_prev
        .withColumnRenamed('PP.H4.abf', 'prev_coloc_h4')
        .join(raw_new.withColumnRenamed('PP.H4.abf', 'new_coloc_h4')
            .select(col_subset_for_duplicates + ['new_coloc_h4']),
                on=col_subset_for_duplicates, how="inner")
    )
    overlap.toPandas().to_csv('/output/coloc_raw.merged.csv')

    raw_merged = raw_new.unionByName(raw_prev, allowMissingColumns=True)
    # Remove duplicates - i.e. keep the first row for a given coloc test,
    # which should come from the new dataset
    raw_merged = raw_merged.dropDuplicates(subset=col_subset_for_duplicates)
    merged_count = raw_merged.count()
    print('Writing {} total coloc tests ({} records overwritten by new colocs)'.format(merged_count, (prev_count + new_count - merged_count) ))

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
        raw_merged.repartitionByRange(100, 'left_chrom', 'left_pos')
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

    # For removing duplicates below
    col_subset_for_duplicates = col_subset_for_duplicates + ['is_flipped', 'right_gene_id']

    # Do the same for processed coloc results
    processed_prev = spark.read.parquet(in_processed_previous)
    prev_count = processed_prev.count()
    print('Loaded {} previous coloc tests'.format(prev_count))
    processed_prev = processed_prev.dropDuplicates(subset=col_subset_for_duplicates)
    print('{} records dropped as duplicates'.format(prev_count - processed_prev.count()))
    prev_count = processed_prev.count()

    # Check number of nulls in each column
    #print("Number of nulls in processed_prev:")
    #processed_prev.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in processed_prev.columns]).show()

    processed_new = spark.read.parquet(in_processed_new)
    new_count = processed_new.count()
    print('Loaded {} new coloc tests'.format(new_count))
    processed_new = processed_new.dropDuplicates(subset=col_subset_for_duplicates)
    print('{} records dropped as duplicates'.format(new_count - processed_new.count()))
    new_count = processed_new.count()

    processed_merged = processed_new.unionByName(processed_prev)
    # Remove duplicates - i.e. keep the first row for a given coloc test,
    # which should come from the new dataset
    processed_merged = processed_merged.dropDuplicates(subset=col_subset_for_duplicates)
    #processed_merged2 = drop_duplicates_keep_first(processed_merged, subset=col_subset_for_duplicates)
    merged_count = processed_merged.count()
    print('Writing {} total coloc tests ({} records overwritten by new colocs)'.format(merged_count, (prev_count + new_count - merged_count) ))
    #merged_count2 = processed_merged2.count()
    #print('Writing {} total coloc tests ({} records overwritten by new colocs)'.format(merged_count2, (prev_count + new_count - merged_count2) ))

    # Repartition
    processed_merged = (
        processed_merged.repartitionByRange(100, 'left_chrom', 'left_pos')
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


def drop_duplicates_keep_first(df, subset):
    ''' Implements the equivalent pd.drop_duplicates(keep='first')
    Args:
        df (spark df)
        subset (list): columns to partition by
    Returns:
        df
    '''
    assert isinstance(subset, list)

    # Specfiy window spec
    window = Window.partitionBy(*subset)
    # Select first
    res = (
        df
        .withColumn('tiebreak', monotonically_increasing_id())
        .withColumn('rank', rank().over(window))
        .filter(col('rank') == 1)
        .drop('rank')
    )
    return res


if __name__ == '__main__':

    main()
