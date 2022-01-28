#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
'''
Join the summary stat beta, se, pval onto the coloc table so they can be
displayed in the portal. Joins on: left variant, but right study!
'''

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''
import pyspark.sql
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():

    # Make spark session
    spark = (
        pyspark.sql.SparkSession.builder
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        # .config("spark.master", "local[*]") # Don't use with dataproc!
        .getOrCreate()
    )
    # sc = spark.sparkContext
    print('Spark version: ', spark.version)

    # File args (dataproc)
    in_parquet = 'gs://genetics-portal-dev-staging/coloc/220127/coloc_processed.parquet'
    in_sumstats = 'gs://genetics-portal-dev-sumstats/filtered/significant_window_2mb_union'
    out_parquet = 'gs://genetics-portal-dev-staging/coloc/220127/coloc_processed_w_betas.parquet'
    out_dups = 'gs://genetics-portal-dev-staging/coloc/220127/coloc_processed_w_betas_dups.parquet'
    
    # # File args (local)
    # in_parquet = '/home/ubuntu/results/coloc/results/coloc_processed.parquet'
    # in_sumstats = '/home/ubuntu/data/sumstats/filtered/significant_window_2mb/union'
    # out_parquet = '/home/ubuntu/results/coloc/results/coloc_processed_w_betas.parquet'

    # Load coloc
    coloc = spark.read.parquet(in_parquet)
    coloc.printSchema()
    nrows_start = coloc.count()

    # Load sumstats
    sumstats = spark.read.parquet(in_sumstats)
    sumstats.printSchema()

    # Rename join columns on sumstat table
    sumstats_join = (
        sumstats
        # Statistic columns
        .withColumnRenamed('beta', 'left_var_right_study_beta')
        .withColumnRenamed('se', 'left_var_right_study_se')
        .withColumnRenamed('pval', 'left_var_right_study_pval')
        .withColumnRenamed('is_cc', 'left_var_right_isCC')
        # Only keep required columns
        .select('chrom', 'pos', 'ref', 'alt',
                'study_id', 'phenotype_id', 'bio_feature',
                'left_var_right_study_beta', 'left_var_right_study_se',
                'left_var_right_study_pval', 'left_var_right_isCC')
    )

    # Join using nullSafe for phenotype and bio_feature
    # print('WARNING: this must be a left join in the final implementation')
    join_expression = (
        # Variants
        (col('coloc.left_chrom') == col('sumstats.chrom')) &
        (col('coloc.left_pos') == col('sumstats.pos')) &
        (col('coloc.left_ref') == col('sumstats.ref')) &
        (col('coloc.left_alt') == col('sumstats.alt')) &
        # Study
        (col('coloc.right_study') == col('sumstats.study_id')) &
        (col('coloc.right_phenotype').eqNullSafe(col('sumstats.phenotype_id'))) &
        (col('coloc.right_bio_feature').eqNullSafe(col('sumstats.bio_feature')))
    )
    merged =(
        coloc.alias('coloc').join(
            sumstats_join.alias('sumstats'),
            on=join_expression,
            how='left'
        )
    )

    # Drop duplicated columns
    merged = merged.drop(
        'chrom', 'pos', 'ref', 'alt',
        'study_id', 'phenotype_id', 'bio_feature'
    )

    # Check the number of rows - it shouldn't have changed
    nrows_end = merged.count()
    if (nrows_end - nrows_start > 0):
        print("WARNING: joining with betas added {} rows (presumed duplicates) to the coloc table!".format(nrows_end - nrows_start))
    
    col_subset = [
        'left_type',
        'left_study',
        'left_chrom',
        'left_pos',
        'left_ref',
        'left_alt',
        'right_type',
        'right_study',
        'right_bio_feature',
        'right_phenotype',
        # 'right_chrom',
        # 'right_pos',
        # 'right_ref',
        # 'right_alt'
    ]
    dups = keep_duplicates(
        merged,
        subset=col_subset,
        order_colname='coloc_h4',
        ascending=False
        )

    (
        dups
        .write.parquet(
            out_dups,
            mode='overwrite'
        )
    )

    # Remove duplicates that may have been introduced by the join
    merged = drop_duplicates_keep_first(
        merged,
        subset=col_subset,
        order_colname='coloc_h4',
        ascending=False
        )

    # Repartition
    merged = (
        merged.repartitionByRange(100, 'left_chrom', 'left_pos')
        .sortWithinPartitions('left_chrom', 'left_pos')
    )

    # Write
    (
        merged
        .write.parquet(
            out_parquet,
            mode='overwrite'
        )
    )

    if (nrows_end - nrows_start > 0):
        return 1
    
    return 0


def drop_duplicates_keep_first(df, subset, order_colname, ascending=True):
    return drop_duplicates_keep_rank(df, subset, order_colname, 1, ascending)

def keep_duplicates(df, subset, order_colname, ascending=True):
    return drop_duplicates_keep_rank(df, subset, order_colname, 2, ascending)

def drop_duplicates_keep_rank(df, subset, order_colname, rank_to_keep, ascending=True):
    ''' Implements the equivalent pd.drop_duplicates(keep='first')
    Args:
        df (spark df)
        subset (list): columns to partition by
        order_colname (str): column to sort by
        ascending (bool): whether to sort ascending
    Returns:
        df
    '''
    assert isinstance(subset, list)

    # Get order column ascending or descending
    if ascending:
        order_col = col(order_colname)
    else:
        order_col = col(order_colname).desc()

    # Specfiy window spec
    window = Window.partitionBy(*subset).orderBy(
        order_col, 'tiebreak')
    # Select first
    res = (
        df
        .withColumn('tiebreak', monotonically_increasing_id())
        .withColumn('rank', rank().over(window))
        .filter(col('rank') == rank_to_keep)
        .drop('rank', 'tiebreak')
    )
    return res


if __name__ == '__main__':

    main()
