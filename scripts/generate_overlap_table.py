#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Creates overlap table
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
import argparse

def main():

    # Parse args
    args = parse_args()

    # Make spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)

    #
    # Load and filter data
    #

    # Load
    df = (
        spark.read.json(args.in_credset)
             .select('type', 'study_id', 'phenotype_id', 'bio_feature', 'lead_chrom',
                     'lead_pos', 'lead_ref', 'lead_alt', 'tag_chrom', 'tag_pos',
                     'tag_ref', 'tag_alt', 'is95_credset', 'is99_credset',
                     'multisignal_method')
    )

    # Filter on credset
    df = df.filter(col('is{}_credset'.format(args.which_set)))

    # Filter of method
    if args.which_method != 'all':
        df = df.filter(col('multisignal_method') == args.which_method)

    # Drop filtering columns
    df = df.drop('is95_credset', 'is99_credset', 'multisignal_method')

    # Study identifier columns
    study_cols = ['type', 'study_id', 'phenotype_id', 'bio_feature',
                  'lead_chrom', 'lead_pos', 'lead_ref', 'lead_alt']

    # Create a single key column as an identifier for the study
    df = df.withColumn('key', concat_ws('_', *study_cols))
    study_cols = ['key'] + study_cols

    #
    # Create left and right datasets
    #
    
    left = df
    for colname in study_cols:
        left = left.withColumnRenamed(colname, 'left_' + colname)
    right = df
    for colname in study_cols:
        right = right.withColumnRenamed(colname, 'right_' + colname)

    #
    # Find overlaps
    #

    # Merge
    cols_to_merge = ['tag_chrom', 'tag_pos', 'tag_ref', 'tag_alt']
    overlap = left.join(right, on=cols_to_merge, how='inner')

    # Remove non-needed overlaps
    overlap = (
        # Remove overlaps that belong to the same study
        overlap.filter(col('left_study_id') != col('right_study_id'))
        # Make sure at least one study is of type 'gwas' (no mol trait vs mol trait)
        .filter((col('left_type') == 'gwas') | (col('right_type') == 'gwas'))
        # Keep only the upper triangle from the square matrix
        .filter(col('left_key') > col('right_key'))
    )

    # Count overlaps
    group_by_cols = [prefix + colname for prefix in ['left_', 'right_']
                     for colname in study_cols]
    overlap_counts = (
        overlap.groupby(group_by_cols)
               .agg(count('tag_chrom').alias('num_overlapping'))
    ).cache()

    #
    # Count total number of tags and merge this to the overlap counts for both left and right keys
    #

    # Calculate the total number of tags per key
    tag_counts = (
        df.groupby('key')
          .agg(count('tag_chrom').alias('num_tags'))
    ).cache()

    # Merge to left key
    overlap_counts = overlap_counts.join(
        tag_counts.alias('left_tag_counts').withColumnRenamed('num_tags', 'left_num_tags'),
        overlap_counts.left_key == col('left_tag_counts.key')
    ).drop('key').cache()

    # Merge to right key
    overlap_counts = overlap_counts.join(
        tag_counts.alias('right_tag_counts').withColumnRenamed('num_tags', 'right_num_tags'),
        overlap_counts.right_key == col('right_tag_counts.key')
    ).drop('key').cache()


    # Calc proportion of overlapping tags in left and right datasets
    overlap_counts = (
        overlap_counts
        .withColumn('left_overlap_prop', col('num_overlapping') / col('left_num_tags'))
        .withColumn('right_overlap_prop', col('num_overlapping') / col('right_num_tags'))
    )

    # Write
    (
        overlap_counts.coalesce(1)
        .drop('left_key', 'right_key')
        .write.json(args.outf,
                    mode='overwrite')
    )

    return 0

    

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_credset',
                        metavar="<parquet>",
                        type=str,
                        required=True)
    parser.add_argument('--which_set',
                        metavar="<95|99>",
                        type=str,
                        required=True,
                        default='95',
                        choices=['95', '99'],
                        help='Whether to use the 95%% or 99%% credible sets (default: 95)')
    parser.add_argument('--which_method',
                        metavar="<all|conditional|distance>",
                        type=str,
                        required=True,
                        default='all',
                        choices=['all', 'conditional', 'distance'],
                        help='Only use credible sets derived using this method (default: all)')
    parser.add_argument('--outf',
                        metavar="<tsv>",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
