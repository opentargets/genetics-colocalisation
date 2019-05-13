#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
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
import sys
import pandas as pd

def main():

    # Args
    in_json = '../tmp/coloc_processed.json'
    out_qtl = '../tmp/coloc_qtl_counts.tsv'
    out_gwas = '../tmp/coloc_gwas_counts.tsv'
    out_gwas_loci = '../tmp/coloc_gwas_loci_counts.tsv'
    gwas_window = 500000 # kb

    # Make spark session
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.master", "local[*]")
        .getOrCreate()
    )
    # sc = spark.sparkContext
    print('Spark version: ', spark.version)

    # Load
    df = spark.read.json(in_json) #.limit(10000)
    
    # Make qtl counts
    (
        df
        .filter(col('right_type') != 'gwas')
        .groupby('left_study', 'left_chrom', 'left_pos',
                 'left_ref', 'left_alt')
        .count()
        .orderBy('count', ascending=False)
        .coalesce(1).write.csv(out_qtl, mode='overwrite', header=True)
    )

    # Make gwas counts
    (
        df
        .filter(col('right_type') == 'gwas')
        .groupby('left_study', 'left_chrom', 'left_pos',
                 'left_ref', 'left_alt')
        .count()
        .orderBy('count', ascending=False)
        .coalesce(1).write.csv(out_gwas, mode='overwrite', header=True)
    )

    #
    # Make gwas counts based on loci ------------------------------------------
    #

    # Filter to keep only gwas type
    df = df.filter(col('right_type') == 'gwas')

    # Perfrom join
    merged = (

        # Create left dataset
        df.select('left_study', 'left_chrom',
                  'left_pos', 'left_ref', 'left_alt')
            .alias('left').join(
            
        # Create right dataset
        df.select('right_study', 'right_chrom', 'right_pos',
                    'right_ref', 'right_alt')
            .alias('right'),

        # Specify equi-join
        ( 
            (col('left.left_chrom') == col('right.right_chrom')) &
            (abs(col('left.left_pos') - col('right.right_pos')) <= gwas_window)
        )
            
        )
    )

    # Remove duplicates
    merged = (
        merged.select('left_study', 'left_chrom',
                      'left_pos', 'left_ref', 'left_alt',
                      'right_study', 'right_chrom', 'right_pos',
                      'right_ref', 'right_alt')
        .dropDuplicates()
    )

    # Count
    (
        merged
        .groupby('left_study', 'left_chrom', 'left_pos',
                 'left_ref', 'left_alt')
        .count()
        .orderBy('count', ascending=False)
        .coalesce(1).write.csv(out_gwas_loci, mode='overwrite', header=True)
    )

    return 0
    
if __name__ == '__main__':

    main()
