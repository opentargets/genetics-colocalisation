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
from shutil import copyfile
from glob import glob

def main():

    # Make spark session
    # Using `ignoreCorruptFiles` will skip empty files
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .getOrCreate()
    )
    # sc = spark.sparkContext
    print('Spark version: ', spark.version)

    # Args
    in_res_pattern = 'output/left_study=*/left_phenotype=*/left_bio_feature=*/left_variant=*/right_study=*/right_phenotype=*/right_bio_feature=*/right_variant=*/coloc_res.json.gz'
    out_coloc = 'results/coloc'
    make_symmetric = True # Will make the coloc matrix symmetric

    # Load
    df = spark.read.json(in_res_pattern)

    # Rename and calc new columns 
    df = (
        df.withColumnRenamed('PP.H0.abf', 'coloc_h0')
        .withColumnRenamed('PP.H1.abf', 'coloc_h1')
        .withColumnRenamed('PP.H2.abf', 'coloc_h2')
        .withColumnRenamed('PP.H3.abf', 'coloc_h3')
        .withColumnRenamed('PP.H4.abf', 'coloc_h4')
        .withColumnRenamed('nsnps', 'coloc_n_vars')
        .withColumn('coloc_h4_H3', (col('coloc_h4') / col('coloc_h3')))
        .withColumn('coloc_log_H4_H3', log(col('coloc_h4_H3')))
    )

    # Make symmetric
    if make_symmetric:

        df_rev = df

        # Move all left_ columns to temp_
        for colname in [x for x in df_rev.columns if x.startswith('left_')]:
            df_rev = df_rev.withColumnRenamed(
                colname, colname.replace('left_', 'temp_'))
        
        # Move all right_ columns to left_
        for colname in [x for x in df_rev.columns if x.startswith('right_')]:
            df_rev = df_rev.withColumnRenamed(
                colname, colname.replace('right_', 'left_'))

        # Move all temp_ columns to right_
        for colname in [x for x in df_rev.columns if x.startswith('temp_')]:
            df_rev = df_rev.withColumnRenamed(
                colname, colname.replace('temp_', 'right_'))
        
        # Take union by name between original and flipped dataset
        df = df.withColumn('is_flipped', lit(False))
        df_rev = df_rev.withColumn('is_flipped', lit(True))
        df = df.unionByName(df_rev)

    # Write
    (
        df
        .coalesce(1)
        .write.json(out_coloc,
                    compression='gzip',
                    mode='overwrite')
    )
    
    # Copy to single file
    copyfile(
        glob(out_coloc + '/part-*.json.gz')[0],
        out_coloc + '.json.gz'
    )

    return 0

if __name__ == '__main__':

    main()
