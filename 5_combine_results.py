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
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():

    # Make spark session
    # Using `ignoreCorruptFiles` will skip empty files
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.master", "local[*]")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.driver.memory", "100g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # # Set logging level
    # sc = spark.sparkContext
    # sc.setLogLevel('INFO')

    # Args
    in_res_dir = '/output/data/'
    out_coloc = '/output/coloc_raw.parquet'

    res_schema = (
        StructType()
        .add('nsnps', IntegerType(), False)
        .add('PP.H0.abf', DoubleType(), False)
        .add('PP.H1.abf', DoubleType(), False)
        .add('PP.H2.abf', DoubleType(), False)
        .add('PP.H3.abf', DoubleType(), False)
        .add('PP.H4.abf', DoubleType(), False)
        .add('left_study', StringType(), False)
        .add('left_type', StringType(), False)
        .add('left_phenotype', StringType(), True)
        .add('left_bio_feature', StringType(), True)
        .add('left_chrom', StringType(), False)
        .add('left_pos', IntegerType(), False)
        .add('left_ref', StringType(), False)
        .add('left_alt', StringType(), False)
        .add('right_study', StringType(), False)
        .add('right_type', StringType(), False)
        .add('right_phenotype', StringType(), True)
        .add('right_bio_feature', StringType(), True)
        .add('right_chrom', StringType(), False)
        .add('right_pos', IntegerType(), False)
        .add('right_ref', StringType(), False)
        .add('right_alt', StringType(), False)
    )

    # Load
#    df = spark.read.option('basePath', in_res_dir).json(in_res_dir, schema=res_schema)
    df = spark.read.option('basePath', in_res_dir).option("header", True).csv(in_res_dir, schema=res_schema)

    df = (
        df.withColumnRenamed('PP.H0.abf', 'coloc_h0')
        .withColumnRenamed('PP.H1.abf', 'coloc_h1')
        .withColumnRenamed('PP.H2.abf', 'coloc_h2')
        .withColumnRenamed('PP.H3.abf', 'coloc_h3')
        .withColumnRenamed('PP.H4.abf', 'coloc_h4')
        .withColumnRenamed('nsnps', 'coloc_n_vars')
    )

    # Repartition
    # df = (
    #     df.repartitionByRange('left_chrom', 'left_pos')
    #     .sortWithinPartitions('left_chrom', 'left_pos')
    # )

    # Coalesce
    df = df.coalesce(100)
    df.explain()

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
    
    (
        df.toPandas().to_csv(
            '/output/coloc_raw.csv.gz',
            index=False,
            compression='gzip'
        )
    )

    return 0

if __name__ == '__main__':

    main()
