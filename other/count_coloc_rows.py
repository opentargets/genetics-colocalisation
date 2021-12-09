#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
'''
Check number of rows in different datasets to find any duplicates.
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
import sys
import os

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

    # Load coloc
    colocraw = spark.read.parquet('gs://genetics-portal-dev-staging/coloc/210927/coloc_raw.parquet')
    print("coloc_raw.parquet number of rows: {}".format(colocraw.count()))

    coloc_processed = spark.read.parquet('gs://genetics-portal-dev-staging/coloc/210927/coloc_processed.parquet')
    print("coloc_processed.parquet number of rows: {}".format(coloc_processed.count()))

    colocnew = spark.read.parquet('gs://genetics-portal-dev-staging/coloc/210927/coloc_processed_w_betas_new.parquet')
    print("coloc_processed_w_betas_new.parquet number of rows: {}".format(colocnew.count()))

    colocdups = spark.read.parquet('gs://genetics-portal-dev-staging/coloc/210927/coloc_processed_w_betas_dups.parquet')
    print("coloc_processed_w_betas_dups.parquet number of rows: {}".format(colocdups.count()))

    coloc = spark.read.parquet('gs://genetics-portal-dev-staging/coloc/210927/coloc_processed_w_betas.parquet')
    print("coloc_processed_w_betas.parquet number of rows: {}".format(coloc.count()))

    return 0

if __name__ == '__main__':

    main()
