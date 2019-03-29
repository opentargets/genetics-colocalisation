#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''

import sys
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():

    # Args
    in_path = 'gs://genetics-portal-staging/finemapping/190320/credset/part-*.json.gz'
    out_path = 'gs://genetics-portal-staging/finemapping/190320/credset_partitioned/'

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # Load, then write partitioned
    (
        spark.read.json(in_path)
        .write
        .partitionBy('study_id', 'phenotype_id', 'bio_feature', 'lead_chrom')
        .json(
            out_path,
            compression='gzip',
            mode='overwrite'
        )
    )
    
    return 0    

if __name__ == '__main__':

    main()
