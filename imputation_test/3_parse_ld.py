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
    in_pattern = 'ld_output/*.ld.tsv.gz'
    out_f = 'input_data/ld.parquet'

    # Make spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)

    
    # Load
    import_schema = (
        StructType()
        .add('index_variant_id', StringType())
        .add('tag_variant_id', StringType())
        .add('R_EUR', DoubleType())
        .add('R2_EUR', DoubleType())
    )
    df = (
        spark.read.csv(
            path=in_pattern,
            sep='\t',
            schema=import_schema,
            header=True,
        )
    )

    # Split variants
    df = (
        df.withColumn('lead_split', split('index_variant_id', '_'))
          .withColumn('lead_chrom', col('lead_split').getItem(0))
          .withColumn('lead_pos', col('lead_split').getItem(1).cast('int'))
          .withColumn('lead_ref', col('lead_split').getItem(2))
          .withColumn('lead_alt', col('lead_split').getItem(3))
          .withColumn('tag_split', split('tag_variant_id', '_'))
          .withColumn('tag_chrom', col('tag_split').getItem(0))
          .withColumn('tag_pos', col('tag_split').getItem(1).cast('int'))
          .withColumn('tag_ref', col('tag_split').getItem(2))
          .withColumn('tag_alt', col('tag_split').getItem(3))
          .drop('lead_split', 'tag_split', 'index_variant_id', 'tag_variant_id')
    )
    
    # Write
    (
        df
        .repartitionByRange('lead_chrom', 'lead_pos')
        .write.parquet(out_f,
                    mode='overwrite',
                    compression='snappy')
    )

    return 0

if __name__ == '__main__':

    main()
