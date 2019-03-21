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
import sys

def main():

    # Make spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    
    # Parse args
    in_ld = 'input_data/ld.parquet'
    in_coloc = 'input_data/coloc.json.gz'
    in_top_loci = 'input_data/top_loci.json.gz'
    # out_f = 'features.parquet'

    # Load
    ld = (
        spark.read.parquet(in_ld)
             .drop('R_EUR')
             .fillna('None')
    )
    coloc = (
        spark.read.json(in_coloc)
             .limit(100) # DEBUG
             .fillna('None')
    )
    toploci = (
        spark.read.json(in_top_loci)
             .fillna('None')
    )

    # ld.printSchema()
    # print(ld.columns)
    # coloc.printSchema()
    # print(coloc.columns)
    # toploci.printSchema()
    # print(toploci.columns)

    #
    # Make LD overlap features ------------------------------------------------
    #

    # Make left ld dataset
    left = (
        coloc.select('left_study', 'left_phenotype', 'left_bio_feature',
                     'left_chrom', 'left_pos', 'left_ref', 'left_alt')
        .join(ld,
              ((coloc.left_chrom == ld.lead_chrom) &
               (coloc.left_pos == ld.lead_pos) &
               (coloc.left_ref == ld.lead_ref) &
               (coloc.left_alt == ld.lead_alt)))
        .drop('lead_chrom', 'lead_pos', 'lead_ref', 'lead_alt')
        .withColumnRenamed('R2_EUR', 'left_R2_EUR')
    )

    # Make right ld dataset
    right = (
        coloc.select('right_study', 'right_phenotype', 'right_bio_feature',
                     'right_chrom', 'right_pos', 'right_ref', 'right_alt')
        .join(ld,
              ((coloc.right_chrom == ld.lead_chrom) &
               (coloc.right_pos == ld.lead_pos) &
               (coloc.right_ref == ld.lead_ref) &
               (coloc.right_alt == ld.lead_alt)))
        .drop('lead_chrom', 'lead_pos', 'lead_ref', 'lead_alt')
        .withColumnRenamed('R2_EUR', 'right_R2_EUR')
    )

    # Make intersection
    intersection = left.join(
        right,
        on=['tag_chrom', 'tag_pos', 'tag_ref', 'tag_alt']
    )

    # Count proportion overlapping for a range of R2 thresholds
    features = (
        intersection
        .groupby('left_study', 'left_phenotype', 'left_bio_feature',
                 'left_chrom', 'left_pos', 'left_ref', 'left_alt',
                 'right_study', 'right_phenotype', 'right_bio_feature',
                 'right_chrom', 'right_pos', 'right_ref', 'right_alt')
            .agg(
                count(col('left_R2_EUR')).alias('total_overlap'),
                (count(when((col('left_R2_EUR') > 0.7) & (col('right_R2_EUR') > 0.7), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.7'),
                (count(when((col('left_R2_EUR') > 0.75) & (col('right_R2_EUR') > 0.75), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.75'),
                (count(when((col('left_R2_EUR') > 0.8) & (col('right_R2_EUR') > 0.8), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.8'),
                (count(when((col('left_R2_EUR') > 0.85) & (col('right_R2_EUR') > 0.85), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.85'),
                (count(when((col('left_R2_EUR') > 0.9) & (col('right_R2_EUR') > 0.9), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.9'),
                (count(when((col('left_R2_EUR') > 0.95) & (col('right_R2_EUR') > 0.95), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.95'),
                (count(when((col('left_R2_EUR') > 1.0) & (col('right_R2_EUR') > 1.0), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_1.0')
            )
    ).cache()

    #
    # Add the number of tags per left - right dataset -------------------------
    #

    # Calculate num tags left
    cols = ['left_study', 'left_phenotype', 'left_bio_feature',
            'left_chrom', 'left_pos', 'left_ref', 'left_alt']
    left_n_tags = (
        left.groupby(cols)
        .agg(count(col('tag_chrom')).alias('left_num_tags'))
    )
    features = features.join(left_n_tags, on=cols)
    
    # Calculate num tags right
    cols = ['right_study', 'right_phenotype', 'right_bio_feature',
            'right_chrom', 'right_pos', 'right_ref', 'right_alt']
    right_n_tags = (
        right.groupby(cols)
        .agg(count(col('tag_chrom')).alias('right_num_tags'))
    )
    features = features.join(right_n_tags, on=cols)

    # Add proportions of left_num_tags/right_num_tags that overlap
    features = (
        features
        .withColumn('left_prop_total_overlap', col('total_overlap') / col('left_num_tags'))
        .withColumn('right_prop_total_overlap', col('total_overlap') / col('right_num_tags'))
    )
    
    #
    # Add abs(distance) between left/right leads ------------------------------
    #

    features = (
        features
        .withColumn('abs_distance', abs(col('left_pos') - col('right_pos')))
    )

    #
    # Add -log(pval) for left/right leads from toploci table ------------------
    #

    features.show(3)
    print(features.columns)



    return 0

if __name__ == '__main__':

    main()
