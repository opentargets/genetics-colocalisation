#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
# This is a fix introduced for R6 because I initially ran coloc with the wrong
# top_loci for FinnGen - ones that were computed with GCTA rather than taking the
# credsets directly from FinnGen. Because there are so many separate coloc files,
# I can't separate out those which are based on "wrong" top loci. So I simply
# do a join with the top_loci table to filter out any colocs that don't match
# with a locus in top_loci.
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
        .config("spark.driver.memory", "10g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    print('Spark version: ', spark.version)

    # # Set logging level
    # sc = spark.sparkContext
    # sc.setLogLevel('INFO')

    # Args
    in_coloc = '/output/coloc_raw_needs_fix.parquet/'
    in_toploci = '/data/finemapping/top_loci.json.gz'
    out_coloc = '/output/coloc_raw.parquet'

    # Load
    df = spark.read.parquet(in_coloc)
    print("Number of rows start: {}".format(df.count()))
    #df.filter(col('left_study').contains("GCST")).show()

    # This is needed because the top_loci table has null in the columns
    # bio_feature and phenotype_id for GWAS studies, whereas the coloc
    # table has "None" for these values - I am guessing because of how
    # the empty values are stored in the partitioned dataframe output
    # for the coloc pipeline.
    def null_as_none(x):
        return when(col(x).isNull(), "None").otherwise(col(x))

    toploci = spark.read.json(in_toploci)
    #toploci.filter(col('study_id').contains("GCST")).show()

    toploci = (
        spark.read.json(in_toploci)
        .select('study_id', 'bio_feature', 'phenotype_id', 'chrom', 'pos', 'ref', 'alt')
        .withColumn('bio_feature', null_as_none('bio_feature'))
        .withColumn('phenotype_id', null_as_none('phenotype_id'))
    )
    #toploci.filter(col('study_id').contains("GCST")).show()

    # Join to add indicator whether coloc LEFT variant is in top_loci
    join_expression_left = (
        # Variants
        (col('coloc.left_chrom') == col('toploci.chrom')) &
        (col('coloc.left_pos') == col('toploci.pos')) &
        (col('coloc.left_ref') == col('toploci.ref')) &
        (col('coloc.left_alt') == col('toploci.alt')) &
        # Study
        (col('coloc.left_study') == col('toploci.study_id')) &
        ((col('coloc.left_phenotype').eqNullSafe(col('toploci.phenotype_id')))) &
        ((col('coloc.left_bio_feature').eqNullSafe(col('toploci.bio_feature'))))
    )
    toploci_left = toploci.withColumn('left_match', lit(1))

    merged =(
        df.alias('coloc').join(
            toploci_left.alias('toploci'),
            on=join_expression_left,
            how='left'
        )
    )

    # Join to add indicator whether coloc RIGHT variant is in top_loci
    join_expression_right = (
        # Variants
        (col('coloc.right_chrom') == col('toploci.chrom')) &
        (col('coloc.right_pos') == col('toploci.pos')) &
        (col('coloc.right_ref') == col('toploci.ref')) &
        (col('coloc.right_alt') == col('toploci.alt')) &
        # Study
        (col('coloc.right_study') == col('toploci.study_id')) &
        ((col('coloc.right_phenotype').eqNullSafe(col('toploci.phenotype_id')))) &
        ((col('coloc.right_bio_feature').eqNullSafe(col('toploci.bio_feature'))))
    )
    toploci_right = toploci.withColumn('right_match', lit(1))

    merged =(
        merged.alias('coloc').join(
            toploci_right.alias('toploci'),
            on=join_expression_right,
            how='left'
        )
    )

    # Drop duplicated columns
    merged = merged.drop(
        'chrom', 'pos', 'ref', 'alt',
        'study_id', 'phenotype_id', 'bio_feature'
    )

    #merged.filter(col('left_study').contains("GCST")).show()

    filteredout = merged.filter(col('left_match').isNull() | col('right_match').isNull())
    print("Num filtered out: {}".format(filteredout.count()))

    merged = (
        merged
        .filter((col('left_match') == lit(1)) & (col('right_match') == lit(1)))
        .drop('left_match', 'right_match')
    )
    print("Number of rows end: {}".format(merged.count()))

    # Write
    (
        merged
        .write
        .parquet(
            out_coloc,
            compression='snappy',
            mode='overwrite'
        )
    )

    return 0

if __name__ == '__main__':

    main()
