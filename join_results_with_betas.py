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
from pyspark.sql.functions import *
from pyspark.sql.types import *
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

    # File args (dataproc)
    in_parquet = '/data/coloc_processed.parquet'
    in_sumstats = '/data/significant_window_2mb'
    out_parquet = '/data/coloc_processed_w_betas.parquet'

    # Load coloc
    coloc = spark.read.parquet(in_parquet)

    # Select studies
    coloc_df = coloc.toPandas()
    for index, row in coloc_df.itterrows():
        sumstats_type = row.right_type
        study = row.right_study
        sumstats_file = os.path.join(in_sumstats, sumstats_type, study + '.parquet')

        # Load sumstats
        sumstats = spark.read.parquet(sumstats_file)
        # sumstats.printSchema()

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

        # Repartition
        merged = (
            merged.repartitionByRange('left_chrom', 'left_pos')
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

    return 0


if __name__ == '__main__':
    main()
