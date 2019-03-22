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
import numpy as np
import os

def main():

    # Make spark session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    print('Spark version: ', spark.version)
    
    # Parse args
    in_ld = 'input_data/ld.parquet'
    in_coloc = 'input_data/coloc.json.gz'
    in_top_loci = 'input_data/top_loci.json.gz'
    out_f = 'output/features.csv'

    # Make output dir
    os.makedirs(os.path.dirname(out_f), exist_ok=True)

    # Load
    ld = (
        spark.read.parquet(in_ld)
             .drop('R_EUR')
             .fillna('None')
    )
    coloc = (
        spark.read.json(in_coloc)
             .filter(col('coloc_n_vars') > 200)
             .fillna('None')
             # Only use 'gwas' type in left
             .filter(col('left_type') == 'gwas')
            #  .limit(100) # DEBUG
    )
    toploci = (
        spark.read.json(in_top_loci)
             .fillna('None')
    )

    #
    # Make LD overlap features ------------------------------------------------
    #

    # Make left ld dataset
    left = (
        coloc
        .select('left_type', 'left_study', 'left_phenotype', 'left_bio_feature',
                     'left_chrom', 'left_pos', 'left_ref', 'left_alt')
        .distinct()
        .join((ld.withColumnRenamed('lead_chrom', 'left_chrom')
                .withColumnRenamed('lead_pos', 'left_pos')
                .withColumnRenamed('lead_ref', 'left_ref')
                .withColumnRenamed('lead_alt', 'left_alt')
                .withColumnRenamed('R2_EUR', 'left_R2_EUR')),
              on=['left_chrom', 'left_pos', 'left_ref', 'left_alt'],
              how='inner')
    )

    # Make right ld dataset
    right = (
        coloc
        .select('right_type', 'right_study', 'right_phenotype', 'right_bio_feature',
                     'right_chrom', 'right_pos', 'right_ref', 'right_alt')
        .distinct()
        .join((ld.withColumnRenamed('lead_chrom', 'right_chrom')
                .withColumnRenamed('lead_pos', 'right_pos')
                .withColumnRenamed('lead_ref', 'right_ref')
                .withColumnRenamed('lead_alt', 'right_alt')
                .withColumnRenamed('R2_EUR', 'right_R2_EUR')),
            on=['right_chrom', 'right_pos', 'right_ref', 'right_alt'],
            how='inner')   
    )

    # Make intersection
    intersection = left.join(
        right,
        on=['tag_chrom', 'tag_pos', 'tag_ref', 'tag_alt']
    )

    # # DEBUG extract from overlap
    # (
    #     intersection
    #     .filter(
    #         (col('left_study') == 'GCST004131_ibd') &
    #         (col('left_chrom') == 7) &
    #         (col('left_pos') == 6505557) &
    #         (col('right_study') == 'Blueprint') &
    #         (col('right_phenotype') == 'ENSG00000215018') &
    #         (col('right_bio_feature') == 'NEUTROPHIL') &
    #         (col('right_pos') == 6457870)
    #     )
    #     .coalesce(1)
    #     .write.csv(
    #         'output/intersection.csv',
    #         mode='overwrite',
    #         header=True
    #     )
    # )
    # sys.exit()

    # Count proportion overlapping for a range of R2 thresholds
    features = (
        intersection
        .groupby('left_type', 'left_study', 'left_phenotype', 'left_bio_feature',
                 'left_chrom', 'left_pos', 'left_ref', 'left_alt',
                 'right_type', 'right_study', 'right_phenotype', 'right_bio_feature',
                 'right_chrom', 'right_pos', 'right_ref', 'right_alt')
            .agg(
                count(when((col('left_R2_EUR') >= 0) & (col('right_R2_EUR') >= 0), lit(1))).alias('total_overlap'),
                (count(when((col('left_R2_EUR') >= 0.1) & (col('right_R2_EUR') >= 0.1), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.1'),
                (count(when((col('left_R2_EUR') >= 0.2) & (col('right_R2_EUR') >= 0.2), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.2'),
                (count(when((col('left_R2_EUR') >= 0.3) & (col('right_R2_EUR') >= 0.3), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.3'),
                (count(when((col('left_R2_EUR') >= 0.4) & (col('right_R2_EUR') >= 0.4), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.4'),
                (count(when((col('left_R2_EUR') >= 0.5) & (col('right_R2_EUR') >= 0.5), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.5'),
                (count(when((col('left_R2_EUR') >= 0.6) & (col('right_R2_EUR') >= 0.6), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.6'),
                (count(when((col('left_R2_EUR') >= 0.7) & (col('right_R2_EUR') >= 0.7), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.7'),
                (count(when((col('left_R2_EUR') >= 0.75) & (col('right_R2_EUR') >= 0.75), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.75'),
                (count(when((col('left_R2_EUR') >= 0.8) & (col('right_R2_EUR') >= 0.8), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.8'),
                (count(when((col('left_R2_EUR') >= 0.85) & (col('right_R2_EUR') >= 0.85), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.85'),
                (count(when((col('left_R2_EUR') >= 0.9) & (col('right_R2_EUR') >= 0.9), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.9'),
                (count(when((col('left_R2_EUR') >= 0.95) & (col('right_R2_EUR') >= 0.95), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.95'),
                (count(when((col('left_R2_EUR') >= 0.975) & (col('right_R2_EUR') >= 0.975), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_0.975'),
                (count(when((col('left_R2_EUR') >= 1.0) & (col('right_R2_EUR') >= 1.0), lit(1))) / count(col('left_R2_EUR'))).alias('prop_overlap_1.0')
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

    # Get table of -log(pval)
    pvals = (
        toploci
        .select('study_id', 'phenotype_id', 'bio_feature',
                'chrom', 'pos', 'ref', 'alt', 'pval')
        .withColumn('log_pval', -log10(col('pval')))
        .fillna(-np.log10(sys.float_info.min))
        .drop('pval')
    )

    # Merge to left
    features = (
        features.join(
            pvals.alias('left_pvals').withColumnRenamed('log_pval', 'left_log_pval'),
            (
                (features.left_study == col('left_pvals.study_id')) &
                (features.left_phenotype == col('left_pvals.phenotype_id')) &
                (features.left_bio_feature == col('left_pvals.bio_feature')) &
                (features.left_chrom == col('left_pvals.chrom')) &
                (features.left_pos == col('left_pvals.pos')) &
                (features.left_ref == col('left_pvals.ref')) &
                (features.left_alt == col('left_pvals.alt'))
            ))
        .drop('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos', 'ref', 'alt')

    )

    # Merge to right
    features = (
        features.join(
            pvals.alias('right_pvals').withColumnRenamed('log_pval', 'right_log_pval'),
            (
                (features.right_study == col('right_pvals.study_id')) &
                (features.right_phenotype == col('right_pvals.phenotype_id')) &
                (features.right_bio_feature == col('right_pvals.bio_feature')) &
                (features.right_chrom == col('right_pvals.chrom')) &
                (features.right_pos == col('right_pvals.pos')) &
                (features.right_ref == col('right_pvals.ref')) &
                (features.right_alt == col('right_pvals.alt'))
            ))
        .drop('study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos', 'ref', 'alt')
    )

    #
    # Add feature showing whether left/right is gwas --------------------------
    #

    features = (
        features
        .withColumn('left_is_gwas', when(col('left_type') == 'gwas', 1).otherwise(0))
        .withColumn('right_is_gwas', when(col('right_type') == 'gwas', 1).otherwise(0))
    )

    #
    # Merge coloc stats -------------------------------------------------------
    #
    features = (
        features.alias('features').join(
            coloc.alias('coloc'),
            on=['left_type', 'left_study', 'left_phenotype', 'left_bio_feature', 'left_chrom', 'left_pos', 'left_ref', 'left_alt',
                'right_type', 'right_study', 'right_phenotype', 'right_bio_feature', 'right_chrom', 'right_pos', 'right_ref', 'right_alt'],
            how='outer'
        )       
        .drop('coloc_h0', 'coloc_h1', 'coloc_h2', 'coloc_h4_H3', 'coloc_n_vars', 'is_flipped', 'left_sumstat', 'right_sumstat')
    ).cache()
    
    print('Rows with LD only: ', features.filter(col('total_overlap').isNotNull() & col('coloc_log_H4_H3').isNull()).count())
    print('Rows with coloc only: ', features.filter(col('total_overlap').isNull() & col('coloc_log_H4_H3').isNotNull()).count())
    print('Rows with LD and coloc: ', features.filter(col('total_overlap').isNotNull() & col('coloc_log_H4_H3').isNotNull()).count())
    
    #
    # Write output -------------------------------------------------------
    #

    # Output outer join for debugging
    (
        features
        .coalesce(1)
        .write.csv(
            out_f.replace('.csv', '.outer.csv'),
            header=True,
            mode='overwrite'
        )
    )

    # Output inner join as final feature set
    (
        features
        .filter(col('total_overlap').isNotNull() & col('coloc_log_H4_H3').isNotNull())
        .coalesce(1)
        .write.csv(
            out_f,
            header=True,
            mode='overwrite'
        )
    )



    return 0

if __name__ == '__main__':

    main()
