#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
'''
Processes results for the genetics portal. Processing includes:
1. Make the table symmetrical again
2. Filter to keep only left_type == gwas
3. Only keep the top colocalising result if multiple right loci were tested
4. Filter to remove colocs where small number of variants overlapped.
'''

'''
# Set SPARK_HOME and PYTHONPATH to use 2.4.0
export PYSPARK_SUBMIT_ARGS="--driver-memory 8g pyspark-shell"
export SPARK_HOME=/Users/em21/software/spark-2.4.0-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-2.4.0-src.zip:$PYTHONPATH
'''
from pyspark.sql import Window
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os
import gzip
from glob import glob

def main():

    # Make spark session
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.master", "local[*]")
        .getOrCreate()
    )
    # sc = spark.sparkContext
    print('Spark version: ', spark.version)

    # File args
    in_parquet = '/home/js29/genetics-colocalisation/results/coloc/results/coloc_raw.parquet'
    out_parquet = '/home/js29/genetics-colocalisation/results/coloc/results/coloc_processed.parquet'
    # in_parquet = '/Users/em21/Projects/genetics-colocalisation/tmp/coloc_raw.parquet'
    # out_parquet = '/Users/em21/Projects/genetics-colocalisation/tmp/coloc_processed.parquet'
    in_phenotype_maps = 'configs/phenotype_id_gene_luts/*.tsv.gz'

    # Results parameters
    make_symmetric = True # Will make the coloc matrix symmetric
    left_gwas_only = True # Output will only contains rows where left_type == gwas
    deduplicate_right = True # For each left dataset, only keep the "best" right dataset
    min_overlapping_vars = 100 # Only keep results with this many overlapping vars

    # Load
    df = spark.read.parquet(in_parquet) #.limit(100)

    # Rename and calc new columns 
    df = (
        df.withColumnRenamed('PP.H0.abf', 'coloc_h0')
        .withColumnRenamed('PP.H1.abf', 'coloc_h1')
        .withColumnRenamed('PP.H2.abf', 'coloc_h2')
        .withColumnRenamed('PP.H3.abf', 'coloc_h3')
        .withColumnRenamed('PP.H4.abf', 'coloc_h4')
        .withColumnRenamed('nsnps', 'coloc_n_vars')
        .withColumn('coloc_h4_h3', (col('coloc_h4') / col('coloc_h3')))
        .withColumn('coloc_log2_h4_h3', log2(col('coloc_h4_h3')))
    )

    # Filter based on the number of snps overlapping the left and right datasets
    if min_overlapping_vars:
        df = df.filter(col('coloc_n_vars') >= min_overlapping_vars)

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

    # Keep only rows where left_type == gwas
    if left_gwas_only:
        df = df.filter(col('left_type') == 'gwas')
    
    # Deduplicate right
    if deduplicate_right:

        # Deduplicate the right dataset
        col_subset = [
            'left_type',
            'left_study',
            'left_phenotype',
            'left_bio_feature',
            'left_chrom',
            'left_pos',
            'left_ref',
            'left_alt',
            'right_type',
            'right_study',
            'right_bio_feature',
            'right_phenotype',
            # 'right_chrom',
            # 'right_pos',
            # 'right_ref',
            # 'right_alt'
        ]
        
        # Drop duplicates, keeping first
        df = drop_duplicates_keep_first(
            df,
            subset=col_subset,
            order_colname='coloc_h4',
            ascending=False
        )

    # Add gene_id using phenotype_id
    phenotype_map = load_pheno_to_gene_map(in_phenotype_maps)
    biofeature_mapper = udf(lambda x: phenotype_map.get(x, x))
    df = (
        df.withColumn('left_gene_id', biofeature_mapper(col('left_phenotype')))
          .withColumn('right_gene_id', biofeature_mapper(col('right_phenotype')))
    )

    # Set gene_id to null if it doesn't start with ENSG
    for colname in ['left_gene_id', 'right_gene_id']:
        df = df.withColumn(
            colname,
            when(col(colname).startswith('ENSG'), col(colname))
                .otherwise(lit(None))
        )
    
    # Remove unneeded columns
    df = df.drop('left_sumstat', 'right_sumstat')
    if left_gwas_only:
        df = df.drop('left_gene_id', 'left_bio_feature', 'left_phenotype')

    # Remove rows that have null in coloc stat columns
    df = df.dropna(
        subset=['coloc_h3', 'coloc_h4', 'coloc_log2_h4_h3'],
        how='any'
    )

    # Repartition
    df = (
        df.repartitionByRange('left_chrom', 'left_pos')
        .sortWithinPartitions('left_chrom', 'left_pos')
    )

    # Write
    (
        df
        .write.parquet(
            out_parquet,
            mode='overwrite'
        )
    )

    return 0

def drop_duplicates_keep_first(df, subset, order_colname, ascending=True):
    ''' Implements the equivalent pd.drop_duplicates(keep='first')
    Args:
        df (spark df)
        subset (list): columns to partition by
        order_colname (str): column to sort by
        ascending (bool): whether to sort ascending
    Returns:
        df
    '''
    assert isinstance(subset, list)

    # Get order column ascending or descending
    if ascending:
        order_col = col(order_colname)
    else:
        order_col = col(order_colname).desc()

    # Specfiy window spec
    window = Window.partitionBy(*subset).orderBy(
        order_col, 'tiebreak')
    # Select first
    res = (
        df
        .withColumn('tiebreak', monotonically_increasing_id())
        .withColumn('rank', rank().over(window))
        .filter(col('rank') == 1)
        .drop('rank', 'tiebreak')
    )
    return res

def load_pheno_to_gene_map(infs):
    ''' Loads a dictionary, mapping phenotype_ids to ensembl gene IDs.
        Input files should have 2 columns phenotype_id, gene_id
    '''
    d = {}

    for inf in glob(infs):

        with gzip.open(inf, 'r') as in_h:

            # Skip header
            header = (
                in_h.readline()
                    .decode()
                    .rstrip()
                    .split('\t')
            )

            # Load each line into dict
            for line in in_h:
                parts = line.decode().rstrip().split('\t')
                if not parts[header.index('gene_id')].startswith('ENSG'):
                    continue
                d[parts[header.index('phenotype_id')]] = \
                    parts[header.index('gene_id')]
    
    return d

if __name__ == '__main__':

    main()
