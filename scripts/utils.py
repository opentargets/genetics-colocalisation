#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#


import dask.dataframe as dd
import numpy as np
import pandas as pd
from collections import OrderedDict
import os

def load_sumstats(in_pq, study_id, phenotype_id=None, bio_feature=None,
                  chrom=None, start=None, end=None, min_maf=None, logger=None):
    ''' Loads summary statistics from Open Targets parquet format using Dask:
        - Loads only required rows
        - Converts to pandas
        - Only extract required columns
    '''

    #
    # Load
    #

    # Create row-group filters
    row_grp_filters = [('study_id', '==', study_id)]
    if phenotype_id:
        row_grp_filters.append(('phenotype_id', '==', phenotype_id))
    if bio_feature:
        row_grp_filters.append(('bio_feature', '==', bio_feature))
    if chrom:
        row_grp_filters.append(('chrom', '==', str(chrom)))
    if start:
        row_grp_filters.append(('pos', '>=', start))
    if end:
        row_grp_filters.append(('pos', '<=', end))

    # Create column filters
    cols_to_keep = ['study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos',
                    'ref', 'alt', 'beta', 'se', 'pval', 'n_total', 'n_cases',
                    'eaf', 'is_cc']

    # Read file
    df = dd.read_parquet(in_pq,
                         columns=cols_to_keep,
                         filters=row_grp_filters,
                         split_row_groups=True,
                         engine='fastparquet')

    # Conversion to in-memory pandas
    df = df.compute(scheduler='single-threaded')
    
    if logger:
        logger.info('    Dask read in {0} variants'.format(df.shape[0]))

    # Make sure that chrom is str
    df['chrom'] = df['chrom'].astype('str')
    #df['chrom'] = str(chrom)

    # Print first few rows of df
    if logger:
        logger.info('    First few rows of Dask df:')
        logger.info(df.head())
        logger.info('    Last few rows of Dask df:')
        logger.info(df.tail())

    # Apply row filters
    if study_id:
        df = df.loc[df['study_id'] == study_id, :]
    if bio_feature:
        df = df.loc[df['bio_feature'] == bio_feature, :]
    if phenotype_id:
        df = df.loc[df['phenotype_id'] == phenotype_id, :]
    if chrom:
        df = df.loc[df['chrom'] == chrom, :]
    if start:
        df = df.loc[df['pos'] >= start, :]
    if end:
        df = df.loc[df['pos'] <= end, :]

    if logger:
        logger.info('    ...{0} variants after row filters'.format(df.shape[0]))

    #
    # Make exclusions
    #

    # Exclude on MAF
    if min_maf:
        to_exclude = (df['eaf'].apply(eaf_to_maf) < min_maf)
        df = df.loc[~to_exclude, :]

    # Create a variant ID
    df['variant_id'] = (
        df.loc[:, ['chrom', 'pos', 'ref', 'alt']]
        .apply(lambda row: ':'.join([str(x) for x in row]), axis=1)
    )

    return df

# import pyspark.sql
# from pyspark.sql.functions import col
# def load_sumstats(in_pq, study_id, phenotype_id=None, bio_feature=None,
#                   chrom=None, start=None, end=None, min_maf=None, logger=None):
#     ''' Loads summary statistics from Open Targets parquet format using Spark
#     '''

#     # Get spark session
#     spark = (
#         pyspark.sql
#         .SparkSession
#         .builder
#         .master("local")
#         .getOrCreate()
#     )

#     # Create column filters
#     cols_to_keep = ['study_id', 'phenotype_id', 'bio_feature', 'chrom', 'pos',
#                     'ref', 'alt', 'beta', 'se', 'pval', 'n_total', 'n_cases',
#                     'eaf', 'is_cc']

#     # Load
#     df_spark = (
#         spark.read.parquet(in_pq)
#         .select(cols_to_keep)
#     )

#     # Apply required filters
#     df_spark = (
#         df_spark.filter(
#             (col('study_id') == study_id) &
#             (col('chrom') == chrom) &
#             (col('pos') >= start) &
#             (col('pos') <= end)
#         )
#     )

#     # Apply optional filters
#     if phenotype_id:
#         df_spark = df_spark.filter(col('phenotype_id') == phenotype_id)
#     if bio_feature:
#         df_spark = df_spark.filter(col('bio_feature') == bio_feature)

#     # Convert to pandas
#     df = df_spark.toPandas()

#     # Exclude on MAF
#     if min_maf:
#         to_exclude = (df['eaf'].apply(eaf_to_maf) < min_maf)
#         df = df.loc[~to_exclude, :]

#     # Create a variant ID
#     df['variant_id'] = (
#         df.loc[:, ['chrom', 'pos', 'ref', 'alt']]
#         .apply(lambda row: ':'.join([str(x) for x in row]), axis=1)
#     )

#     return df

def eaf_to_maf(eaf):
    ''' Convert effect allele frequency to MAF
    '''
    eaf = float(eaf)
    if eaf <= 0.5:
        return eaf
    else:
        return 1 - eaf

def extract_window(sumstats, chrom, pos, window):
    ''' Extracts a window around a genomic position
    Args:
        sumstats (pd.df)
        chrom (str)
        pos (int)
        window (int): kb around pos to extract
    Returns:
        pd.df
    '''
    in_window = ( (sumstats['chrom'] == chrom) &
                  (sumstats['pos'] >= pos - 1000 * window) &
                  (sumstats['pos'] <= pos + 1000 * window) )
    sumstat_wind = sumstats.loc[in_window, :]

    return sumstat_wind
