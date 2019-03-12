#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import pyspark.sql
from pyspark.sql.functions import col
import numpy as np
import pandas as pd
from collections import OrderedDict
import os

# import dask.dataframe as dd
# def load_sumstats(in_pq, study_id, phenotype_id=None, biofeature=None,
#                   chrom=None, start=None, end=None, min_maf=None, logger=None):
#     ''' Loads summary statistics from Open Targets parquet format using Dask:
#         - Loads only required rows
#         - Converts to pandas
#         - Only extract required columns
#     '''

#     #
#     # Load
#     #

#     # Create row-group filters
#     row_grp_filters = [('study_id', '==', study_id)]
#     if phenotype_id:
#         row_grp_filters.append(('phenotype_id', '==', phenotype_id))
#     if chrom:
#         row_grp_filters.append(('chrom', '==', str(chrom)))
#     if start:
#         row_grp_filters.append(('pos', '>=', start))
#     if end:
#         row_grp_filters.append(('pos', '<=', end))

#     # Add biofeature to path
#     if biofeature:
#         in_pq = os.path.join(in_pq, 'biofeature={}'.format(biofeature))

#     # Create column filters
#     cols_to_keep = ['study_id', 'phenotype_id', 'chrom', 'pos',
#                     'ref', 'alt', 'beta', 'se', 'pval', 'n_total', 'n_cases',
#                     'eaf', 'is_cc']

#     # Read file
#     in_pq_pattern = os.path.join(in_pq, '*.parquet')
#     df = dd.read_parquet(in_pq_pattern,
#                          columns=cols_to_keep,
#                          filters=row_grp_filters,
#                          engine='fastparquet')

#     # Conversion to in-memory pandas
#     df = df.compute(scheduler='single-threaded')

#     # Apply row filters
#     query_parts = []
#     for part in row_grp_filters:
#         if isinstance(part[2], int):
#             query_parts.append('{} {} {}'.format(part[0], part[1], part[2]))
#         else:
#             query_parts.append('{} {} "{}"'.format(part[0], part[1], part[2]))
#     query = ' & '.join(query_parts)
#     df = df.query(query)

#     # Add biofeature back in
#     df.loc[:, 'biofeature'] = biofeature

#     #
#     # Make exclusions
#     #

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

def load_sumstats(in_pq, study_id, phenotype_id=None, biofeature=None,
                  chrom=None, start=None, end=None, min_maf=None, logger=None):
    ''' Loads summary statistics from Open Targets parquet format using Spark
    '''

    # Get spark session
    spark = (
        pyspark.sql
        .SparkSession
        .builder
        .master("local")
        .getOrCreate()
    )

    # Create column filters
    cols_to_keep = ['study_id', 'phenotype_id', 'biofeature', 'chrom', 'pos',
                    'ref', 'alt', 'beta', 'se', 'pval', 'n_total', 'n_cases',
                    'eaf', 'is_cc']

    # Load
    df_spark = (
        spark.read.parquet(in_pq)
        .select(cols_to_keep)
    )

    # Apply required filters
    df_spark = (
        df_spark.filter(
            (col('study_id') == study_id) &
            (col('chrom') == chrom) &
            (col('pos') >= start) &
            (col('pos') <= end)
        )
    )

    # Apply optional filters
    if phenotype_id:
        df_spark = df_spark.filter(col('phenotype_id') == phenotype_id)
    if biofeature:
        df_spark = df_spark.filter(col('biofeature') == biofeature)

    # Convert to pandas
    df = df_spark.toPandas()

    # Exclude on MAF
    if min_maf:
        to_exclude = (df['eaf'].apply(eaf_to_maf) < min_maf)
        df = df.loc[~to_exclude, :]

    # Create a variant ID
    df['variant_id'] = (
        df.loc[:, ['chrom', 'pos', 'ref', 'alt']]
        .apply(lambda row: ':'.join([str(x) for x in row]), axis=1)
    )

    print(df.columns)

    return df

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
