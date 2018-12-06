#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import dask.dataframe as dd
import numpy as np
import pandas as pd
from collections import OrderedDict

def load_sumstats(in_pq, study_id, cell_id=None, group_id=None, trait_id=None,
                  chrom=None, start=None, end=None, excl_mhc=None, min_maf=None,
                  build='b37', logger=None):
    ''' Loads summary statistics from Open Targets parquet format:
        - Loads only required rows
        - Converts to pandas
        - Extracts N samples, N cases, EAF
        - TODO only extract required columns
        - TODO extract eaf
    Args:
        excl_mhc (b37|b38|None): whether to exclude the MHC region
        build (b37|b38): which build to use
    '''

    #
    # Load
    #

    # Create row-group filters
    row_grp_filters = [('study_id', '==', study_id)]
    if cell_id:
        row_grp_filters.append(('cell_id', '==', cell_id))
    if group_id:
        row_grp_filters.append(('group_id', '==', group_id))
    if trait_id:
        row_grp_filters.append(('trait_id', '==', trait_id))
    if chrom:
        row_grp_filters.append(('chrom', '==', str(chrom)))
    if start:
        row_grp_filters.append(('pos_{}'.format(build), '>=', start))
    if end:
        row_grp_filters.append(('pos_{}'.format(build), '<=', end))

    # Create column filters
    # TODO

    # Read file
    df = dd.read_parquet(in_pq,
                         filters=row_grp_filters,
                         engine='fastparquet')

    # Conversion to in-memory pandas
    df = df.compute(scheduler='single-threaded') # DEBUG
    # df = df.astype(dtype=get_meta_info(type='sumstats'))

    # Apply row filters
    query = ' & '.join(
        ['{} {} {}'.format(
            filter[0],
            filter[1],
            '"{}"'.format(filter[2]) if isinstance(filter[2], str) else filter[2])
         for filter in row_grp_filters] )
    df = df.query(query)

    #
    # Extract fields
    #

    # Extract n_samples
    df['n_samples'] = np.where(pd.isnull(df['n_samples_variant_level']),
                               df['n_samples_study_level'],
                               df['n_samples_variant_level'])
    # Extract n_cases
    df['n_cases'] = np.where(pd.isnull(df['n_cases_variant_level']),
                             df['n_cases_study_level'],
                             df['n_cases_variant_level'])

    # Extract EAF. TODO this needs to be changed to use estimated EAF if EAF_est
    if pd.isnull(df['eaf']).any():
        if logger:
            logger.warning('Warning: using MAF instead of EAF')
    df['eaf'] = np.where(pd.isnull(df['eaf']),
                             df['maf'],
                             df['eaf'])

    # Extract required build
    df['variant_id'] = df['variant_id_{0}'.format(build)]
    df['pos'] = df['pos_{0}'.format(build)]

    #
    # Make exclusions
    #

    # Exclude on MAF
    if min_maf:
        to_exclude = ( df['eaf'].apply(eaf_to_maf) < min_maf )
        df = df.loc[~to_exclude, :]

    # Exclude MHC
    if excl_mhc and (df.chrom == '6').any():
        # Exclude MHC
        if excl_mhc == 'b37':
            is_mhc = ( (df['chrom'] == '6') &
                       (df['pos_b37'] >= 28477797) &
                       (df['pos_b37'] <= 33448354) )
            df = df.loc[~is_mhc, :]
        elif excl_mhc == 'b38':
            is_mhc = ( (df['chrom'] == '6') &
                       (df['pos_b38'] >= 28510120) &
                       (df['pos_b38'] <= 33480577) )
            df = df.loc[~is_mhc, :]

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
