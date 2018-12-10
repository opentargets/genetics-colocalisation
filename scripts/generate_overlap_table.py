#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Creates overlap table
#

import os
import sys
from pprint import pprint
import argparse
import pandas as pd

def main():

    # Parse args
    args = parse_args()

    #
    # Load and filter data
    #

    # Load credible set data
    cred_set = pd.read_parquet(args.in_credset,
                               engine='fastparquet')#.head(1000)

    # Filter on credible set type
    to_keep = cred_set['is{0}_credset'.format(args.which_set)]
    cred_set = cred_set.loc[to_keep, :]

    # Filter on method type
    if args.which_method != 'all':
        to_keep = cred_set['multisignal_method'] == args.which_method
        cred_set = cred_set.loc[to_keep, :]

    #
    # Convert data into dictionary of {(study key, lead var): set([tag vars])}
    #

    # Extract index variant info: chrom, pos, ref, alt
    ( cred_set['chrom_index'],
      cred_set['pos_index'],
      cred_set['ref_index'],
      cred_set['alt_index'] ) = cred_set.variant_id_index.str.split('_').str

    cred_set['pos_index'] = cred_set['pos_index'].astype(int)

    # Create a key column
    key_cols = ['study_id', 'cell_id', 'gene_id', 'group_id', 'trait_id',
                'chrom_index', 'pos_index', 'ref_index', 'alt_index']
    cred_set['key'] = cred_set[key_cols].values.tolist()
    cred_set['key'] = cred_set['key'].apply(tuple)

    # Group by key column and aggregate tag variants into a set
    tag_df = cred_set.groupby('key').variant_id_tag.agg(set).reset_index()
    tag_dict = pd.Series(tag_df.variant_id_tag.values,
                         index=tag_df.key).to_dict()

    #
    # Find overlaps ------------------------------------------------------------
    #

    # Find overlaps
    print('Finding overlaps...')
    overlap_data = []
    header = ['key_left', 'key_right', 'distinct_left', 'overlap', 'distinct_right']

    # Partition by chromosome to speed things up
    tag_dict_chroms = {}
    for key in tag_dict:
        chrom, _ = parse_chrom_pos(key)
        try:
            tag_dict_chroms[chrom][key] = tag_dict[key]
        except KeyError:
            tag_dict_chroms[chrom] = {}
            tag_dict_chroms[chrom][key] = tag_dict[key]

    # Run each chromosome separately
    window = args.window * 1e6
    c = 0
    for chrom in tag_dict_chroms:
        tag_dict_chrom = tag_dict_chroms[chrom]
        for key_A in tag_dict_chrom.keys():
            if c % 1000 == 0:
                print(' processing {0} of {1}...'.format(c, len(tag_dict)))
            c += 1
            for key_B in tag_dict_chrom.keys():
                if key_A != key_B and varids_overlap_window(key_A, key_B, window):
                    # Find overlap in sets
                    distinct_A = tag_dict_chrom[key_A].difference(
                        tag_dict_chrom[key_B])
                    overlap_AB = tag_dict_chrom[key_A].intersection(
                        tag_dict_chrom[key_B])
                    distinct_B = tag_dict_chrom[key_B].difference(
                        tag_dict_chrom[key_A])
                    # Save result
                    if len(overlap_AB) > 0:
                        out_row = [key_A,
                                   key_B,
                                   len(distinct_A),
                                   len(overlap_AB),
                                   len(distinct_B)]
                        overlap_data.append(out_row)

    #
    # Save output --------------------------------------------------------------
    #

    # Convert to dataframe
    res = pd.DataFrame(overlap_data, columns=header)

    # Split keys into separate columns
    for suffix in ['left', 'right']:
        ( res['study_id_{0}'.format(suffix)],
          res['cell_id_{0}'.format(suffix)],
          res['gene_id_{0}'.format(suffix)],
          res['group_id_{0}'.format(suffix)],
          res['trait_id_{0}'.format(suffix)],
          res['chrom_index_{0}'.format(suffix)],
          res['pos_index_{0}'.format(suffix)],
          res['ref_index_{0}'.format(suffix)],
          res['alt_index_{0}'.format(suffix)] ) = res['key_{0}'.format(suffix)].str
        res = res.drop(columns='key_{0}'.format(suffix))

    # Put counts at end
    cols = list(res.columns.values)
    res = res.loc[:, cols[3:] + cols[:3]]

    # Calculate proportion overlapping
    total_var_n = (res[['distinct_left', 'overlap', 'distinct_right']]).sum(axis=1)
    res['proportion_overlap'] = res['overlap'] / total_var_n

    # Write to tsv
    os.makedirs(os.path.dirname(args.outf), exist_ok=True)
    res.to_csv(args.outf, sep='\t', index=None, compression='gzip')

    # Write to json
    # res.to_json(args.outf, orient='split', compression='gzip')

    return 0

def parse_chrom_pos(key):
    ''' Gets chrom and pos from a variant ID
    Returns:
        (chrom, pos)
    '''
    return key[5], key[6]

def varids_overlap_window(key_A, key_B, window):
    ''' Extracts chrom:pos info from two variant IDs and checks if they are
        within a certain window of each other
    Args:
        var_A (chr_pos_a1_a2)
        var_B (chr_pos_a1_a2)
        window (int): bp window to consider an overlap
    '''
    # Get positional info
    chrom_A, pos_A = parse_chrom_pos(key_A)
    chrom_B, pos_B = parse_chrom_pos(key_B)
    # Check chroms are the same
    if not chrom_A == chrom_B:
        return False
    # Check window
    if abs(int(pos_A) - int(pos_B)) > window:
        return False
    else:
        return True

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_credset',
                        metavar="<parquet>",
                        type=str,
                        required=True)
    parser.add_argument('--window',
                        metavar="<int>",
                        type=int,
                        required=True,
                        default=5,
                        help='Window (in Mb) around lead variant to look for overlapping signals (default: 5)')
    parser.add_argument('--which_set',
                        metavar="<95|99>",
                        type=str,
                        required=True,
                        default='95',
                        choices=['95', '99'],
                        help='Whether to use the 95%% or 99%% credible sets (default: 95)')
    parser.add_argument('--which_method',
                        metavar="<all|conditional|distance>",
                        type=str,
                        required=True,
                        default='all',
                        choices=['all', 'conditional', 'distance'],
                        help='Only use credible sets derived using this method (default: all)')
    parser.add_argument('--outf',
                        metavar="<tsv>",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
