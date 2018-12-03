#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

# import sys
# import os
# import gzip
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
                               engine='fastparquet').head(1000)

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

    # Create a key column
    key_cols = ['study_id', 'cell_id', 'gene_id', 'group_id', 'trait_id',
                'chrom_index', 'pos_index', 'ref_index', 'alt_index']
    cred_set['key'] = cred_set[key_cols].values.tolist()
    cred_set['key'] = cred_set['key'].apply(tuple)

    # Group by key column and aggregate tag variants into a set
    tag_df = cred_set.groupby('key').variant_id_tag.agg(set).reset_index()
    tag_dict = pd.Series(tag_df.variant_id_tag.values,
                         index=tag_df.key).to_dict()

    


#     #
#     # Prepare data -------------------------------------------------------------
#     #
#
#     # Load set of valid (study, index) pairs
#     study_index_set = set([])
#     with open(args.top_loci, 'r') as in_h:
#         in_h.readline() # Skip header
#         for line in in_h:
#             study_id, index_var = line.rstrip().split('\t')[:2]
#             study_index_set.add((study_id, index_var))
#
#     # Load finemap data
#     print('Loading finemap...')
#     tag_dict_finemap = {}
#     with gzip.open(args.finemap, 'r') as in_h:
#         in_h.readline() # Skip header
#         for line in in_h:
#             line = line.decode()
#             study_id, index_var, tag_var, _, _ = line.rstrip().split('\t')
#             key = (study_id, index_var)
#             # Skip (stid, index) pairs that are not in the top loci table
#             if not key in study_index_set:
#                 continue
#             # Add to dict
#             try:
#                 tag_dict_finemap[key].add(tag_var)
#             except KeyError:
#                 tag_dict_finemap[key] = set([tag_var])
#
#     # Load LD data
#     print('Loading LD...')
#     tag_dict_ld = {}
#     with gzip.open(args.ld, 'r') as in_h:
#         in_h.readline() # Skip header
#         for line in in_h:
#             line = line.decode()
#             study_id, index_var, tag_var, r2, *_ = line.rstrip().split('\t')
#             key = (study_id, index_var)
#             # Skip (stid, index) pairs that are not in the top loci table
#             if not key in study_index_set:
#                 continue
#             # Skip low R2
#             if float(r2) < min_r2:
#                 continue
#             # Add to dict
#             try:
#                 tag_dict_ld[key].add(tag_var)
#             except KeyError:
#                 tag_dict_ld[key] = set([tag_var])
#
#     # Merge finemap and LD. This will select finemapping over LD if available.
#     print('Merging finemap and LD...')
#     tag_dict = {}
#     for d in [tag_dict_finemap, tag_dict_ld]:
#         for key in d:
#             if not key in tag_dict:
#                 tag_dict[key] = d[key]
#
#     #
#     # Find overlaps ------------------------------------------------------------
#     #
#
#     # Find overlaps
#     print('Finding overlaps...')
#     overlap_data = []
#     header = ['study_id_A',
#               'index_variantid_b37_A',
#               'study_id_B',
#               'index_variantid_b37_B',
#               'set_type',
#               'distinct_A',
#               'overlap_AB',
#               'distinct_B']
#
#     # set_types = {'finemapping': tag_dict_finemap,
#     #              'ld_eur': tag_dict_ld,
#     #              'combined': tag_dict}
#     set_types = {'combined': tag_dict}
#
#     # Process each set type separately
#     for set_key in set_types:
#
#         set_dict = set_types[set_key]
#
#         # Partition by chromosome to speed things up
#         set_dict_chroms = {}
#         for key in set_dict:
#             chrom, _ = parse_chrom_pos(key[1])
#             try:
#                 set_dict_chroms[chrom][key] = set_dict[key]
#             except KeyError:
#                 set_dict_chroms[chrom] = {}
#                 set_dict_chroms[chrom][key] = set_dict[key]
#
#         # Run each chromosome separately
#         c = 0
#         for chrom in set_dict_chroms:
#             set_dict_chrom = set_dict_chroms[chrom]
#
#             for study_A, var_A in set_dict_chrom.keys():
#                 if c % 1000 == 0:
#                     print(' processing {0} {1} of {2}...'.format(set_key, c, len(set_dict)))
#                 c += 1
#                 for study_B, var_B in set_dict_chrom.keys():
#                     if varids_overlap_window(var_A, var_B, window):
#                         # Find overlap in sets
#                         distinct_A = set_dict_chrom[(study_A, var_A)].difference(set_dict_chrom[(study_B, var_B)])
#                         overlap_AB = set_dict_chrom[(study_A, var_A)].intersection(set_dict_chrom[(study_B, var_B)])
#                         distinct_B = set_dict_chrom[(study_B, var_B)].difference(set_dict_chrom[(study_A, var_A)])
#                         # Save result
#                         if len(overlap_AB) > 0 or only_save_overlapping == False:
#                             out_row = [study_A,
#                                        var_A,
#                                        study_B,
#                                        var_B,
#                                        set_key,
#                                        len(distinct_A),
#                                        len(overlap_AB),
#                                        len(distinct_B)]
#                             overlap_data.append(out_row)
#
#     # Write results
#     with gzip.open(args.outf, 'w') as out_h:
#         # Write header
#         out_h.write(('\t'.join(header) + '\n').encode())
#         for row in overlap_data:
#             out_h.write(('\t'.join([str(x) for x in row]) + '\n').encode())
#
# def parse_chrom_pos(varid):
#     ''' Gets chrom and pos from a variant ID
#     Returns:
#         (chrom, pos)
#     '''
#     chrom, pos = varid.split('_')[:2]
#     return chrom, pos
#
# def varids_overlap_window(var_A, var_B, window):
#     ''' Extracts chrom:pos info from two variant IDs and checks if they are
#         within a certain window of each other
#     Args:
#         var_A (chr_pos_a1_a2)
#         var_B (chr_pos_a1_a2)
#         window (int): bp window to consider an overlap
#     '''
#     # Get positional info
#     chrom_A, pos_A = parse_chrom_pos(var_A)
#     chrom_B, pos_B = parse_chrom_pos(var_B)
#     # Check chroms are the same
#     if not chrom_A == chrom_B:
#         return False
#     # Check window
#     if abs(int(pos_A) - int(pos_B)) > window:
#         return False
#     else:
#         return True

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
                        metavar="<parquet>",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
