#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import os
import sys
import pandas as pd
from pprint import pprint
from glob import glob

def main():

    # Parse args
    in_overlap_table = glob('tmp/overlap_table.json/*.json')
    outf = 'configs/manifest_file.tsv'
    prop_threshold = 0.01
    ld_path = ''

    #

    return 0

if __name__ == '__main__':

    main()

# #
# # Load and filter ----------------------------------------------------------
# #

# # Load
# df = pd.read_csv(inf, sep='\t', compression='gzip')

# # Only keep rows where A is a disease and B is a molecular QTL
# # and proportion of overlapping variants greater than threshold
# print('Number of tests pre-filter: {0}'.format(df.shape[0]))
# to_keep = ((pd.isnull(df.cell_id_left)) &
#             (~pd.isnull(df.cell_id_right)) &
#             (df.proportion_overlap > prop_threshold))
# df = df.loc[to_keep, :]
# print('Number of tests post-filter: {0}'.format(df.shape[0]))

# #
# # Prepare manifest ---------------------------------------------------------
# #

# print(df.columns)

# manifest = pd.DataFrame()

# # Add dataset parameters
# for prefix in ['left', 'right']:
#     # Add sumstat lociation
#     manifest['sumstat_{}'.format(prefix)] = df['study_id_{}'.format(prefix)].apply(
#         lambda x: os.path.join('/Users/em21/Projects/genetics-finemapping/input/sumstats', x))
#     # Add study identifiers
#     for col in ['study', 'cell', 'group', 'trait']:
#         manifest['{}_{}'.format(col, prefix)
#                     ] = df['{}_id_{}'.format(col, prefix)]
#     # Add variant identifiers
#     for col in ['chrom', 'pos', 'ref', 'alt']:
#         manifest['{}_{}'.format(col, prefix)
#                     ] = df['{}_index_{}'.format(col, prefix)]
#     # Add location of the ld reference
#     manifest['ld_{}'.format(prefix)] = ld_path

# # Add method
# manifest['method'] = 'conditional'
# # Add output path
# manifest['out'] = manifest.apply(lambda row: os.path.abspath(
#     'output/study_left={study_left}/cell_left={cell_left}/group_left={group_left}/trait_left={trait_left}/variant_left={chrom_left}_{pos_left}_{ref_left}_{alt_left}/study_right={study_right}/cell_right={cell_right}/group_right={group_right}/trait_right={trait_right}/variant_right={chrom_right}_{pos_right}_{ref_right}_{alt_right}/coloc_res.json'.format(**row.fillna('').to_dict())), axis=1)
# # manifest['plot'] = manifest.apply(lambda row: os.path.abspath('output/study_left={study_left}/cell_left={cell_left}/group_left={group_left}/trait_left={trait_left}/variant_left={chrom_left}_{pos_left}_{ref_left}_{alt_left}/study_right={study_right}/cell_right={cell_right}/group_right={group_right}/trait_right={trait_right}/variant_right={chrom_right}_{pos_right}_{ref_right}_{alt_right}/coloc_plot.png'.format(**row.fillna('').to_dict())), axis=1)
# manifest['plot'] = manifest.apply(lambda row: os.path.abspath(
#     'plots/{study_left}_{cell_left}_{group_left}_{trait_left}_{chrom_left}_{pos_left}_{ref_left}_{alt_left}_{study_right}_{cell_right}_{group_right}_{trait_right}_{chrom_right}_{pos_right}_{ref_right}_{alt_right}.png'.format(**row.fillna('').to_dict())), axis=1)
# manifest['log'] = manifest.apply(lambda row: os.path.abspath(
#     'logs/study_left={study_left}/cell_left={cell_left}/group_left={group_left}/trait_left={trait_left}/variant_left={chrom_left}_{pos_left}_{ref_left}_{alt_left}/study_right={study_right}/cell_right={cell_right}/group_right={group_right}/trait_right={trait_right}/variant_right={chrom_right}_{pos_right}_{ref_right}_{alt_right}/log_file.txt'.format(**row.fillna('').to_dict())), axis=1)
# manifest['tmpdir'] = manifest.apply(lambda row: os.path.abspath(
#     'tmp/study_left={study_left}/cell_left={cell_left}/group_left={group_left}/trait_left={trait_left}/variant_left={chrom_left}_{pos_left}_{ref_left}_{alt_left}/study_right={study_right}/cell_right={cell_right}/group_right={group_right}/trait_right={trait_right}/variant_right={chrom_right}_{pos_right}_{ref_right}_{alt_right}/log_file.txt'.format(**row.fillna('').to_dict())), axis=1)

# # pprint(manifest.head(2).to_dict(orient='records'))

# #
# # Write --------------------------------------------------------------------
# #

# # Write manifest
# manifest.head(20).fillna('None').to_csv(
#     outf, sep='\t', index=None, header=None)
# # manifest.fillna('None').to_csv(outf, sep='\t', index=None, header=None)

# # Print columns
# for i, col in enumerate(manifest.columns):
#     print(i, col)

# return 0
