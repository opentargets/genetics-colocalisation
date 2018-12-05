#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import os
import argparse
import logging
from pprint import pprint
# import pandas as pd
# import time
# import subprocess as sp

def main():

    # Args
    global args
    args = parse_args()
    pprint(vars(args))

    # # Make temp and output dirs
    # os.makedirs(args.outpref, exist_ok=True)
    #
    # # Start log
    # log_file = '{0}/log.txt'.format(args.outpref)
    # logging.basicConfig(filename=log_file,
    #                     format='%(asctime)s: %(message)s',
    #                     datefmt='%m/%d/%Y %H:%M:%S',
    #                     filemode='w',
    #                     level=logging.INFO)
    # logging.info('Started')
    # logging.info(sys.argv)
    # logging.info(' '.join(sys.argv))
    #
    # # Check inputs exist
    # infs = [args.left_sumstats, args.right_sumstats]
    # if args.vloc:
    #     infs.append(args.bgen)
    # for inf in infs:
    #     if not os.path.exists(inf):
    #         logging.info('Input file not found: {0}'.format(inf))
    #         sys.exit()
    #
    # #
    # # Prepare sumstat files ----------------------------------------------------
    # #
    #
    # # Load sumstats
    # left_ss = parse_sumstats(args.left_sumstats, args.pos, args.window_kb)
    # logging.info('left sumstats file contains {0} variants in region'.format(left_ss.shape[0]))
    # right_ss = parse_sumstats(args.right_sumstats, args.pos, args.window_kb)
    # logging.info('right sumstats file contains {0} variants in region'.format(right_ss.shape[0]))
    #
    # # Make a merge key
    # left_ss['merge_key'] = make_merge_key(left_ss, ['chrom', 'pos_b37', 'ref_al', 'alt_al'])
    # right_ss['merge_key'] = make_merge_key(right_ss, ['chrom', 'pos_b37', 'ref_al', 'alt_al'])
    # left_ss.index = left_ss.merge_key
    # right_ss.index = right_ss.merge_key
    #
    # # Take intersection
    # shared_vars = sorted(set(left_ss.index).intersection(set(right_ss.index)))
    # left_ss = left_ss.loc[shared_vars, :]
    # right_ss = right_ss.loc[shared_vars, :]
    # logging.info('left-right intersection contains {0} variants in region'.format(right_ss.shape[0]))
    #
    # #
    # # Prepare covariance matrix ------------------------------------------------
    # #
    #
    # # Covariance matrix only required if using Zuber et al's method
    # if args.vloc:
    #
    #     logging.info('creating ldstore cov matrix')
    #     cov_mat, eff_dict = create_covariance_matrix(args.bgen, args.pos, args.window_kb)
    #     logging.info('cov matrix file contains {0} variants in region'.format(cov_mat.shape))
    #
    #     # Take intersection between cov_mat and ss files
    #     shared_vars = sorted(set(cov_mat.index).intersection(set(left_ss.merge_key)))
    #     left_ss = left_ss.loc[shared_vars, :]
    #     right_ss = right_ss.loc[shared_vars, :]
    #     cov_mat = cov_mat.loc[shared_vars, shared_vars]
    #     logging.info('left-right-covmatrix intersection contains {0} variants in region'.format(cov_mat.shape[0]))
    #
    #     # Harmonise the alleles between sumstats and covmatrix
    #     cov_eff_al = pd.Series([eff_dict[key] for key in shared_vars])
    #     harmonise_vec = (cov_eff_al.reset_index(drop=True) == left_ss.alt_al.reset_index(drop=True)).replace({True: 1, False:-1})
    #     harmonise_vec.index = shared_vars
    #     cov_mat = cov_mat.multiply(harmonise_vec, axis=0)
    #     cov_mat = cov_mat.multiply(harmonise_vec, axis=1)
    #
    # # Write to temp dir
    # logging.info('writing files')
    # left_ss.to_csv(args.outpref + '/left_ss.tsv.gz', sep='\t', index=None, compression='gzip')
    # right_ss.to_csv(args.outpref + '/right_ss.tsv.gz', sep='\t', index=None, compression='gzip')
    # if args.vloc:
    #     cov_mat.to_csv(args.outpref + '/ldstore_cov.mat.hm.tsv', sep='\t')
    #
    # #
    # # Run coloc ----------------------------------------------------------------
    # #
    #
    # logging.info('running coloc')
    # outpref = args.outpref + '/coloc'
    # run_coloc(args.outpref + '/left_ss.tsv.gz',
    #           args.outpref + '/right_ss.tsv.gz',
    #           outpref)
    #
    # # Check output exists
    # outf = outpref + '.pp.tsv'
    # if not os.path.exists(outf):
    #     logging.info('Output file not found: {0}'.format(outf))
    #     sys.exit()
    #
    # # Touch COMPLETE
    # touch(args.outpref + '/COMPLETE')
    #
    # logging.info('Finished!')
    #
    # return 0

# def run_coloc(left_ss_path, right_ss_path, outpref):
#     ''' Runs coloc.abf
#     '''
#
#     # Make command
#     cmd = [
#         'Rscript',
#         'scripts/coloc.R',
#         left_ss_path,
#         right_ss_path,
#         outpref
#     ]
#     # Run
#     cmd = ' '.join([str(x) for x in cmd])
#     sp.run(cmd, shell=True)
#
#
# def create_covariance_matrix(bgen, pos, window):
#     ''' Uses LDstore to make a covariance matrix for variants in defined region
#     Args:
#         bgen (file): input bgen file
#         pos (int): centre position to extract from
#         window (int): window around pos to extract (kb)
#     returns:
#         (pd.Df, dict): (covariance matrix, dict of effect alleles)
#     '''
#     range = '{}-{}'.format(pos - window * 1000, pos + window * 1000)
#
#     # Create ldstore command
#     outpref = args.outpref + '/ldstore_cov'
#     cmd = [
#         # Make bcor and merge
#         'ldstore',
#         '--bgen', bgen,
#         '--incl-range', range,
#         '--bcor', '{0}.bcor'.format(outpref),
#         '--n-threads', 1,
#         ';',
#         'ldstore',
#         '--bcor', '{0}.bcor'.format(outpref),
#         '--merge', 1,
#         ';',
#         'rm', '-f', '{0}.bcor_*'.format(outpref),
#         ';',
#         # Extract LD matrix
#         'ldstore',
#         '--bcor', '{0}.bcor'.format(outpref),
#         '--matrix', '{0}.mat.tsv'.format(outpref),
#         ';',
#         'ldstore',
#         '--bcor', '{0}.bcor'.format(outpref),
#         '--meta', '{0}.meta.tsv'.format(outpref)
#     ]
#
#     # Run LDstore
#     logging.info('running ldstore')
#     cmd = ' '.join([str(x) for x in cmd])
#     # sp.run(cmd, shell=True) # Debug
#
#     # Load cov matrix
#     logging.info('loading covariance matrix')
#     cov_mat, eff_dict = parse_cov_matrix(
#         '{0}.mat.tsv'.format(outpref),
#         '{0}.meta.tsv'.format(outpref)
#     )
#
#     return cov_mat, eff_dict
#
# def make_merge_key(df, cols):
#     ''' Makes a key to merge variant IDs on when alleles are not harmonised
#     Args:
#         df (pd.Df)
#         cols (list): list of columns to use [chrom, pos, allele1, allele2]
#     returns:
#         str
#     '''
#     merge_key = ['_'.join( [str(l[0]), str(l[1])] + sorted([str(l[2]), str(l[3])]) )
#                  for l in df.loc[:, cols].values.tolist()]
#     return merge_key
#
# def parse_cov_matrix(in_cov, in_meta):
#     """ Parses ldstore cov matrix and associated meta data file
#     Args:
#         in_cov (str): file containing matrix of correlations
#         in_meta (str): file containing variant meta-data
#     Returns:
#         cov matrix (dataframe), effect alleles (dict)
#     """
#     cov = pd.read_csv(in_cov, sep=" ", header=None)
#     meta = pd.read_csv(in_meta, sep=" ", header=0)
#
#     # Make a merge key
#     merge_key = make_merge_key(meta, ['chromosome', 'position', 'A_allele', 'B_allele'])
#     cov.index = merge_key
#     meta.index = merge_key
#     cov.columns = merge_key
#
#     # Remove duplicates
#     isdupe = meta.index.duplicated()
#     cov = cov.loc[~isdupe, ~isdupe]
#     meta = meta[~isdupe]
#
#     return cov, meta["A_allele"].to_dict()
#
# def parse_sumstats(inf, pos, window):
#     ''' Parses a summary statistic file
#     '''
#     df = pd.read_csv(inf, sep='\t', header=0)
#     # Extract required window
#     df = df.loc[ ( (df.pos_b37 >= pos - window * 1000) &
#       (df.pos_b37 <= pos + window * 1000) ), :]
#     return df

def parse_args():
    """ Load command line args.
    """
    p = argparse.ArgumentParser()

    # Left input args
    p.add_argument('--sumstat_left',
                   help=("Input: left summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--study_left',
                   help=("Left study_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--cell_left',
                   help=("Left cell_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--group_left',
                   help=("Left group_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--trait_left',
                   help=("Left trait_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--chrom_left',
                   help=("Left chromomsome"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--pos_left',
                   help=("Left position"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--ref_left',
                   help=("Left ref allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--alt_left',
                   help=("Left alt allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--ld_left',
                   help=("Left LD plink reference"),
                   metavar="<str>", type=str, required=False)

    # Right input args
    p.add_argument('--sumstat_right',
                   help=("Input: right summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--study_right',
                   help=("Right study_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--cell_right',
                   help=("Right cell_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--group_right',
                   help=("Right group_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--trait_right',
                   help=("Right trait_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--chrom_right',
                   help=("Right chromomsome"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--pos_right',
                   help=("Right position"),
                   metavar="<int>", type=int, required=False)
    p.add_argument('--ref_right',
                   help=("Right ref allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--alt_right',
                   help=("Right alt allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--ld_right',
                   help=("Right LD plink reference"),
                   metavar="<str>", type=str, required=False)

    # Method parameteres
    p.add_argument('--method',
                   help=("Which method to run (i) conditional analysis"
                         " or (ii) distance based with conditional"),
                   type=str,
                   choices=['conditional', 'distance'],
                   required=True)
    p.add_argument('--window_coloc',
                   help=("Plus/minus window (kb) to perform coloc on"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--window_cond',
                   help=("Plus/minus window (kb) to perform conditional "
                         "analysis on"),
                   metavar="<int>", type=int, required=True)

    # Add output file
    p.add_argument('--out',
                   metavar="<file>",
                   help=("Output: Coloc results"),
                   type=str,
                   required=True)
    p.add_argument('--log',
                   metavar="<file>",
                   help=("Output: log file"),
                   type=str,
                   required=True)
    p.add_argument('--tmpdir',
                   metavar="<file>",
                   help=("Output: temp dir"),
                   type=str,
                   required=True)

    args = p.parse_args()

    # Convert "None" strings to None type
    for arg in vars(args):
        if getattr(args, arg) == "None":
            setattr(args, arg, None)

    return args

if __name__ == '__main__':

    main()
