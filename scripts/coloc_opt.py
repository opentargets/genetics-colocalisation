#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import pandas as pd
import subprocess as sp
import logging
import os
import sys
import argparse
import json
import gzip
from datetime import datetime
from shutil import rmtree


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(process)d - %(filename)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def main(args):
    start_time = datetime.now()

    # Make output and temp directories
    os.makedirs(os.path.split(args.out)[0], exist_ok=True)
    os.makedirs(os.path.split(args.log)[0], exist_ok=True)
    os.makedirs(args.tmpdir , exist_ok=True)

    sumstat_wind_left = pd.read_csv(os.path.join(args.left_sumstat, 'sumstat.tsv.gz'), sep='\t', index_col='variant_id', compression='gzip')
    sumstat_wind_right = pd.read_csv(os.path.join(args.right_sumstat, 'sumstat.tsv.gz'), sep='\t', index_col='variant_id', compression='gzip')

    # Take intersection
    shared_vars = sorted(
        set(sumstat_wind_left.index).intersection(
        set(sumstat_wind_right.index))
    )
    sumstat_int_left = sumstat_wind_left.loc[shared_vars, :]
    sumstat_int_right = sumstat_wind_right.loc[shared_vars, :]
    logger.info('Left-right intersection contains {0} variants'.format(
        sumstat_int_left.shape[0]))

    # Perform coloc
    #
    res = None

    if sumstat_int_left.shape[0] > 0:

        logger.info('Running colocalisation')

        res = run_coloc(sumstat_int_left, sumstat_int_right, args.tmpdir, logger)

        # Add left_ and right_ columns to result file
        for key, val in read_relevant_args(args.left_sumstat).items():
            res['left_' + key] = val

        for key, val in read_relevant_args(args.right_sumstat).items():
            res['right_' + key] = val

        logger.info(' H4={:.3f} and H3={:.3f}'.format(
            res['PP.H4.abf'], res['PP.H3.abf']))


        # Write results
        with gzip.open(args.out, 'w') as out_h:
            out_h.write(json.dumps(res).encode())

    else:
        logger.error('Cannot run coloc with no intersection')

        # Touch empty results file
        touch(args.out)

    # --------------------------------------------------------------------------
    # Make plot if requested
    #

    if args.plot and (sumstat_int_left.shape[0] > 0):

        logger.info('Plotting')


        # Plotting libraries
        

        # Add coloc value to front of filename
        coloc_val = log2(res['PP.H4.abf'] / res['PP.H3.abf'])
        dirname, basename = os.path.split(args.plot)
        basename = '{0:.2f}_{1}'.format(coloc_val, basename)
        out_plot_name = os.path.join(dirname, basename)
        
        # Make output folder and run
        os.makedirs(dirname, exist_ok=True)
        run_make_coloc_plot(sumstat_int_left,
                            sumstat_int_right,
                            res['PP.H4.abf'],
                            res['PP.H3.abf'],
                            out_plot_name)

    # --------------------------------------------------------------------------
    # Output results
    #

    # Remove temp directory
    if args.delete_tmpdir:
        rmtree(args.tmpdir)

    # Log time taken
    logger.info('Time taken: {0}'.format(datetime.now() - start_time))
    logger.info('Finished!')

    return 0

def run_coloc(left_ss, right_ss, tmp_dir, logger):
    ''' Runs R coloc script and read the results
    Args:
        left_path (str)
        right_path (str)
        tmp_dir (str)
    Returns:
        dict of results
    '''
    # Write sumstat files
    left_path = os.path.join(tmp_dir, 'left_ss.tsv.gz')
    left_ss.to_csv(left_path, sep='\t', index=None, compression='gzip')
    right_path = os.path.join(tmp_dir, 'right_ss.tsv.gz')
    right_ss.to_csv(right_path, sep='\t', index=None, compression='gzip')

    # Build command
    out_pref = os.path.join(tmp_dir, 'coloc')
    coloc_script = os.path.join(os.path.dirname(__file__), 'coloc.R')
    cmd = [
        'Rscript',
        coloc_script,
        left_path,
        right_path,
        out_pref
    ]

    # Run command
    # print(' '.join(cmd))
    fnull = open(os.devnull, 'w')
    cp = sp.run(' '.join(cmd), shell=True, stdout=fnull, stderr=sp.STDOUT)

    # Read results
    res_path = out_pref + '.pp.tsv'
    res_df = pd.read_csv(res_path, sep='\t', header=0)

    # Convert to a dict
    res_dict = dict(zip(res_df.field, res_df.value))
    res_dict['nsnps'] = int(res_dict['nsnps'])

    return res_dict

def read_relevant_args(path):
    args = read_args(path)
    relevant_keys = {'type', 'sumstat', 'study', 'phenotype', 'bio_feature',
                       'chrom', 'pos', 'ref', 'alt'}
    return {key: val for key, val in args.items() if key in relevant_keys}

def read_args(path):
    args_json = os.path.join(path, 'args.json')
    with open(args_json, 'r') as f:
        return json.load(f)

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)


def parse_args():
    ''' Load command line args.
    '''
    p = argparse.ArgumentParser()

    # Left input args
    p.add_argument('--left_sumstat',
                   help=("Input: path to a directory with left sumstat.tsv.gz file"),
                   metavar="<file>", type=str, required=True)
    
    # Right input args
    p.add_argument('--right_sumstat',
                   help=("Input: path to a directory with right sumstat.tsv.gz file"),
                   metavar="<file>", type=str, required=True)

    p.add_argument('--delete_tmpdir',
                   help=("Remove temp dir when complete"),
                   action='store_true')

    # Add output file
    p.add_argument('--out',
                   metavar="<file>",
                   help=("Output: Coloc results file"),
                   type=str,
                   required=True)
    p.add_argument('--plot',
                   metavar="<file>",
                   help=("Output: Plot of colocalisation"),
                   type=str,
                   required=False)
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
    args = parse_args()
    main(args)
