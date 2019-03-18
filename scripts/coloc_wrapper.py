#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import os
import gzip
import argparse
import logging
import pprint
import utils as coloc_utils
import gcta as coloc_gcta
import pandas as pd
import dask.dataframe as dd
import subprocess as sp
import json
from datetime import datetime
from numpy import log10, log2
# Plotting libraries
import matplotlib.pyplot as plt
import seaborn as sns

def main():

    start_time = datetime.now()

    # Args
    global args
    args = parse_args()

    # Silence pandas warning
    pd.options.mode.chained_assignment = None

    # Make output and temp directories
    os.makedirs(os.path.split(args.out)[0], exist_ok=True)
    os.makedirs(os.path.split(args.log)[0], exist_ok=True)
    os.makedirs(args.tmpdir , exist_ok=True)

    # Start logging
    logger = make_logger(args.log)
    logger.info('Started coloc pipeline')
    logger.info('Printing args: \n' + pprint.pformat(vars(args), indent=2))

    # --------------------------------------------------------------------------
    # Load summary statistics (using wider `window_cond` window for condtional
    # analysis)
    #

    # Load left sumstats
    logger.info('Loading left sumstats for {0}kb conditional window'.format(
        args.window_cond))
    sumstat_left = coloc_utils.load_sumstats(
        in_pq=args.left_sumstat,
        study_id=args.left_study,
        phenotype_id=args.left_phenotype,
        bio_feature=args.left_bio_feature,
        chrom=args.left_chrom,
        start=args.left_pos - args.window_cond * 1000,
        end=args.left_pos + args.window_cond * 1000,
        min_maf=args.min_maf,
        logger=logger)
    logger.info('Loaded {0} left variants'.format(sumstat_left.shape[0]))

    # Load right sumstats
    logger.info('Loading right sumstats for {0}kb conditional window'.format(
        args.window_cond))
    sumstat_right = coloc_utils.load_sumstats(
        in_pq=args.right_sumstat,
        study_id=args.right_study,
        phenotype_id=args.right_phenotype,
        bio_feature=args.right_bio_feature,
        chrom=args.right_chrom,
        start=args.right_pos - args.window_cond * 1000,
        end=args.right_pos + args.window_cond * 1000,
        min_maf=args.min_maf,
        logger=logger)
    logger.info('Loaded {0} right variants'.format(sumstat_right.shape[0]))

    # --------------------------------------------------------------------------
    # Perform conditional analysis
    #

    if args.method == 'conditional':

        logger.info('Starting conditional analysis')

        # Load top loci json
        logger.info(' Loading top loci table')
        top_loci = pd.read_json(
            args.top_loci,
            orient='records',
            lines=True
        )

        # Make left and right variant IDs
        varid_left = ':'.join([str(x) for x in [
            args.left_chrom, args.left_pos, args.left_ref, args.left_alt]])
        varid_right = ':'.join([str(x) for x in [
            args.right_chrom, args.right_pos, args.right_ref, args.right_alt]])

        # Extract left and right top loci variants
        query_left = make_pandas_top_loci_query(
            study_id=args.left_study,
            phenotype_id=args.left_phenotype,
            bio_feature=args.left_bio_feature,
            chrom=args.left_chrom)
        top_loci_left = top_loci.query(query_left)
        query_right = make_pandas_top_loci_query(
            study_id=args.right_study,
            phenotype_id=args.right_phenotype,
            bio_feature=args.right_bio_feature,
            chrom=args.right_chrom)
        top_loci_right = top_loci.query(query_right)

        # Create list of variants to condition on
        cond_list_left = make_list_to_condition_on(
            varid_left, sumstat_left.variant_id, top_loci_left.variant_id)
        cond_list_right = make_list_to_condition_on(
            varid_right, sumstat_right.variant_id, top_loci_right.variant_id)

        # Perform conditional on left
        if len(cond_list_left) > 0:
            logger.info(' Left, conditioning {} variants on {} variants'.format(
                sumstat_left.shape[0], len(cond_list_left)))
            sumstat_cond_left = coloc_gcta.perfrom_conditional_adjustment(
                sumstat_left,
                args.left_ld,
                args.tmpdir,
                varid_left,
                args.left_chrom,
                cond_list_left,
                logger=logger)
            logger.info(' Left, finished conditioning, {} variants remain'.format(
                sumstat_cond_left.shape[0]))
            # Copy the conditional stats into the original positions
            print(sumstat_cond_left.loc[sumstat_cond_left['pos'] == 39304989, :])
            sumstat_cond_left['beta'] = sumstat_cond_left['beta_cond']
            sumstat_cond_left['se'] = sumstat_cond_left['se_cond']
            sumstat_cond_left['pval'] = sumstat_cond_left['pval_cond']
            sumstat_left = sumstat_cond_left.drop(columns=[
                'beta_cond', 'se_cond', 'pval_cond'])
        else:
            logger.info(' Left, no variants to condition on')

        # Perform conditional on right
        if len(cond_list_right) > 0:
            logger.info(' Right, conditioning {} variants on {} variants'.format(
                sumstat_right.shape[0], len(cond_list_right)))
            sumstat_cond_right = coloc_gcta.perfrom_conditional_adjustment(
                sumstat_right,
                args.right_ld,
                args.tmpdir,
                varid_right,
                args.right_chrom,
                cond_list_right,
                logger=logger)
            logger.info(' Right, finished conditioning, {} variants remain'.format(
                sumstat_cond_right.shape[0]))
            # Copy the conditional stats into the original positions
            sumstat_cond_right['beta'] = sumstat_cond_right['beta_cond']
            sumstat_cond_right['se'] = sumstat_cond_right['se_cond']
            sumstat_cond_right['pval'] = sumstat_cond_right['pval_cond']
            sumstat_right = sumstat_cond_right.drop(columns=[
                'beta_cond', 'se_cond', 'pval_cond'])
        else:
            logger.info(' Right, no variants to condition on')

    # --------------------------------------------------------------------------
    # Extract coloc window and harmonise left and right
    #

    # Extract coloc window
    logger.info('Extracting coloc window ({}kb)'.format(args.window_coloc))

    sumstat_wind_left = coloc_utils.extract_window(
        sumstat_left,
        args.left_chrom,
        args.left_pos,
        args.window_coloc)
    logger.info(' Left, {} variants remain'.format(sumstat_wind_left.shape[0]))

    sumstat_wind_right = coloc_utils.extract_window(
        sumstat_right,
        args.left_chrom,
        args.left_pos,
        args.window_coloc)
    logger.info(' Right, {} variants remain'.format(sumstat_wind_right.shape[0]))

    # Harmonise left and right
    logger.info('Harmonising left and right dfs'.format(args.window_coloc))

    # Set indexes
    sumstat_wind_left.index = sumstat_wind_left.variant_id
    sumstat_wind_right.index = sumstat_wind_right.variant_id

    # Take intersection
    shared_vars = sorted(
        set(sumstat_wind_left.index).intersection(
        set(sumstat_wind_right.index))
    )
    sumstat_int_left = sumstat_wind_left.loc[shared_vars, :]
    sumstat_int_right = sumstat_wind_right.loc[shared_vars, :]
    logger.info('Left-right intersection contains {0} variants'.format(
        sumstat_int_left.shape[0]))

    # --------------------------------------------------------------------------
    # Perform coloc
    #

    res = None

    if sumstat_int_left.shape[0] > 0:

        logger.info('Running colocalisation')

        res = run_coloc(args.r_coloc_script, sumstat_int_left,
                        sumstat_int_right, args.tmpdir, logger)

        logger.info(' H4={:.3f} and H3={:.3f}'.format(
            res['PP.H4.abf'], res['PP.H3.abf']))

        # Add columns to result file
        for prefix in ['left_', 'right_']:
            for suffix in ['type', 'sumstat', 'study', 'phenotype', 'bio_feature',
                           'chrom', 'pos', 'ref', 'alt']:
                key = prefix + suffix
                res[key] = args.__dict__[key]

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

    # Log time taken
    logger.info('Time taken: {0}'.format(datetime.now() - start_time))
    logger.info('Finished!')

    return 0

def run_make_coloc_plot(left_ss, right_ss, h4, h3, outf):
    ''' Uses seaborn to make a scatter plot of left vs right pvals
    '''

    # Take -log10 pvals
    left_ss['log_pval'] = left_ss.pval.apply(log10) * -1
    right_ss['log_pval'] = right_ss.pval.apply(log10) * -1

    # Make longform
    left_ss['dataset'] = 'left'
    right_ss['dataset'] = 'right'
    plot_df = pd.concat([
        left_ss.loc[:, ['variant_id', 'pos', 'log_pval', 'dataset']],
        right_ss.loc[:, ['variant_id', 'pos', 'log_pval', 'dataset']]])

    # Create facet grid scatter plot
    plt.ioff()
    g = sns.FacetGrid(plot_df, row='dataset', sharey=False, aspect=2, hue='dataset')
    g.map(plt.scatter, 'pos', 'log_pval', s=1)
    g.fig.subplots_adjust(top=0.87)
    g.fig.suptitle('H4={:.3f}\nH3={:.3f}'.format(h4, h3))
    plt.savefig(outf, bbox_inches='tight')

    return 0

def run_coloc(script, left_ss, right_ss, tmp_dir, logger):
    ''' Runs R coloc script and read the results
    Args:
        left_ss (df)
        right_ss (df)
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
    cmd = [
        'Rscript',
        script,
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

def make_pandas_top_loci_query(study_id, phenotype_id=None,
                               bio_feature=None, chrom=None):
    ''' Creates query to extract top loci for specific study
    '''
    queries = ['study_id == "{}"'.format(study_id)]
    if phenotype_id:
        queries.append('phenotype_id == "{}"'.format(phenotype_id))
    if bio_feature:
        queries.append('bio_feature == "{}"'.format(bio_feature))
    if chrom:
        queries.append('chrom == "{}"'.format(chrom))
    return ' & '.join(queries)

def make_list_to_condition_on(index_var, all_vars, top_vars):
    ''' Makes a list of variants on which to condition on
    Args:
        index_var (str): index variant at this locus
        all_vars (pd.Series): A series of all variant IDs in the locus window
        top_vars (pd.Series): A series of top loci variant IDs
    Returns:
        list of variants to condition on
    '''
    window_top_vars = set(all_vars).intersection(set(top_vars))
    cond_list = list(window_top_vars - set([index_var]))
    return cond_list

def touch(fname, times=None):
    with open(fname, 'a'):
        os.utime(fname, times)

def parse_args():
    ''' Load command line args.
    '''
    p = argparse.ArgumentParser()

    # Left input args
    p.add_argument('--left_sumstat',
                   help=("Input: left summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--left_type',
                   help=("Left type"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--left_study',
                   help=("Left study_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--left_phenotype',
                   help=("Left phenotype_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--left_bio_feature',
                   help=("Left bio_feature"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--left_chrom',
                   help=("Left chromomsome"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--left_pos',
                   help=("Left position"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--left_ref',
                   help=("Left ref allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--left_alt',
                   help=("Left alt allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--left_ld',
                   help=("Left LD plink reference"),
                   metavar="<str>", type=str, required=False)
    
    # Right input args
    p.add_argument('--right_sumstat',
                   help=("Input: Right summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--right_type',
                   help=("Right type"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--right_study',
                   help=("Right study_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--right_phenotype',
                   help=("Right phenotype_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--right_bio_feature',
                   help=("Right bio_feature"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--right_chrom',
                   help=("Right chromomsome"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--right_pos',
                   help=("Right position"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--right_ref',
                   help=("Right ref allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--right_alt',
                   help=("Right alt allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--right_ld',
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

    # Other options
    p.add_argument('--min_maf',
                   help=("Minimum minor allele frequency to be included"),
                   metavar="<float>", type=float, required=False)
    p.add_argument('--r_coloc_script',
                   help=("R script that implements coloc"),
                   type=str, required=True, metavar="<str>")
    # Top loci input
    p.add_argument('--top_loci',
                   help=("Input: Top loci table (required for conditional analysis)"),
                   metavar="<str>", type=str, required=False)

    # Add output file
    p.add_argument('--out',
                   metavar="<file>",
                   help=("Output: Coloc results"),
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

def make_logger(log_file):
    ''' Creates a logging handle.
    '''
    # Basic setup
    logging.basicConfig(
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=None)

    # Create formatter
    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    rootLogger = logging.getLogger(__name__)

    # Add file logging
    fileHandler = logging.FileHandler(log_file, mode='w')
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    # Add stdout logging
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    # Prevent logging from propagating to the root logger
    rootLogger.propagate = 0

    return rootLogger

if __name__ == '__main__':

    main()
