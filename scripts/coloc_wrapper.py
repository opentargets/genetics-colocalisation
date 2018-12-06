#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys
import os
import argparse
import logging
import pprint
import utils as coloc_utils
import gcta as coloc_gcta
import pandas as pd
import dask.dataframe as dd
import subprocess as sp
import json

def main():

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
        in_pq=args.sumstat_left,
        study_id=args.study_left,
        cell_id=args.cell_left,
        group_id=args.group_left,
        trait_id=args.trait_left,
        chrom=args.chrom_left,
        start=args.pos_left - args.window_cond * 1000,
        end=args.pos_left + args.window_cond * 1000,
        min_maf=args.min_maf,
        build=args.build,
        logger=logger)
    logger.info('Loaded {0} left variants'.format(sumstat_left.shape[0]))

    # Load right sumstats
    logger.info('Loading right sumstats for {0}kb conditional window'.format(
        args.window_cond))
    sumstat_right = coloc_utils.load_sumstats(
        in_pq=args.sumstat_right,
        study_id=args.study_right,
        cell_id=args.cell_right,
        group_id=args.group_right,
        trait_id=args.trait_right,
        chrom=args.chrom_right,
        start=args.pos_right - args.window_cond * 1000,
        end=args.pos_right + args.window_cond * 1000,
        min_maf=args.min_maf,
        build=args.build,
        logger=logger)
    logger.info('Loaded {0} right variants'.format(sumstat_right.shape[0]))

    # --------------------------------------------------------------------------
    # Perform conditional analysis
    #

    if args.method == 'conditional':

        logger.info('Starting conditional analysis')

        # Load top loci
        logger.info(' Loading top loci table')
        top_loci = dd.read_parquet(args.top_loci,
                                   engine='fastparquet').compute()

        # Make left and right variant IDs
        varid_left = '_'.join([str(x) for x in [
            args.chrom_left, args.pos_left, args.ref_left, args.alt_left]])
        varid_right = '_'.join([str(x) for x in [
            args.chrom_right, args.pos_right, args.ref_right, args.alt_right]])

        # Extract left and right top loci variants
        query_left = make_pandas_top_loci_query(
            study_id=args.study_left,
            cell_id=args.cell_left,
            group_id=args.group_left,
            trait_id=args.trait_left,
            chrom=args.chrom_left)
        top_loci_left = top_loci.query(query_left)
        query_right = make_pandas_top_loci_query(
            study_id=args.study_right,
            cell_id=args.cell_right,
            group_id=args.group_right,
            trait_id=args.trait_right,
            chrom=args.chrom_right)
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
                args.ld_left,
                args.tmpdir,
                varid_left,
                args.chrom_left,
                cond_list_left,
                logger=logger)
            logger.info(' Left, finished conditioning, {} variants remain'.format(
                sumstat_cond_left.shape[0]))
            # Copy the conditional stats into the original positions
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
                args.ld_right,
                args.tmpdir,
                varid_right,
                args.chrom_right,
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
        args.chrom_left,
        args.pos_left,
        args.window_coloc)
    logger.info(' Left, {} variants remain'.format(sumstat_wind_left.shape[0]))

    sumstat_wind_right = coloc_utils.extract_window(
        sumstat_right,
        args.chrom_left,
        args.pos_left,
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
    logger.info('Left-right intersection contains {0} variants'.format(sumstat_int_left.shape[0]))

    # --------------------------------------------------------------------------
    # Perform coloc
    #

    res = None

    if sumstat_int_left.shape[0] > 0:

        logger.info('Running colocalisation')

        res = run_coloc(sumstat_int_left, sumstat_int_right, args.tmpdir)

        logger.info(' H4={:.3f} and H3={:.3f}'.format(
            res['PP.H4.abf'], res['PP.H3.abf']))

    else:

        logger.error('Cannot run coloc with no intersection')

    # --------------------------------------------------------------------------
    # Output results
    #

    # Write as json to output folder
    if res:
        with open(args.out, 'w') as out_h:
            json.dump(res, out_h)
    # If no results, then touch empty file
    else:
        touch(args.out)

    return 0

def run_coloc(left_ss, right_ss, tmp_dir):
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
        'scripts/coloc.R',
        left_path,
        right_path,
        out_pref
    ]

    # Run command
    fnull = open(os.devnull, 'w')
    cp = sp.run(' '.join(cmd), shell=True, stdout=fnull, stderr=sp.STDOUT)
    # cp = sp.run(' '.join(cmd), shell=True)

    # Read results
    res_path = out_pref + '.pp.tsv'
    res_df = pd.read_csv(res_path, sep='\t', header=0)

    # Convert to a dict
    res_dict = dict(zip(res_df.field, res_df.value))
    res_dict['nsnps'] = int(res_dict['nsnps'])

    return res_dict

def make_pandas_top_loci_query(study_id, cell_id=None, group_id=None,
                               trait_id=None, chrom=None):
    ''' Creates query to extract top loci for specific study
    '''
    queries = ['study_id == "{}"'.format(study_id)]
    if cell_id:
        queries.append('cell_id == "{}"'.format(cell_id))
    if group_id:
        queries.append('group_id == "{}"'.format(group_id))
    if trait_id:
        queries.append('trait_id == "{}"'.format(trait_id))
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

    # Other options
    p.add_argument('--min_maf',
                   help=("Minimum minor allele frequency to be included"),
                   metavar="<float>", type=float, required=False)
    p.add_argument('--build',
                   help=("Which genome build to use (default: b37)"),
                   choices=['b37', 'b38'],
                   default='b37', type=str, required=False)

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
        level=logging.DEBUG,
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

"""

    # Make temp and output dirs
    os.makedirs(args.outpref, exist_ok=True)

    # Start log
    log_file = '{0}/log.txt'.format(args.outpref)
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s: %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S',
                        filemode='w',
                        level=logging.INFO)
    logging.info('Started')
    logging.info(sys.argv)
    logging.info(' '.join(sys.argv))

    # Check inputs exist
    infs = [args.left_sumstats, args.right_sumstats]
    if args.vloc:
        infs.append(args.bgen)
    for inf in infs:
        if not os.path.exists(inf):
            logging.info('Input file not found: {0}'.format(inf))
            sys.exit()

    #
    # Prepare sumstat files ----------------------------------------------------
    #

    # Load sumstats
    left_ss = parse_sumstats(args.left_sumstats, args.pos, args.window_kb)
    logging.info('left sumstats file contains {0} variants in region'.format(left_ss.shape[0]))
    right_ss = parse_sumstats(args.right_sumstats, args.pos, args.window_kb)
    logging.info('right sumstats file contains {0} variants in region'.format(right_ss.shape[0]))

    # Make a merge key
    left_ss['merge_key'] = make_merge_key(left_ss, ['chrom', 'pos_b37', 'ref_al', 'alt_al'])
    right_ss['merge_key'] = make_merge_key(right_ss, ['chrom', 'pos_b37', 'ref_al', 'alt_al'])
    left_ss.index = left_ss.merge_key
    right_ss.index = right_ss.merge_key

    # Take intersection
    shared_vars = sorted(set(left_ss.index).intersection(set(right_ss.index)))
    left_ss = left_ss.loc[shared_vars, :]
    right_ss = right_ss.loc[shared_vars, :]
    logging.info('left-right intersection contains {0} variants in region'.format(right_ss.shape[0]))

    #
    # Prepare covariance matrix ------------------------------------------------
    #

    # Covariance matrix only required if using Zuber et al's method
    if args.vloc:

        logging.info('creating ldstore cov matrix')
        cov_mat, eff_dict = create_covariance_matrix(args.bgen, args.pos, args.window_kb)
        logging.info('cov matrix file contains {0} variants in region'.format(cov_mat.shape))

        # Take intersection between cov_mat and ss files
        shared_vars = sorted(set(cov_mat.index).intersection(set(left_ss.merge_key)))
        left_ss = left_ss.loc[shared_vars, :]
        right_ss = right_ss.loc[shared_vars, :]
        cov_mat = cov_mat.loc[shared_vars, shared_vars]
        logging.info('left-right-covmatrix intersection contains {0} variants in region'.format(cov_mat.shape[0]))

        # Harmonise the alleles between sumstats and covmatrix
        cov_eff_al = pd.Series([eff_dict[key] for key in shared_vars])
        harmonise_vec = (cov_eff_al.reset_index(drop=True) == left_ss.alt_al.reset_index(drop=True)).replace({True: 1, False:-1})
        harmonise_vec.index = shared_vars
        cov_mat = cov_mat.multiply(harmonise_vec, axis=0)
        cov_mat = cov_mat.multiply(harmonise_vec, axis=1)

    # Write to temp dir
    logging.info('writing files')
    left_ss.to_csv(args.outpref + '/left_ss.tsv.gz', sep='\t', index=None, compression='gzip')
    right_ss.to_csv(args.outpref + '/right_ss.tsv.gz', sep='\t', index=None, compression='gzip')
    if args.vloc:
        cov_mat.to_csv(args.outpref + '/ldstore_cov.mat.hm.tsv', sep='\t')

    #
    # Run coloc ----------------------------------------------------------------
    #

    logging.info('running coloc')
    outpref = args.outpref + '/coloc'
    run_coloc(args.outpref + '/left_ss.tsv.gz',
              args.outpref + '/right_ss.tsv.gz',
              outpref)

    # Check output exists
    outf = outpref + '.pp.tsv'
    if not os.path.exists(outf):
        logging.info('Output file not found: {0}'.format(outf))
        sys.exit()

    # Touch COMPLETE
    touch(args.outpref + '/COMPLETE')

    logging.info('Finished!')

    return 0

def run_coloc(left_ss_path, right_ss_path, outpref):
    ''' Runs coloc.abf
    '''

    # Make command
    cmd = [
        'Rscript',
        'scripts/coloc.R',
        left_ss_path,
        right_ss_path,
        outpref
    ]
    # Run
    cmd = ' '.join([str(x) for x in cmd])
    sp.run(cmd, shell=True)


def create_covariance_matrix(bgen, pos, window):
    ''' Uses LDstore to make a covariance matrix for variants in defined region
    Args:
        bgen (file): input bgen file
        pos (int): centre position to extract from
        window (int): window around pos to extract (kb)
    returns:
        (pd.Df, dict): (covariance matrix, dict of effect alleles)
    '''
    range = '{}-{}'.format(pos - window * 1000, pos + window * 1000)

    # Create ldstore command
    outpref = args.outpref + '/ldstore_cov'
    cmd = [
        # Make bcor and merge
        'ldstore',
        '--bgen', bgen,
        '--incl-range', range,
        '--bcor', '{0}.bcor'.format(outpref),
        '--n-threads', 1,
        ';',
        'ldstore',
        '--bcor', '{0}.bcor'.format(outpref),
        '--merge', 1,
        ';',
        'rm', '-f', '{0}.bcor_*'.format(outpref),
        ';',
        # Extract LD matrix
        'ldstore',
        '--bcor', '{0}.bcor'.format(outpref),
        '--matrix', '{0}.mat.tsv'.format(outpref),
        ';',
        'ldstore',
        '--bcor', '{0}.bcor'.format(outpref),
        '--meta', '{0}.meta.tsv'.format(outpref)
    ]

    # Run LDstore
    logging.info('running ldstore')
    cmd = ' '.join([str(x) for x in cmd])
    # sp.run(cmd, shell=True) # Debug

    # Load cov matrix
    logging.info('loading covariance matrix')
    cov_mat, eff_dict = parse_cov_matrix(
        '{0}.mat.tsv'.format(outpref),
        '{0}.meta.tsv'.format(outpref)
    )

    return cov_mat, eff_dict

def make_merge_key(df, cols):
    ''' Makes a key to merge variant IDs on when alleles are not harmonised
    Args:
        df (pd.Df)
        cols (list): list of columns to use [chrom, pos, allele1, allele2]
    returns:
        str
    '''
    merge_key = ['_'.join( [str(l[0]), str(l[1])] + sorted([str(l[2]), str(l[3])]) )
                 for l in df.loc[:, cols].values.tolist()]
    return merge_key

def parse_cov_matrix(in_cov, in_meta):
    ''' Parses ldstore cov matrix and associated meta data file
    Args:
        in_cov (str): file containing matrix of correlations
        in_meta (str): file containing variant meta-data
    Returns:
        cov matrix (dataframe), effect alleles (dict)
    '''
    cov = pd.read_csv(in_cov, sep=" ", header=None)
    meta = pd.read_csv(in_meta, sep=" ", header=0)

    # Make a merge key
    merge_key = make_merge_key(meta, ['chromosome', 'position', 'A_allele', 'B_allele'])
    cov.index = merge_key
    meta.index = merge_key
    cov.columns = merge_key

    # Remove duplicates
    isdupe = meta.index.duplicated()
    cov = cov.loc[~isdupe, ~isdupe]
    meta = meta[~isdupe]

    return cov, meta["A_allele"].to_dict()

def parse_sumstats(inf, pos, window):
    ''' Parses a summary statistic file
    '''
    df = pd.read_csv(inf, sep='\t', header=0)
    # Extract required window
    df = df.loc[ ( (df.pos_b37 >= pos - window * 1000) &
      (df.pos_b37 <= pos + window * 1000) ), :]
    return df

"""
