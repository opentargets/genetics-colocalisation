#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This script assumes that if an LD reference is provided, then conditional
# analysis should be done to get conditionally independent sumstats for the
# signal defined by the top SNP. If no LD is provided, then it simply
# extracts sumstats for the specified window.

import pandas as pd
import logging
import argparse
import sys
import os

import utils as coloc_utils
import gcta as coloc_gcta

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(process)d - %(filename)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def main(args):

    # Start logging
    logger.info('Started selecting relevant sumstats')

    if args.ld is None:
        # No conditional analysis - just extract sumstats
        logger.info('Loading sumstats for {0}kb window'.format(
            args.window_coloc))
        sumstat_wind = coloc_utils.load_sumstats(
            in_pq=args.sumstat,
            study_id=args.study,
            phenotype_id=args.phenotype,
            bio_feature=args.bio_feature,
            chrom=args.chrom,
            start=args.pos - args.window_coloc * 1000,
            end=args.pos + args.window_coloc * 1000,
            min_maf=args.min_maf,
            logger=logger)
        logger.info('Loaded {0} variants'.format(sumstat_wind.shape[0]))

    else: # do conditional analysis

        # Load sumstats
        logger.info('Loading sumstats for {0}kb conditional window'.format(
            args.window_cond))
        sumstat = coloc_utils.load_sumstats(
            in_pq=args.sumstat,
            study_id=args.study,
            phenotype_id=args.phenotype,
            bio_feature=args.bio_feature,
            chrom=args.chrom,
            start=args.pos - args.window_cond * 1000,
            end=args.pos + args.window_cond * 1000,
            min_maf=args.min_maf,
            logger=logger)
        logger.info('Loaded {0} variants'.format(sumstat.shape[0]))

        logger.info('Starting conditional analysis')

        # Load top loci json
        logger.info('Loading top loci table')
        top_loci = pd.read_json(
            args.top_loci.replace('CHROM', args.chrom),
            orient='records',
            lines=True
        )

        # Make variant ID
        varid = ':'.join([str(x) for x in [
            args.chrom, args.pos, args.ref, args.alt]])

        # Extract top loci variants
        query = make_pandas_top_loci_query(
            study_id=args.study,
            phenotype_id=args.phenotype,
            bio_feature=args.bio_feature,
            chrom=args.chrom)
        top_loci = top_loci.query(query)

        # Create list of variants to condition on
        cond_list = make_list_to_condition_on(
            varid, sumstat.variant_id, top_loci.variant_id)
        # Perform conditional on sumstats
        if len(cond_list) > 0:
            logger.info('Conditioning {} variants on {} variants'.format(
                sumstat.shape[0], len(cond_list)))
            sumstat_cond = coloc_gcta.perform_conditional_adjustment(
                sumstat,
                args.ld,
                args.tmpdir,
                varid,
                args.chrom,
                cond_list,
                logger=logger)
            logger.info('Finished conditioning, {} variants remain'.format(
                sumstat_cond.shape[0]))
            # Copy the conditional stats into the original positions
            sumstat_cond['beta'] = sumstat_cond['beta_cond']
            sumstat_cond['se'] = sumstat_cond['se_cond']
            sumstat_cond['pval'] = sumstat_cond['pval_cond']
            sumstat = sumstat_cond.drop(columns=[
                'beta_cond', 'se_cond', 'pval_cond'])
        else:
            logger.info('No variants to condition on')

        # Extract coloc window
        logger.info('Extracting coloc window ({}kb)'.format(args.window_coloc))

        sumstat_wind = coloc_utils.extract_window(
            sumstat,
            args.chrom,
            args.pos,
            args.window_coloc)
        logger.info('{} variants remain'.format(sumstat_wind.shape[0]))

    # Write sumstat file
    sumstat_wind.to_csv(output_file_path(args), sep='\t', index=None, compression='gzip')


def output_file_path(args):
    return args.out


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


def parse_args_or_fail():
    ''' Load command line args.
    '''
    p = argparse.ArgumentParser()

    p.add_argument('--sumstat',
                   help=("Summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--ld',
                   help=("LD plink reference"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--study',
                   help=("study_id"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--phenotype',
                   help=("phenotype_id"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--bio_feature',
                   help=("bio_feature"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--chrom',
                   help=("Chromomsome"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--pos',
                   help=("Position"),
                   metavar="<int>", type=int, required=True)
    p.add_argument('--ref',
                   help=("Ref allele"),
                   metavar="<str>", type=str, required=False)
    p.add_argument('--alt',
                   help=("Alt allele"),
                   metavar="<str>", type=str, required=False)

    # Method parameteres
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
    # Top loci input
    p.add_argument('--top_loci',
                   help=("Top loci table (required for conditional analysis)"),
                   metavar="<str>", type=str, required=True)

    # Add output file
    p.add_argument('--out',
                   help=("Directory to sumstat.tsv.gz file with relevant only data and args.json file"),
                   metavar="<str>", type=str, required=True)
    p.add_argument('--tmpdir',
                   metavar="<file>",
                   help=("Temp dir"),
                   type=str,
                   required=True)

    if len(sys.argv)==1:
        p.print_help(sys.stderr)
        sys.exit(1)

    args = p.parse_args()

    # Convert "None" strings to None type
    for arg in vars(args):
        if getattr(args, arg) == "None":
            setattr(args, arg, None)

    return args


if __name__ == '__main__':
    args = parse_args_or_fail()
    ofp = output_file_path(args)
    if os.path.exists(ofp):
        logger.error("The output file {} exists already. If you want to regenerate the file you have to remove it first.".format(ofp))
        sys.exit(1)
    logger.debug('Making sure {} folder exist.'.format(args.out))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    main(args)
