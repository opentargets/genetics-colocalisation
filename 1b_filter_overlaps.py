#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import pandas as pd
import argparse
import glob


def main():

    args = parse_args()
    print(args)

    top_loci = pd.read_json(args.in_top_loci, orient='records', lines=True)
    print(top_loci.head(5))

    # There should be only one overlap table file part, but just in case
    # we will loop through multiple potential files
    overlap_table = pd.read_json(args.in_overlaps, orient='records', lines=True) # read data frame from json file

    # Filter top_loci based on pvalue threshold
    top_loci = top_loci[top_loci['pval'] < args.below_p]
    print(top_loci.head(5))

    # Set up left variant key and right variant key for overlap table
    overlap_table['left_variant_id'] = (
        str(overlap_table['left_lead_chrom']) + ':' +
        str(overlap_table['left_lead_pos']) + ':' +
        overlap_table['left_lead_ref'] + ':' +
        overlap_table['left_lead_alt']
    )
    overlap_table['right_variant_id'] = (
        str(overlap_table['right_lead_chrom']) + ':' +
        str(overlap_table['right_lead_pos']) + ':' +
        overlap_table['right_lead_ref'] + ':' +
        overlap_table['right_lead_alt']
    )
    print(overlap_table.head(5))

    #
    # Join overlap table with filtered top_loci table
    #

    # First join on left variant key, which will all be GWAS (so we don't need phenotype_id and bio_feature)
    top_loci_join = top_loci[['study_id', 'variant_id', 'pval']].rename(columns={'pval': 'left_pval'})
    overlap_table = pd.merge(overlap_table, top_loci_join, how='left', left_on=['left_study_id', 'left_variant_id'], right_on=['stud_yid', 'variant_id'])
    print(overlap_table.head(5))

    # Then join on right variant key, which may be GWAS or QTL
    top_loci_join = top_loci[['study_id', 'variant_id', 'phenotype_id', 'bio_feature', 'pval']].rename(columns={'pval': 'right_pval'})
    overlap_table = pd.merge(overlap_table, top_loci_join, how='left', left_on=['right_study_id', 'right_variant_id', 'right_phenotype_id', 'right_bio_feature'], right_on=['study_id', 'variant_id', 'phenotype_id', 'bio_feature'])
    print(overlap_table.head(5))

    # Filter table to rows where left or right pval is below threshold
    overlap_table = overlap_table[(overlap_table['left_pval'] < args.below_p) | (overlap_table['right_pval'] < args.below_p)]

    # Write out filtered overlap table in json format
    overlap_table.to_json(args.out_overlaps, orient='records', lines=True)

    return 0

def parse_args():
    """ Load command line args """
    parser = argparse.ArgumentParser()
    parser.add_argument('--in_overlaps',
                        metavar="<json>",
                        type=str,
                        required=True)
    parser.add_argument('--in_top_loci',
                        metavar="<json>",
                        type=str,
                        required=True)
    parser.add_argument('--below_p',
                        type=float,
                        default='1e-300',
                        help='Filter to include only overlaps where one study has lead p less than this')
    parser.add_argument('--out_overlaps',
                        metavar="<json>",
                        type=str,
                        required=True)
    args = parser.parse_args()
    return args

if __name__ == '__main__':

    main()
