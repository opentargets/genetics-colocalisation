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
import json
from collections import OrderedDict

def main():

    # Parse args
    in_overlap_table = glob('tmp/overlap_table.json/*.json')[0]
    outf = 'configs/manifest_file.tsv'
    prop_threshold = 0.01
    ld_path = '/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/{chrom}.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101'

    # File paths patterns
    sumstats = '../genetics-finemapping/example_data/sumstats/{type}/{study_id}.parquet'
    out = "output/left_study={left_study}/left_phenotype={left_phenotype}/left_biofeature={left_biofeature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_biofeature={right_biofeature}/right_variant={right_variant}/coloc_res.json"
    log = "logs/left_study={left_study}/left_phenotype={left_phenotype}/left_biofeature={left_biofeature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_biofeature={right_biofeature}/right_variant={right_variant}/log_file.txt"
    tmpdir = "tmp/left_study={left_study}/left_phenotype={left_phenotype}/left_biofeature={left_biofeature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_biofeature={right_biofeature}/right_variant={right_variant}/"
    plot = "plots/{left_study}_{left_phenotype}_{left_biofeature}_{left_variant}_{right_study}_{right_phenotype}_{right_biofeature}_{right_variant}.png"

    manifest = []
    with open(in_overlap_table, 'r') as in_h:
        for in_record in in_h:
            in_record = json.loads(in_record)
            out_record = OrderedDict()

            # Skip if proportion_overlap < prop_threshold
            if in_record['proportion_overlap'] < prop_threshold:
                continue
            
            # Add information for left/right
            for side in ['left', 'right']:

                # Add file information
                out_record['{}_sumstats'.format(side)] = sumstats.format(
                    type=in_record['{}_type'.format(side)],
                    study_id=in_record['{}_study_id'.format(side)])
                out_record['{}_ld'.format(side)] = ld_path.format(
                    chrom=in_record['{}_lead_chrom'.format(side)])
                
                # Add study identifiers
                identifiers = ['study_id', 'phenotype_id', 'biofeature', 'lead_chrom',
                               'lead_pos', 'lead_ref', 'lead_alt']
                for i in identifiers:
                    out_record['{}_{}'.format(side, i)] = in_record.get('{}_{}'.format(side, i), None)
                
            # Add output files
            left_variant = '_'.join(
                [str(in_record['left_lead_{}'.format(part)])
                    for part in ['chrom', 'pos', 'ref', 'alt']]
            )
            right_variant = '_'.join(
                [str(in_record['right_lead_{}'.format(part)])
                    for part in ['chrom', 'pos', 'ref', 'alt']]
            )
            out_record['out'] = out.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_biofeature=in_record.get('left_biofeature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_biofeature=in_record.get('right_biofeature', None),
                right_variant=right_variant
            )
            out_record['log'] = log.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_biofeature=in_record.get('left_biofeature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_biofeature=in_record.get('right_biofeature', None),
                right_variant=right_variant
            )
            out_record['tmpdir'] = tmpdir.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_biofeature=in_record.get('left_biofeature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_biofeature=in_record.get('right_biofeature', None),
                right_variant=right_variant
            )
            out_record['plot'] = plot.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_biofeature=in_record.get('left_biofeature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_biofeature=in_record.get('right_biofeature', None),
                right_variant=right_variant
            )

            # Make all paths absolute
            for colname in ['left_sumstats', 'left_ld', 'right_sumstats', 'right_ld',
                            'out', 'log', 'tmpdir', 'plot']:
                out_record[colname] = os.path.abspath(out_record[colname])

            manifest.append(out_record)

    # Convert to a dataframe and save
    df = pd.DataFrame(manifest)
    df.to_csv(outf, sep='\t', header=None, index=None, na_rep='None')

    # Print column numbers
    for i, colname in enumerate(df.columns):
        print(i, colname)

    return 0

if __name__ == '__main__':

    main()
