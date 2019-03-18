#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import os
import sys
# import pandas as pd
from pprint import pprint
from glob import glob
import json
from collections import OrderedDict

def main():

    # Parse args
    in_overlap_table = glob('tmp/overlap_table.json/*.json')[0]
    out_manifest = 'configs/manifest.json'
    prop_threshold = 0.10
    ld_path = '/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/{chrom}.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101'

    # File paths patterns
    sumstats = '../genetics-finemapping/example_data/sumstats/{type}_2/{study_id}.parquet'
    out = "output/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/coloc_res.json.gz"
    log = "logs/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/log_file.txt"
    tmpdir = "tmp/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/"
    plot = "plots/{left_study}_{left_phenotype}_{left_bio_feature}_{left_variant}_{right_study}_{right_phenotype}_{right_bio_feature}_{right_variant}.png"

    manifest = []
    with open(in_overlap_table, 'r') as in_h:
        for in_record in in_h:
            in_record = json.loads(in_record)
            out_record = OrderedDict()

            # Skip if proportion_overlap < prop_threshold
            max_overlap_prop = max(in_record['left_overlap_prop'],
                                   in_record['right_overlap_prop'])
            if max_overlap_prop < prop_threshold:
                continue
            
            # Add information for left/right
            for side in ['left', 'right']:

                # Add file information
                study_type = 'gwas' if in_record['{}_type'.format(side)] == 'gwas' else 'molecular_trait'
                out_record['{}_sumstats'.format(side)] = sumstats.format(
                    type=study_type,
                    study_id=in_record['{}_study_id'.format(side)])
                out_record['{}_ld'.format(side)] = ld_path.format(
                    chrom=in_record['{}_lead_chrom'.format(side)])
                
                # Add study identifiers
                identifiers = ['study_id', 'type', 'phenotype_id', 'bio_feature', 'lead_chrom',
                               'lead_pos', 'lead_ref', 'lead_alt']
                for i in identifiers:
                    out_record['{}_{}'.format(side, i)] = in_record.get('{}_{}'.format(side, i), None)

            # Add method (always conditional for now)
            out_record['method'] = 'conditional'
            
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
                left_bio_feature=in_record.get('left_bio_feature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_bio_feature=in_record.get('right_bio_feature', None),
                right_variant=right_variant
            )
            out_record['log'] = log.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_bio_feature=in_record.get('left_bio_feature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_bio_feature=in_record.get('right_bio_feature', None),
                right_variant=right_variant
            )
            out_record['tmpdir'] = tmpdir.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_bio_feature=in_record.get('left_bio_feature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_bio_feature=in_record.get('right_bio_feature', None),
                right_variant=right_variant
            )
            out_record['plot'] = plot.format(
                left_study=in_record['left_study_id'],
                left_phenotype=in_record.get('left_phenotype_id', None),
                left_bio_feature=in_record.get('left_bio_feature', None),
                left_variant=left_variant,
                right_study=in_record['right_study_id'],
                right_phenotype=in_record.get('right_phenotype_id', None),
                right_bio_feature=in_record.get('right_bio_feature', None),
                right_variant=right_variant
            )

            # Make all paths absolute
            for colname in ['left_sumstats', 'left_ld', 'right_sumstats', 'right_ld',
                            'out', 'log', 'tmpdir', 'plot']:
                out_record[colname] = os.path.abspath(out_record[colname])

            manifest.append(out_record)

    # # Convert to a dataframe and save
    # df = pd.DataFrame(manifest)
    # df.to_csv(outf, sep='\t', header=None, index=None, na_rep='None')

    # # Print column numbers
    # for i, colname in enumerate(df.columns):
    #     print(i, colname)

    with open(out_manifest, 'w') as out_h:
        for record in manifest:
            out_h.write(json.dumps(record) + '\n')

    return 0

if __name__ == '__main__':

    main()
