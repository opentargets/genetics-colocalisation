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
import gzip
import yaml


def main():
    # Load config
    with open('/coloc/configs/config.yaml') as config_input:
        config = yaml.load(config_input, Loader=yaml.FullLoader)

    # Parse args
    in_overlap_table = glob('/data/overlap_table/*.json.gz')[0]
    out_manifest = '/coloc/configs/manifest.json.gz'
    overlap_prop_threshold = 0.01
    max_credset_threshold = None

    # # In path patterns (local)
    # sumstats = '../genetics-finemapping/example_data/sumstats/{type}_2/{study_id}.parquet'
    # ld_path = '/Users/em21/Projects/reference_data/uk10k_2019Feb/3_liftover_to_GRCh38/output/{chrom}.ALSPAC_TWINSUK.maf01.beagle.csq.shapeit.20131101'
    
    # In path patterns (server)
    sumstats = os.path.join(config['sumstats'], '{type}/{study_id}.parquet')
    ld_path = os.path.join(config['ld_reference'], 'ukb_v3_chr{chrom}.downsampled10k')

    # Out path patterns
    out = "/data/output/data/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/coloc_res.json.gz"
    log = "/data/output/logs/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/log_file.txt"
    tmpdir = "/data/output/tmp/left_study={left_study}/left_phenotype={left_phenotype}/left_bio_feature={left_bio_feature}/left_variant={left_variant}/right_study={right_study}/right_phenotype={right_phenotype}/right_bio_feature={right_bio_feature}/right_variant={right_variant}/"
    plot = "/data/output/plot/{left_study}_{left_phenotype}_{left_bio_feature}_{left_variant}_{right_study}_{right_phenotype}_{right_bio_feature}_{right_variant}.png"

    manifest = []
    with gzip.open(in_overlap_table, 'r') as in_h:
        for in_record in in_h:
            in_record = json.loads(in_record.decode())
            out_record = OrderedDict()

            # Skip if proportion_overlap < prop_threshold
            if overlap_prop_threshold:
                max_overlap_prop = max(in_record['left_overlap_prop'],
                                    in_record['right_overlap_prop'])
                if max_overlap_prop < overlap_prop_threshold:
                    continue

            #  Skip if the biggest credible has > max_credset_threshold variants
            if max_credset_threshold:
                max_credset_size = max(in_record['left_num_tags'],
                                    in_record['right_num_tags'])
                if max_credset_size > max_credset_threshold:
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

            # Check that all input paths exist
            for colname in ['left_sumstats', 'left_ld', 'right_sumstats', 'right_ld']:
                # Get path
                in_path = out_record[colname]
                # If plink prefix, add .bed suffix
                if colname == 'left_ld' or colname == 'right_ld':
                    in_path = in_path + '.bed'
                # Assert exists
                assert os.path.exists(in_path), \
                    "Input file not found ({}): {}".format(colname, in_path)

            manifest.append(out_record)

    # Write manifest file
    os.makedirs(os.path.dirname(out_manifest), exist_ok=True)
    with gzip.open(out_manifest, 'w') as out_h:
        for record in manifest:
            out_h.write((json.dumps(record) + '\n').encode())

    return 0

if __name__ == '__main__':

    main()
