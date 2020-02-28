#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import gzip
import json
import os
from collections import OrderedDict
from glob import glob

import pandas as pd
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

    # In path patterns (server)
    sumstats = os.path.join(config['sumstats'], '{type}/{study_id}.parquet')
    ld_path = os.path.join(config['ld_reference'], 'ukb_v3_chr{chrom}.downsampled10k')
    custom_studies = pd.read_parquet(config['custom_studies'], columns=['study_id']).study_id.unique()


    # Out path patterns
    data_out = '/data/output'
    # id column names to overlap table columns mapping
    id_cols_mapping = {'study': 'study_id', 'type': 'type', 'phenotype': 'phenotype_id', 'bio_feature': 'bio_feature',
                   'chrom': 'lead_chrom', 'pos': 'lead_pos', 'ref': 'lead_ref', 'alt': 'lead_alt'}
    def construct_left_right_hive_partition_dirs(rec):
        dirs = []
        for side in ['left', 'right']:
            for col, ocol in id_cols_mapping.items():
                dirs.append(side + '_' + col + '=' + str(rec.get(side + '_' + ocol, None)))
        return os.path.join(*dirs)

    manifest = []
    with gzip.open(in_overlap_table, 'r') as in_h:
        for in_record in in_h:
            in_record = json.loads(in_record.decode())

            if in_record['left_study_id'] not in custom_studies and \
                    in_record['right_study_id'] not in custom_studies:
                continue

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

                for i in id_cols_mapping.values():
                    out_record['{}_{}'.format(side, i)] = in_record.get('{}_{}'.format(side, i), None)

            # Add method (always conditional for now)
            out_record['method'] = 'conditional'

            left_right_hive_partition_dirs = construct_left_right_hive_partition_dirs(in_record)
            out_record['out'] = os.path.join(data_out, 'data', left_right_hive_partition_dirs, 'coloc_res.json.gz')
            out_record['log'] = os.path.join(data_out, 'logs', left_right_hive_partition_dirs, 'log_file.txt')
            out_record['tmpdir'] = os.path.join(data_out, 'tmp', left_right_hive_partition_dirs)
            out_record['plot'] = os.path.join(data_out, 'plot', left_right_hive_partition_dirs, 'plot.png')

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
