#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#
# Filters manifest to remove coloc tests already present in the main coloc table.

import gzip
import os
from collections import OrderedDict
import pandas as pd
import yaml


def main():
    # Load config
    with open('/configs/config.yaml') as config_input:
        config = yaml.load(config_input, Loader=yaml.FullLoader)

    in_manifest = '/configs/manifest_unfiltered.json.gz'
    out_manifest = '/configs/manifest.json.gz'

    os.makedirs(os.path.dirname(out_manifest), exist_ok=True)

    shared_cols_dict = OrderedDict([
        ('left_study', 'left_study_id'),
        ('left_phenotype', 'left_phenotype_id'),
        ('left_bio_feature', 'left_bio_feature'),
        ('left_chrom', 'left_lead_chrom'),
        ('left_pos', 'left_lead_pos'),
        ('left_ref', 'left_lead_ref'),
        ('left_alt', 'left_lead_alt'),
        ('right_study', 'right_study_id'),
        ('right_phenotype', 'right_phenotype_id'),
        ('right_bio_feature', 'right_bio_feature'),
        ('right_chrom', 'right_lead_chrom'),
        ('right_pos', 'right_lead_pos'),
        ('right_ref', 'right_lead_ref'),
        ('right_alt', 'right_lead_alt'),
    ])

    # Read unfiltered manifest file
    # Make sure we read these as string, not integers, since they may have "None" rather than NaN values
    manifest_dtypes = {
        'left_lead_chrom': str,
        'right_lead_chrom': str,
        'right_phenotype_id': str,
        'right_bio_feature': str,
    }

    if 'coloc_table' in config:
        print('Filtering out previous colocs from manifest')
        # Read table of completed coloc tests
        coloc_table = pd.read_parquet(config['coloc_table'], columns=list(shared_cols_dict.keys()))
        coloc_table.rename(columns=shared_cols_dict, inplace=True)
        
        # Ensure that both tables represent "None" in the same way
        coloc_table.loc[coloc_table['left_phenotype_id'] == "None", 'left_phenotype_id'] = None
        coloc_table.loc[coloc_table['left_bio_feature'] == "None", 'left_bio_feature'] = None
        coloc_table.loc[coloc_table['right_phenotype_id'] == "None", 'right_phenotype_id'] = None
        coloc_table.loc[coloc_table['right_bio_feature'] == "None", 'right_bio_feature'] = None
        coloc_table = coloc_table.drop_duplicates()
        print('{0} previous colocs read in'.format(len(coloc_table)))

    # Go through the manifest in chunks, so that we don't run out of memory.
    # This is why out_manifest is opened in append mode.
    with gzip.open(out_manifest, 'a') as out_manifest_h:
        with pd.read_json(in_manifest, orient='records', lines=True, chunksize=1e5, dtype=manifest_dtypes) as reader:
            for manifest_chunk in reader:
                if not 'coloc_table' in config:
                    manifest_filtered = manifest_chunk
                else:
                    print("Filtering out coloc tests already present in the main coloc table {0}".format(config['coloc_table']))

                    manifest_chunk.loc[manifest_chunk['left_phenotype_id'] == "None", 'left_phenotype_id'] = None
                    manifest_chunk.loc[manifest_chunk['left_bio_feature'] == "None", 'left_bio_feature'] = None
                    manifest_chunk.loc[manifest_chunk['right_phenotype_id'] == "None", 'right_phenotype_id'] = None
                    manifest_chunk.loc[manifest_chunk['right_bio_feature'] == "None", 'right_bio_feature'] = None
                    
                    # Remove manifest lines that are in the coloc table
                    # To do this we do a left join with the manifest, and then keep rows that are
                    # present only in the "left" side, i.e. manifest. (Removing those in "both".)
                    manifest_joined = manifest_chunk.merge(coloc_table, on=list(shared_cols_dict.values()), how='left', indicator=True)

                    # At some point I got errors that the merge couldn't be done because some col types were "object" or "float64"
                    # Manually specifying the column types in the manifest above fixed this.

                    # Keep columns that were only present in the manifest and not the coloc table
                    manifest_filtered = manifest_joined[manifest_joined._merge == "left_only"].drop(columns="_merge")

                # Write manifest file
                manifest_filtered.to_json(out_manifest_h, orient='records', lines=True, force_ascii=True)

    return 0

if __name__ == '__main__':

    main()
