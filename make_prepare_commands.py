#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
# Reads the manifest file and makes commands
#

import gzip
import json
import os
import argparse

def main(args):

    in_manifest = '/coloc/configs/manifest.json.gz'
    out_manifest = '/coloc/configs/ext_manifest.json.gz'
    cache_folder = '/data/output/cache/'
    os.makedirs(cache_folder, exist_ok=True)

    # Pipeline args
    prepare_script = 'scripts/select_relevant_sumstat.py'
    top_loci_file = '/data/top_loci_by_chrom/CHROM.json'
    window_colc = 500 # in KB
    window_cond = 1000  # in KB
    min_maf = 0.01

    files_to_cache = set()
    out_json_lines = set()
    # Iterate over manifest
    with gzip.open(in_manifest, 'rt') as in_mani:
        for line in in_mani:

            # Parse
            rec = json.loads(line.rstrip())
            # pprint(rec)

            if args.type is None or args.type == rec['left_type']:
                # Prepare left sumstat
                rec['left_reduced_sumstats'] = os.path.join(cache_folder, *[ str(rec[key]) for key in ['left_type', 'left_study_id', 'left_phenotype_id', 'left_bio_feature', 'left_lead_chrom', 'left_lead_pos', 'left_lead_ref', 'left_lead_alt']])
                prepare_left_cmd = [
                    'python',
                    os.path.abspath(prepare_script),
                    '--sumstat', os.path.abspath(rec['left_sumstats']),
                    '--ld', os.path.abspath(rec['left_ld']),
                    '--study', rec['left_study_id'],
                    '--phenotype', rec['left_phenotype_id'],
                    '--bio_feature', rec['left_bio_feature'],
                    '--chrom', rec['left_lead_chrom'],
                    '--pos', rec['left_lead_pos'],
                    '--ref', rec['left_lead_ref'],
                    '--alt', rec['left_lead_alt'],
                    '--top_loci', os.path.abspath(top_loci_file),
                    '--window_coloc', window_colc,
                    '--window_cond', window_cond,
                    '--min_maf', min_maf,
            '--tmpdir', os.path.abspath(rec['tmpdir']),
                    '--out', rec['left_reduced_sumstats']
                ]
                if not rec['left_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['left_reduced_sumstats'])
                    print(' '.join([str(arg) for arg in prepare_left_cmd]))

            if args.type is None or args.type == rec['right_type']:
                # Prepare right sumstat
                rec['right_reduced_sumstats'] = os.path.join(cache_folder, *[ str(rec[key]) for key in ['right_type', 'right_study_id', 'right_phenotype_id', 'right_bio_feature', 'right_lead_chrom', 'right_lead_pos', 'right_lead_ref', 'right_lead_alt']])
                prepare_right_cmd = [
                    'python',
                    os.path.abspath(prepare_script),
                    '--sumstat', os.path.abspath(rec['right_sumstats']),
                    '--ld', os.path.abspath(rec['right_ld']),
                    '--study', rec['right_study_id'],
                    '--phenotype', rec['right_phenotype_id'],
                    '--bio_feature', rec['right_bio_feature'],
                    '--chrom', rec['right_lead_chrom'],
                    '--pos', rec['right_lead_pos'],
                    '--ref', rec['right_lead_ref'],
                    '--alt', rec['right_lead_alt'],
                    '--top_loci', os.path.abspath(top_loci_file),
                    '--window_coloc', window_colc,
                    '--window_cond', window_cond,
                    '--min_maf', min_maf,
            '--tmpdir', os.path.abspath(rec['tmpdir']),
                    '--out', rec['right_reduced_sumstats']
                ]
                if not rec['right_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['right_reduced_sumstats'])
                    print(' '.join([str(arg) for arg in prepare_right_cmd]))
            out_json_lines.add(json.dumps(rec) + "\n")

        with gzip.open(out_manifest, 'wt') as out_mani:
            out_mani.writelines(out_json_lines)
        os.replace(out_manifest, in_manifest)


def parse_args():
    ''' Load command line args
    '''
    p = argparse.ArgumentParser()

    # Add input files
    p.add_argument('--type',
                   help=("Type of sumstats to use"),
                   type=str,
                   required=False)

    args = p.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(args)
