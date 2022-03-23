#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Reads the manifest file and makes commands
#

import gzip
import json
import os
import argparse

def main(args):

    in_manifest = '/configs/manifest.json.gz'
    #out_manifest = '/configs/coloc_manifest.json.gz'
    out_manifest_tsv = '/configs/coloc_manifest.tsv.gz'
    out_todo = '/configs/commands_todo.cond.txt.gz'
    out_done = '/configs/commands_done.cond.txt.gz'
    cache_folder = '/output/cache'
    logs_folder = '/output/logs/extract_sumstats'
    os.makedirs(cache_folder, exist_ok=True)


    # Pipeline args
    prepare_script = os.path.abspath('scripts/select_relevant_sumstat.py')
    top_loci_file = '/data/finemapping/top_loci_by_chrom/CHROM.json'
    window_colc = 500 # in KB
    window_cond = 1000  # in KB
    min_maf = 0.01

    # Open command files
    todo_h = gzip.open(out_todo, 'wt')
    done_h = gzip.open(out_done, 'wt')
    #out_mani_h = gzip.open(out_manifest, 'wt')

    # The coloc output expects slightly different column names than what
    # we have in the manifest.
    manifest_header_cols = ['left_type', 'left_study', 'left_bio_feature', 'left_phenotype', 'left_chrom', 'left_pos', 'left_ref', 'left_alt', 'left_ld', 'left_sumstats', 'left_reduced_sumstats',
                     'right_type', 'right_study', 'right_bio_feature', 'right_phenotype', 'right_chrom', 'right_pos', 'right_ref', 'right_alt', 'right_ld', 'right_sumstats', 'right_reduced_sumstats']
    manifest_cols = ['left_type', 'left_study_id', 'left_bio_feature', 'left_phenotype_id', 'left_lead_chrom', 'left_lead_pos', 'left_lead_ref', 'left_lead_alt', 'left_ld', 'left_sumstats', 'left_reduced_sumstats',
                     'right_type', 'right_study_id', 'right_bio_feature', 'right_phenotype_id', 'right_lead_chrom', 'right_lead_pos', 'right_lead_ref', 'right_lead_alt', 'right_ld', 'right_sumstats', 'right_reduced_sumstats']
    
    out_mani_tsv_h = gzip.open(out_manifest_tsv, 'wt')
    out_mani_tsv_h.write('\t'.join(manifest_header_cols) + '\n')

    files_to_cache = set()
    # Iterate over manifest
    with gzip.open(in_manifest, 'rt') as in_mani:
        for line in in_mani:

            # Parse
            rec = json.loads(line.rstrip())
            # pprint(rec)

            if args.type is None or args.type == rec['left_type']:
                # Prepare left sumstat
                rec['left_reduced_sumstats'] = '/'.join([cache_folder] + [ str(rec[key]) for key in ['left_type', 'left_study_id', 'left_bio_feature', 'left_phenotype_id', 'left_lead_chrom', 'left_lead_pos', 'left_lead_ref', 'left_lead_alt']] + ['sumstat.tsv.gz'])
                log = '/'.join([logs_folder] + [ str(rec[key]) for key in ['left_type', 'left_study_id', 'left_bio_feature', 'left_phenotype_id', 'left_lead_chrom', 'left_lead_pos', 'left_lead_ref', 'left_lead_alt']] + ['log_file.txt'])

                # Do not create paths here, since this is single-threaded. Instead
                # do it in the script itself.
                #os.makedirs(os.path.dirname(log), exist_ok=True)
                ld = None
                if rec['left_ld'] is not None:
                    ld = rec['left_ld']
                prepare_left_cmd = [
                    'python',
                    prepare_script,
                    '--sumstat', rec['left_sumstats'],
                    '--ld', ld,
                    '--split_ld',
                    '--study', rec['left_study_id'],
                    '--phenotype', rec['left_phenotype_id'],
                    '--bio_feature', rec['left_bio_feature'],
                    '--chrom', rec['left_lead_chrom'],
                    '--pos', rec['left_lead_pos'],
                    '--ref', rec['left_lead_ref'],
                    '--alt', rec['left_lead_alt'],
                    '--top_loci', top_loci_file,
                    '--window_coloc', window_colc,
                    '--window_cond', window_cond,
                    '--min_maf', min_maf,
                    '--tmpdir', rec['tmpdir'],
                    '--out', rec['left_reduced_sumstats'],
                    '--log', log
                ]
                if not rec['left_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['left_reduced_sumstats'])
                    cmd_str = ' '.join([str(arg) for arg in prepare_left_cmd])
                    # Skip if output exists
                    if os.path.exists(rec['left_reduced_sumstats']):
                        done_h.write(cmd_str + '\n')
                    else:
                        todo_h.write(cmd_str + '\n')
                        if not args.quiet:
                            print(cmd_str)

            if args.type is None or args.type == rec['right_type']:
                # Prepare right sumstat
                rec['right_reduced_sumstats'] = '/'.join([cache_folder] + [ str(rec[key]) for key in ['right_type', 'right_study_id', 'right_bio_feature', 'right_phenotype_id', 'right_lead_chrom', 'right_lead_pos', 'right_lead_ref', 'right_lead_alt']] + ['sumstat.tsv.gz'])
                log = '/'.join([logs_folder] + [ str(rec[key]) for key in ['right_type', 'right_study_id', 'right_bio_feature', 'right_phenotype_id', 'right_lead_chrom', 'right_lead_pos', 'right_lead_ref', 'right_lead_alt']] + ['log_file.txt'])

                # Do not create paths here, since this is single-threaded. Instead
                # do it in the script itself.
                #os.makedirs(os.path.dirname(log), exist_ok=True)
                ld = None
                if rec['right_ld'] is not None:
                    ld = rec['right_ld']
                prepare_right_cmd = [
                    'python',
                    prepare_script,
                    '--sumstat', rec['right_sumstats'],
                    '--ld', ld,
                    '--split_ld',
                    '--study', rec['right_study_id'],
                    '--phenotype', rec['right_phenotype_id'],
                    '--bio_feature', rec['right_bio_feature'],
                    '--chrom', rec['right_lead_chrom'],
                    '--pos', rec['right_lead_pos'],
                    '--ref', rec['right_lead_ref'],
                    '--alt', rec['right_lead_alt'],
                    '--top_loci', top_loci_file,
                    '--window_coloc', window_colc,
                    '--window_cond', window_cond,
                    '--min_maf', min_maf,
                    '--tmpdir', rec['tmpdir'],
                    '--out', rec['right_reduced_sumstats'],
                    '--log', log
                ]
                if not rec['right_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['right_reduced_sumstats'])
                    cmd_str = ' '.join([str(arg) for arg in prepare_right_cmd])
                    # Skip if output exists
                    if os.path.exists(rec['right_reduced_sumstats']):
                        done_h.write(cmd_str + '\n')
                    else:
                        todo_h.write(cmd_str + '\n')
                        if not args.quiet:
                            print(cmd_str)

            # Save the matching manifest items as a new manifest for the coloc step
            #out_mani_h.write(json.dumps(rec) + '\n')
            out_mani_tsv_h.write('\t'.join([str(rec[key]) for key in manifest_cols]) + '\n')
        
    done_h.close()
    todo_h.close()
    #out_mani_h.close()
    out_mani_tsv_h.close()


def parse_args():
    ''' Load command line args
    '''
    p = argparse.ArgumentParser()

    p.add_argument('--type',
                   help=("Type of sumstats to use"),
                   type=str,
                   required=False)

    p.add_argument('--quiet',
                   help=("Don't print commands to stdout"),
                   action='store_true')
    
    args = p.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    main(args)
