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
    out_manifest = '/configs/coloc_manifest.json.gz'
    out_todo = '/configs/commands_todo.txt.gz'
    out_done = '/configs/commands_done.txt.gz'
    cache_folder = '/output/cache/'
    os.makedirs(cache_folder, exist_ok=True)


    # Pipeline args
    prepare_script = 'scripts/select_relevant_sumstat.py'
    top_loci_file = '/data/top_loci_by_chrom/CHROM.json'
    window_colc = 500 # in KB
    window_cond = 1000  # in KB
    min_maf = 0.01

    # Open command files
    todo_h = gzip.open(out_todo, 'w')
    done_h = gzip.open(out_done, 'w')

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
                log = os.path.abspath(rec['log'])
                os.makedirs(os.path.dirname(log), exist_ok=True)
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
                    '--out', rec['left_reduced_sumstats'],
                    '>>', log, '2>&1'
                ]
                if not rec['left_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['left_reduced_sumstats'])
                    cmd_str = ' '.join([str(arg) for arg in prepare_left_cmd])
                    # Skip if output exists
                    if os.path.exists(rec['left_reduced_sumstats']):
                        done_h.write((cmd_str + '\n').encode())
                        continue
                    else:
                        todo_h.write((cmd_str + '\n').encode())
                        if not args.quiet:
                            print(cmd_str)

            if args.type is None or args.type == rec['right_type']:
                # Prepare right sumstat
                rec['right_reduced_sumstats'] = os.path.join(cache_folder, *[ str(rec[key]) for key in ['right_type', 'right_study_id', 'right_phenotype_id', 'right_bio_feature', 'right_lead_chrom', 'right_lead_pos', 'right_lead_ref', 'right_lead_alt']])
                log = os.path.abspath(rec['log'])
                os.makedirs(os.path.dirname(log), exist_ok=True)
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
                    '--out', rec['right_reduced_sumstats'],
                    '>>', log, '2>&1'
                ]
                if not rec['right_reduced_sumstats'] in files_to_cache:
                    files_to_cache.add(rec['right_reduced_sumstats'])
                    cmd_str = ' '.join([str(arg) for arg in prepare_right_cmd])
                    # Skip if output exists
                    if os.path.exists(rec['right_reduced_sumstats']):
                        done_h.write((cmd_str + '\n').encode())
                        continue
                    else:
                        todo_h.write((cmd_str + '\n').encode())
                        if not args.quiet:
                            print(cmd_str)

            out_json_lines.add(json.dumps(rec) + "\n")
        
    done_h.close()
    todo_h.close()
    
    # Save the matching manifest items as a new manifest for the coloc step
    with gzip.open(out_manifest, 'wt') as out_mani:
        out_mani.writelines(out_json_lines)


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
