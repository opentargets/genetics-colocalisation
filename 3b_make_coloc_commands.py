#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Reads the manifest file and makes commands (optimised version)
#

import argparse
import gzip
import json
import os
import sys


def main():

    # Args
    args = parse_args()
    in_manifest = '/configs/coloc_manifest.json.gz'
    out_todo = '/configs/commands_todo.coloc.txt.gz'
    out_done = '/configs/commands_done.coloc.txt.gz'
    out_manifest = '/configs/coloc_manifest_opt.txt.gz'

    # Pipeline args
    #script = 'scripts/coloc_opt.py'
    script = 'scripts/coloc_opt.R'
    make_plots = False

    # Open command files
    todo_h = gzip.open(out_todo, 'w')
    done_h = gzip.open(out_done, 'w')
    out_manifest_h = gzip.open(out_manifest, 'w')
    
    # Iterate over manifest
    with gzip.open(in_manifest, 'r') as in_mani:
        for line in in_mani:

            # Parse
            rec = json.loads(line.decode().rstrip())

            if not 'left_reduced_sumstats' in rec:
               print("Skipping a command as the following record missing left_reduced_sumstats:" + str(rec), file=sys.stderr) 
               continue

            if not 'right_reduced_sumstats' in rec:
               print("Skipping a command as the following record missing right_reduced_sumstats:" + str(rec), file=sys.stderr) 
               continue

            # Build command
            log = os.path.abspath(rec['log'])
            outpath = os.path.abspath(rec['out'])
            os.makedirs(os.path.dirname(log), exist_ok=True)
            os.makedirs(os.path.dirname(outpath), exist_ok=True)
            cmd = [
                'Rscript',
                os.path.abspath(script),
                os.path.abspath(rec['left_reduced_sumstats']),
                os.path.abspath(rec['right_reduced_sumstats']),
                outpath,
                #'--tmpdir', os.path.abspath(rec['tmpdir']),
                #'--delete_tmpdir',
                '>>', log, '2>&1'
                ]

            if make_plots:
                cmd = cmd + ['--plot', os.path.abspath(rec['plot'])]
            
            cmd_str = ' '.join([str(arg) for arg in cmd])

            # Skip if output exists
            if os.path.exists(rec['out']):
                done_h.write((cmd_str + '\n').encode())
                continue
            else:
                todo_h.write((cmd_str + '\n').encode())
                if not args.quiet:
                    print(cmd_str)
                # Also write to a manifest for the optimised coloc script
                manifest_line = (
                    '\t'.join([os.path.abspath(rec['left_reduced_sumstats']),
                               os.path.abspath(rec['right_reduced_sumstats']),
                               os.path.abspath(rec['out']),
                               log]) + '\n')
                out_manifest_h.write(manifest_line.encode())
    
    # Close files
    done_h.close()
    todo_h.close()
    out_manifest_h.close()

    return 0

def parse_args():
    ''' Load command line args
    '''
    p = argparse.ArgumentParser()

    p.add_argument('--quiet',
                   help=("Don't print commands to stdout"),
                   action='store_true')

    args = p.parse_args()
    return args

if __name__ == '__main__':

    main()
