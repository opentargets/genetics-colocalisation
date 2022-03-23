#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# THIS SCRIPT IS NOW OBSOLETE.
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
    cmd_todo = '/configs/commands_todo.coloc.txt.gz'
    cmd_done = '/configs/commands_done.coloc.txt.gz'
    manifest_todo = '/configs/coloc_manifest_opt.todo.txt.gz'
    manifest_done = '/configs/coloc_manifest_opt.done.txt.gz'

    # Pipeline args
    #script = 'scripts/coloc_opt.py'
    script = 'scripts/coloc_opt.R'
    make_plots = False

    # Command files - individual coloc commands
    if args.individual_commands:
        todo_cmd_h = gzip.open(cmd_todo, 'w')
        done_cmd_h = gzip.open(cmd_done, 'w')
    # Manifest for optimised (batch) coloc commands
    todo_manifest_h = gzip.open(manifest_todo, 'w')
    done_manifest_h = gzip.open(manifest_done, 'w')
    
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

            # This step can be extremely slow when building millions of commands.
            # It's only needed with the old (non-optimised) way of running commands.
            if args.individual_commands:
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

            # Also write to a manifest for the optimised coloc script
            manifest_line = (
                '\t'.join([os.path.abspath(rec['left_reduced_sumstats']),
                            os.path.abspath(rec['right_reduced_sumstats']),
                            outpath,
                            log]) + '\n')

            # Skip if output exists
            if os.path.exists(rec['out']):
                done_manifest_h.write(manifest_line.encode())
                if args.individual_commands:
                    done_cmd_h.write((cmd_str + '\n').encode())
                continue
            else:
                todo_manifest_h.write(manifest_line.encode())
                if args.individual_commands:
                    todo_cmd_h.write((cmd_str + '\n').encode())
                if not args.quiet:
                    print(cmd_str)
    
    # Close files
    todo_manifest_h.close()
    done_manifest_h.close()
    if args.individual_commands:
        todo_cmd_h.close()
        done_cmd_h.close()

    return 0

def parse_args():
    ''' Load command line args
    '''
    p = argparse.ArgumentParser()

    p.add_argument('--quiet',
                   help=("Don't print commands to stdout"),
                   action='store_true')
    p.add_argument('--individual_commands',
                   help=("Don't print commands to stdout"),
                   action='store_true',
                   default=False)

    args = p.parse_args()
    return args

if __name__ == '__main__':

    main()
