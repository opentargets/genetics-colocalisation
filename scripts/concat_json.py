#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import argparse
import pandas as pd

def main():

    # Args
    args = parse_args()

    # Load
    dfs = (pd.read_json(inf, orient='records', lines=True)
           for inf in args.in_json)

    # Concatenate
    full_df = pd.concat(dfs, ignore_index=True)

    # Write json
    full_df.to_json(
        args.out,
        orient='records',
        lines=True
    )

    # Write tsv
    full_df.to_csv(
        args.out.replace('.json', '.tsv'),
        sep='\t',
        index=None
    )

    return 0

def parse_args():
    ''' Load command line args
    '''
    p = argparse.ArgumentParser()

    # Add input files
    p.add_argument('--in_json',
                   metavar="<file>",
                   help=("List of json files to concatenate"),
                   type=str,
                   nargs='+',
                   required=True)
    p.add_argument('--out',
                   metavar="<file>",
                   help=("Concatenated json file"),
                   type=str,
                   required=True)

    args = p.parse_args()

    return args

if __name__ == '__main__':

    main()
