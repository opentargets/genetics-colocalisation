#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import os
import sys
import pandas as pd

def main():

    # Parse args
    inf = 'output/overlap_table.tsv.gz'
    outf = 'configs/manifest_file.tsv'
    prop_threshold = 0.01

    # Load
    df = pd.read_csv(inf, sep='\t', compression='gzip')

    # Only keep rows where A is a disease and B is a molecular QTL
    # and proportion of overlapping variants greater than threshold
    print('Number of tests pre-filter: {0}'.format(df.shape[0]))
    to_keep = ( (pd.isnull(df.cell_id_A)) &
                (~pd.isnull(df.cell_id_B)) &
                (df.proportion_overlap > prop_threshold) )
    df = df.loc[to_keep, :]
    print('Number of tests post-filter: {0}'.format(df.shape[0]))

    # Write temp
    df.to_csv(outf, sep='\t', index=None)

    return 0


if __name__ == '__main__':

    main()
