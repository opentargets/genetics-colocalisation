#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#
'''
Partition the top loci table, useful for speeding up coloc pipeleine.
'''

import sys
import gzip
import json
import os

def main():

    # Args
    in_file = '/data/finemapping/top_loci.json.gz'
    out_dir = '/data/finemapping/top_loci_by_chrom'

    # Make outdir
    os.makedirs(out_dir, exist_ok=False)
    
    # Dict to hold output handles
    out_handles = {}

    # Iterate over input lines
    with gzip.open(in_file, 'r') as in_h:
        for line in in_h:

            # Get chrom name
            l_decode = line.decode()
            chrom = json.loads(l_decode)['chrom']
            
            # Write line to correct out handle
            try:
                out_handles[chrom].write(l_decode)
            except KeyError:
                # Make handle
                out_path = os.path.join(out_dir, "{}.json".format(chrom))
                out_handles[chrom] = open(out_path, 'w')
                # Write decoded line
                out_handles[chrom].write(l_decode)
    
    # Close all handles
    for chrom in out_handles:
        out_handles[chrom].close()

    return 0

if __name__ == '__main__':

    main()
