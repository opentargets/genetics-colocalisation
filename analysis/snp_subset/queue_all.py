#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy, Jeremy Schwartzentruber

import sys
import os
import time
import re
import argparse
import subprocess as sp

def main():

    # Args
    args = parse_args()
    print_only = False

    in_paths = 'gcs_input_paths.txt'
    completed_paths = 'gcs_completed_paths.txt'
    script = 'filter_on_pval.py'
    cluster_name = 'js-sumstat-filter'

    # Load set of completed studies
    completed_studies = set([])
    with open(completed_paths, 'r') as in_h:
        for line in in_h:
            completed_studies.add(os.path.basename(os.path.dirname(line.rstrip())))

    # Run each job in the manifest
    jobnum = 0
    for i, line in enumerate(open(in_paths, 'r')):
        
        # if i != 0:
        #     continue

        # Make path names
        in_path = os.path.dirname(line.rstrip())
        study = os.path.basename(in_path)
        data_type = get_datatype(in_path)
        out_path = os.path.join(args.out_dir, data_type, study)

        # Skip completed
        if study in completed_studies:
            print('Skipping {}'.format(study))
            continue
        
        # Build script args
        script_args = [
            '--in_sumstats', in_path,
            '--out_sumstats', out_path,
            '--pval', str(float(args.pval)),
            '--data_type', data_type
        ]

        # Build command
        cmd = [
            'gcloud dataproc jobs submit pyspark',
            '--cluster={0}'.format(cluster_name),
            '--properties spark.submit.deployMode=cluster',
            '--async',
            '--project=open-targets-genetics-dev',
            '--region=europe-west1',
            script,
            '--'
        ] + script_args
        
        # Run command
        jobnum = jobnum + 1
        print('Running job {0}...'.format(jobnum))
        print(' '.join(cmd))
        if not print_only:
            sp.call(' '.join(cmd), shell=True)
            print('Complete\n')
            time.sleep(0.25)

    return 0


def get_datatype(s):
    ''' Parses datatype from the input path name
    '''
    if re.search('molecular_trait', s):
        data_type = 'molecular_trait'
    elif re.search('gwas', s):
        data_type = 'gwas'
    else:
        sys.exit(f'Error: could not determine data type from path name: {s}')
    return data_type


def parse_args():
    p = argparse.ArgumentParser()

    # p.add_argument('--in_paths',
    #                help=("Input summary stats _SUCCESS file paths"),
    #                metavar="<file>", type=str, required=True)
    p.add_argument('--out_dir',
                   help=("Output summary stats directory"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--pval',
                   help=("pval threshold"),
                   metavar="<float>", type=float, required=True)

    args = p.parse_args()
    return args


if __name__ == '__main__':

    main()
