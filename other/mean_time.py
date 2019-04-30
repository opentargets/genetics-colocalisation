#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Ed Mountjoy
#

import os
import sys
import glob

def main():

    # Args
    in_pattern = '/home/ubuntu/results/coloc/logs/left_study=*/left_phenotype=*/left_bio_feature=*/left_variant=*/right_study=*/right_phenotype=*/right_bio_feature=*/right_variant=*/log_file.txt'
    mean_interval = 1000

    # Intite
    times = []
    total_count = 0
    mean = 0.0

    # Iterate over log files, outputting the rolling mean
    for i, inf in enumerate(glob.iglob(in_pattern)):

        # Once iterated over a set number of files, output rolling mean
        if i > 0 and i % mean_interval == 0:

            # Add to rolling mean
            mean = (mean * total_count + sum(times)) / (total_count + len(times))

            # Change count
            total_count += len(times)

            # Print rolling mean
            print('iteration {0}, mean {1:.1f} sec'.format(i, mean))

            # Reset times list
            times = []
        
        # Parse time
        sec = parse_log(inf)
        if sec is not None:
            times.append(sec)
    
    return 0

def parse_seconds(time_str):
    ''' Converts the time string into seconds
    '''
    h, m, s = time_str.split(':')
    combined_s = float(s) + 60 * int(m) + 60 * 60 * int(h)
    return combined_s

def parse_log(logf):
    ''' Parse the time taken from the log file
    '''
    with open(logf, 'r') as in_h:
        for line in in_h:
            if "Time taken: " in line:
                time_str = line.rstrip().split('Time taken: ')[1]
                return parse_seconds(time_str)
    return None

if __name__ == '__main__':

    main()
