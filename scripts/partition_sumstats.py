#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import pyspark.sql
from pyspark.sql.functions import *
import logging
import argparse
import sys
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(process)d - %(filename)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

def main(args):
    logger.info('Started partitioning {} data.'.format(args.input))
    
    # Make spark session
    spark = (
        pyspark.sql.SparkSession.builder
        .config("spark.master", "local[*]")
        .config("parquet.summary.metadata.level", "ALL")
        .getOrCreate()
    )
    logger.debug('Spark version: {}'.format(spark.version))
    logger.info('Read {} input data'.format(args.input))
    df = spark.read.parquet(args.input)
    (df.write.partitionBy('study_id', 'bio_feature', 'chrom', 'phenotype_id')
        .parquet(args.output, compression='snappy', mode='append'))
    logger.info('Finished partitioning {} data.'.format(args.input))


def parse_args_or_fail():
    ''' Load command line args.
    '''
    p = argparse.ArgumentParser(description='Partition sumstats parquet data by following columns: study_id, phenotype_id, bio_feature and chrom columns and by 100000 items on pos column')

    p.add_argument('input', help=("Input summary stats parquet data."), metavar="<input_parquet>", type=str)
    p.add_argument('output', help=("Output summary stats parquet data."), metavar="<output_parquet>", type=str)

    if len(sys.argv)==1:
        p.print_help(sys.stderr)
        sys.exit(1)

    args = p.parse_args()

    return args


if __name__ == '__main__':
    args = parse_args_or_fail()
    logger.debug('Ensuring {} output folder exists.'.format(args.output))
    os.makedirs(args.output, exist_ok=True)
    main(args)
