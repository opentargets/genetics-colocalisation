#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Jeremy Schwartzentruber
#

import argparse
import pyspark.sql
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():

    # Args
    args = parse_args()

    # Make spark session
    global spark
    spark = (
        pyspark.sql.SparkSession.builder
            .config("parquet.summary.metadata.level", "ALL")
            .getOrCreate()
        #    .master("local[*]")
    )
    print('Spark version: ', spark.version)

    # Load
    df = spark.read.parquet(args.in_sumstats)

    df_flt = df.filter(F.col('pval') <= args.pval)

    # Write output
    if args.data_type == 'gwas':
        (
            df_flt
            .write
            .parquet(
                args.out_sumstats,
                mode='overwrite'
            )
        )
    elif args.data_type == 'molecular_trait':
        (
            df_flt
            .write
            .partitionBy('bio_feature', 'chrom')
            .parquet(
                args.out_sumstats,
                mode='overwrite'
            )
        )
    
    return 0


def parse_args():
    p = argparse.ArgumentParser()

    p.add_argument('--in_sumstats',
                   help=("Input: summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--out_sumstats',
                   help=("Output: summary stats parquet file"),
                   metavar="<file>", type=str, required=True)
    p.add_argument('--pval',
                   help=("pval threshold in window. Only used if --data_type gwas"),
                   metavar="<float>", type=float, required=True)
    p.add_argument('--data_type',
                   help=("Whether dataset is of GWAS or molecular trait type"),
                   metavar="<str>", type=str, choices=['gwas', 'molecular_trait'], required=True)

    args = p.parse_args()
    return args
    

if __name__ == '__main__':

    main()
