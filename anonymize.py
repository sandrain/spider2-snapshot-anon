import os, sys
import hashlib
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession, functions
from pyspark.sql.types import *
from pyspark.sql.functions import udf

scratch = '/gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot-analysis'
fsdata_dir = scratch + '/data/fsdata'
anon_dir = scratch + '/result/anon-csv'

def namehash(s):
    pathname = s.replace("/ROOT/", "/")
    e = [ hashlib.sha1(e.encode('utf-8')).hexdigest() \
            for e in pathname.split('/')[1:]]
    return "/" + "/".join(e)

def idhash(n):
    # original key is blinded
    key = 0x10101010
    return str(int(n) ^ key)

def do_date(spark, d):
    path = 'file://' + fsdata_dir + '/' + d + "/parquet"
    outdir_path = anon_dir + '/' + d
    outdir = 'file://' + outdir_path

    if os.path.isfile(outdir_path):
        print('Already processed, skipping .. {}'.format(path))
        return

    print('Processing .. {}'.format(path))

    try:
        df = spark.read.parquet(path)
        df.createOrReplaceTempView("fsdata")
        old = spark.sql("select * from fsdata")

        namehash_udf = udf(lambda path: namehash(path))
        idhash_udf = udf(lambda n: idhash(n))
        updated = old.withColumn('path', namehash_udf(df.path)) \
                     .withColumn('uid', idhash_udf(df.uid)) \
                     .withColumn('gid', idhash_udf(df.gid))

        updated.write.csv(outdir)
        print('Done processing .. {}'.format(outdir))

    except Exception as e:
        print('Exception while processing .. {}'.format(path))
        print(e)


def main():
    spark = SparkSession.builder.appName("SnapshotAnonymize") \
            .getOrCreate()

    # if dates is given, just do with the given dates
    if len(sys.argv) > 1:
        dates = sys.argv[1:]
    else:
        dates = os.listdir(fsdata_dir) 

    jobid = os.environ['SLURM_JOBID']

    print('JobID={}'.format(jobid))

    for d in dates:
        do_date(spark, d)

if __name__ == "__main__":
    main()

