import os, sys
import hashlib
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession, functions
from pyspark.sql.types import *
from pyspark.sql.functions import udf
#import pyarrow.parquet as pq

#pfile = pq.read_table("file.parquet")
#print("Column names: {}".format(pfile.column_names))
#print("Schema: {}".format(pfile.schema))

#fsdata_dir = "/gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot/fsdata/"
#result_dir = "/gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot/result/"
scratch = '/gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot-analysis'
fsdata_dir = scratch + '/data/fsdata'

def namehash(s):
    pathname = s.replace("/ROOT/", "/")
    e = [ hashlib.sha1(e.encode('utf-8')).hexdigest() \
            for e in pathname.split('/')[1:]]
    return "/" + "/".join(e)

def idhash(n):
    key = 0x7a57b33
    return (n ^ key)

def main():
    spark = SparkSession.builder.appName("SnapshotAnonymize") \
            .getOrCreate()

    dates = os.listdir(fsdata_dir) 
    jobid = os.environ['SLURM_JOBID']

    print("JOBID = {}".format(jobid))

    for d in dates:
        #outdir = "file://" + result_dir + d
        path = "file://" + fsdata_dir + '/' + d + "/parquet"
        outdir = "file://" + scratch + '/result/' + str(jobid) + '/' + d

        print("current file: {}".format(path))

        df = spark.read.parquet(path)
        df.createOrReplaceTempView("fsdata")
        #old = spark.sql("select * from fsdata limit 100")
        spark.sql("desc formatted fsdata").show()

        sys.exit(0)
#
#        namehash_udf = udf(lambda path: namehash(path))
#        idhash_udf = udf(lambda n: idhash(n))
#        updated = old.withColumn('path', namehash_udf(df.path)) \
#                     .withColumn('uid', idhash_udf(df.uid)) \
#                     .withColumn('gid', idhash_udf(df.gid))
#
#        updated.write.csv(outdir)


if __name__ == "__main__":
    main()

