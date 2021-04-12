import os
import sys
from pyspark import SparkContext, SparkConf

infile = 'file:///gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot-analysis/data/bible.txt'
outdir = 'file:///gpfs/alpine/stf008/proj-shared/hs2/spider2-snapshot-analysis/scratch'

def main():
    conf = SparkConf().setAppName("WordCount")
    sc = SparkContext(conf=conf)
    words = sc.textFile(infile).flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)

    jobid = os.environ['SLURM_JOBID']
    outfile = outdir + '/' + str(jobid)

    wordCounts.saveAsTextFile(outfile)


if __name__ == "__main__":
    main()

