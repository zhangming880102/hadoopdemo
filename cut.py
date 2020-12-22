from __future__ import print_function

import codecs
import sys
import subprocess
from pyspark import SparkContext
from operator import add
import jieba

def cut(line):
	return jieba.cut(line)

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: srt dst")
		exit(-1)
	sc = SparkContext(appName = "WeiboCut")
	rdd = sc.textFile(sys.argv[1], use_unicode=False)
	# flatmap
	rdd = rdd.flatMap(lambda line:cut(line))
	# map
	rdd =rdd.map(lambda x:(x,1))
	# reduce
	rdd =rdd.reduceByKey(lambda a,b:a+b)
	# sort
	rdd =rdd.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x:(x[0],x[1]))
	rdd.saveAsTextFile(sys.argv[2])
	sc.stop()
