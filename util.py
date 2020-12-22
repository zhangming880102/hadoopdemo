from hdfs import *

cli=Client('http://master:50070')

def upload(src,dst):
	cli.upload(src,dst)
