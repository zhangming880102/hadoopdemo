# coding:utf8
import happybase
import util
import sys

con = happybase.Connection(host="master",port=9090)
con.open()

table=happybase.Table('test',con)
scanner=table.scan()
f=open(sys.argv[1],'w')
for row,va in scanner:
	for k in va:
		f.write(va[k].decode('utf-8')+'\n')
f.close()
util.upload(sys.argv[2],sys.argv[1])
