#!/usr/bin/env python
import os,sys
fw = open(sys.argv[1],'w')

val = sys.argv[2]
count = int(sys.argv[3])

for i in range(count):
    fw.write(str(i*1.0/count) + '\t' + val + '\n')
fw.close()
