#!/usr/bin/env python

import os
import sys

f = open(sys.argv[1])

fw = open(sys.argv[2], 'w')
for line in f:
    tokens = line.split()
    if len(tokens) == 2:
        fw.write(tokens[1])
        fw.write('\n')
fw.close()


