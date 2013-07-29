#!/usr/bin/env python2.7
import os
import sys
from write_array_to_file import *
#from read_array_from_file import read_col_from_file
from random import shuffle

def write_all_traces():
    files = os.listdir('lookupTrace')
    
    i = 0
    number = 1000
    for f in files:
        hostname = f[len('lookup_'):]
        fname = os.path.join('lookupTrace', f)
        write_trace(fname, number, i)
        fname = os.path.join('updateTrace', 'update_' + hostname)
        write_trace(fname, 0, i)
        fname = os.path.join('workloadTrace', 'workload_' + hostname)
        write_trace(fname, 1, i)
        i = (i + 1) % 100


def write_trace(filename, number, name):
    fw = open(filename, 'w')
    for i in range(number):
        fw.write(str(name) + '\n')
    fw.close()



### THIS CODE IS USED ####
def write_trace():
    filename = sys.argv[1]
    size = int(sys.argv[2])
    #x = range(size)
    x = [0]*size
    from write_array_to_file import write_array
    write_array(x, filename,  p = True)


def write_queries():
    filename = sys.argv[1]
    names = int(sys.argv[2])
    size = int(sys.argv[3])
    from random import randint
    x = []
    for i in range(size):
        x.append(randint(0, names - 1))
    from write_array_to_file import write_array
    write_array(x, filename,  p = True)
    

if __name__ == "__main__":
    #write_all_traces()
    #write_queries()
    write_trace()
