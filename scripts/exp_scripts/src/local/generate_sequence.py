#!/usr/bin/env python
import os
import sys
from write_array_to_file import *
from read_array_from_file import read_col_from_file
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



def write_single_name_trace(filename, number, name):
    folder = os.path.split(filename)[0]
    os.system('mkdir -p ' + folder)
    fw = open(filename, 'w')
    for i in range(number):
        fw.write(str(name) + '\n')
    fw.close()


def write_random_name_trace(filename, names, size):
    #filename = sys.argv[1]
    #names = int(sys.argv[2])
    #size = int(sys.argv[3])
    from random import randint
    x = []
    for i in range(size):
        x.append(randint(0, names - 1))
    #y = []
    #y.extend(x)
    #y.extend(x)
    from write_array_to_file import write_array
    write_array(x, filename,  p = True)
    



### THIS CODE IS USED ####
def write_sequence_name_trace():
    filename = sys.argv[1]
    size = int(sys.argv[2])
    x = range(size)
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
    y = []
    #y.extend(x)
    y.extend(x)
    from write_array_to_file import write_array
    write_array(y, filename,  p = True)
    

def gen_trace_for_lns():
    f = open('pl_lns')
    folder = sys.argv[1]
    num_queries = 50
    name = '1'
    for line in f:
        lns = line.strip()
        filename = os.path.join(folder,'lookup_' + lns)
        write_single_name_trace(filename, num_queries, name)
    

if __name__ == "__main__":
    #write_all_traces()
    #write_queries()
    #write_trace()
    #write_queries()
    #write_sequence_name_trace()
    #write_random_name_trace(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    gen_trace_for_lns()

