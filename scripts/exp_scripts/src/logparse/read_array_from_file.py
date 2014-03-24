#!/usr/bin/env python
'''
Created on Jul 31, 2012

@author: abhigyan
'''
import os,sys

def read_array_from_file(filename):
    
    f = open(filename)
    array2d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array2d.append(tokens)
    return array2d


def read_col_from_file(filename):
    
    f = open(filename)
    array1d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array1d.append(tokens[0])
    return array1d
    
def read_col_from_file2(filename, col_no):
    f = open(filename)
    array1d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array1d.append(tokens[col_no])
        #print tokens[col_no]
    return array1d
    


if __name__ == "__main__":
    import os,sys
    col = read_col_from_file2(sys.argv[1], int(sys.argv[2]))
