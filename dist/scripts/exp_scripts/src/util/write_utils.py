"""
Created on Mar 5, 2012

@author: abhigyan
"""
import os


def write_array(array, output_file, p=True):
    if os.path.dirname(output_file) != '' and not os.path.exists(os.path.dirname(output_file)):
        os.system('mkdir -p ' + os.path.dirname(output_file))
    fw = open(output_file, 'w')
    for val in array:
        fw.write(str(val) + '\n')
    fw.close()
    if p:
        print "Output File:", output_file


def write_tuple_array(tuple_array, output_file, p=True, append=False):
    if os.path.dirname(output_file) != '' and not os.path.exists(os.path.dirname(output_file)):
        os.system('mkdir -p ' + os.path.dirname(output_file))

    if append:
        fw = open(output_file, 'a')
    else:
        fw = open(output_file, 'w')
    for t in tuple_array:
        for val in t:
            fw.write(str(val) + '\t')
        fw.write('\n')
    fw.close()
    if p:
        print "Output File:", output_file
