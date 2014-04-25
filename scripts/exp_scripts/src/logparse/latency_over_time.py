#!/usr/bin/env python

import os, sys
import random
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

gnuplot_file_write = os.path.join(script_folder, 'plot_over_time_write.gp')

gnuplot_file_read = os.path.join(script_folder, 'plot_over_time_read.gp')

gnuplot_file_latency = os.path.join(script_folder, 'plot_time_vs_latency.gp')


def main():
    tuples_file = sys.argv[1]
    #failed_over_time(tuples_file)
    latency_over_time(tuples_file)


def latency_over_time(tuples_file):

    folder = os.path.split(tuples_file)[0]
    line_max = 20000
    f = open(tuples_file)
    lines = f.readlines()
    p_th = 1.0
    if line_max < len(lines):
        p_th = line_max*1.0/len(lines)
    
    select_lines = []
    for line in lines:
        p = random.random()
        if p > p_th:
            continue
        tokens = line.split()
        select_lines.append(line)
    
    from write_array_to_file import write_array
    filename = os.path.join(folder, 'latency-over-time.txt')
    write_array(select_lines, filename, p = True)
    try:
        os.system('cd ' + folder + '; gnuplot ' + gnuplot_file_latency)
    except:
        print 'ERROR: gnuplot error'


def failed_over_time(tuples_file):

    folder = os.path.split(tuples_file)[0]
    #name_coordinator_ns = get_name_ns_mapping()
    
    f = open (tuples_file)
    reads = []
    writes = []
    lines = f.readlines()
    for line in lines:
        p = random.random()
        #if p > 0.05:
        #    continue
        tokens = line.split()
        if tokens[5] == 'wf':
            writes.append([tokens[6], tokens[1], tokens[0], tokens[2]])
            

        if tokens[5] == 'rf':
            reads.append([tokens[6], tokens[1], tokens[0], tokens[2]])
            #reads.append([tokens[6], tokens[1], tokens[0], name_coordinator_ns[tokens[0]]])
    
    from write_array_to_file import write_tuple_array
    filename = os.path.join(folder,'write-failed.txt')
    write_tuple_array(writes, filename, p = True)
    filename = os.path.join(folder,'read-failed.txt')
    write_tuple_array(reads, filename, p = True)
    
#    os.system('cp ' + gnuplot_file + ' ' + folder)
    try:
        os.system('cd ' + folder + '; gnuplot ' + gnuplot_file_write)
        os.system('cd ' + folder + '; gnuplot ' + gnuplot_file_read)
    except:
        print 'ERROR: gnuplot error'
    

def get_name_ns_mapping():
    ## not used now
    name_coordinator_ns_file = '/home/abhigyan/gnrs/logparse/nameCoordinator.txt'
    name_coordinator_ns = {}
    f = open(name_coordinator_ns_file)
    for line in f:
        tokens = line.split()
        #print tokens
        name_coordinator_ns[tokens[0]] = tokens[1]
    return name_coordinator_ns
    
if __name__ == "__main__":
    main()
