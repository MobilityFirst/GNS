#!/usr/bin/env python
import os
import sys
from os.path import  dirname, exists
from group_by import group_by
from stats import get_stat_in_tuples
from write_array_to_file import	write_tuple_array, write_array
from plot_cdf import get_cdf_and_plot
import inspect

script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory

def main():
    folder = sys.argv[1]
    name_index = int(sys.argv[2])
    value_index = int(sys.argv[3])
    output_file = sys.argv[4]  #'retrans_by_ns.txt'
    
    if not folder.startswith('/home/abhigyan'):
        folder = os.path.join('/home/abhigyan/gnrs', folder)
    
    filename = os.path.join(folder, 'all_tuples.txt')
    
    if not exists(filename):
        print 'File does not exist:', filename
        return
    #output_stats_by_name(filename)
    
    group_and_write_output(filename, name_index, value_index, output_file, filter = fail_filter)


def group_and_write_output(filename, name_index, value_index, output_file, filter):
    folder = dirname(filename)
    
    outfile1 = os.path.join(folder, output_file)
    output_tuples1 = group_by(filename, name_index, value_index, filter)
    write_tuple_array(output_tuples1, outfile1, p = True)
                

def output_stats_by_name(all_tuples_filename):

    value_index = 4
    name_index = 0  # 0 = name, 1 = lns, 2 = ns

    # this option removes names for which there is a failed read request

    folder = dirname(all_tuples_filename)

    exclude_failed_reads = True
    if exclude_failed_reads:
        failed_reads_names = select_failed_reads_names(all_tuples_filename)
        write_array(failed_reads_names.keys(), os.path.join(folder, 'failed_reads_names.txt'))
        all_tuples_filename = write_all_tuples_excluding_failed(all_tuples_filename, failed_reads_names)

    outfile1 = os.path.join(folder, 'all_by_name.txt')
    output_tuples1 = group_by(all_tuples_filename, name_index, value_index)
    write_tuple_array(output_tuples1, outfile1, p = True)
    
    outfile2 =os.path.join(folder, 'writes_by_name.txt')
    output_tuples2 = group_by(all_tuples_filename, name_index, value_index, filter = write_filter)
    write_tuple_array(output_tuples2, outfile2, p = True)
    
    outfile3 = os.path.join(folder, 'reads_by_name.txt')
    output_tuples3 = group_by(all_tuples_filename, name_index, value_index, filter = read_filter)
    write_tuple_array(output_tuples3, outfile3, p = True)
    
    filenames = [outfile1, outfile2, outfile3]
    schemes = ['ALL', 'WRITES', 'READS']
    template_file = os.path.join(script_folder, 'template1.gpt')
    
    col_no = 4
    pdf_filename = os.path.join(folder, 'median_by_name.pdf')
    get_cdf_and_plot(filenames, schemes, [col_no]*len(schemes), pdf_filename, folder, template_file)
    
    col_no = 5
    pdf_filename = os.path.join(folder, 'mean_by_name.pdf')
    get_cdf_and_plot(filenames, schemes, [col_no]*len(schemes), pdf_filename, folder, template_file)
    
    # output key stats
    read_median_list = [t[4] for t in output_tuples3]
    read_mean_list = [t[5] for t in output_tuples3]
    write_median_list = [t[4] for t in output_tuples2]
    write_mean_list = [t[5] for t in output_tuples2]

    # delete this.
    #read_median_list2 = []
    #for v in read_median_list:
    #    if v <5000:
    #        read_median_list2.append(v)
    
    kv_tuples = []
    kv_tuples.extend(get_stat_in_tuples(read_median_list, 'read_median_names'))
    kv_tuples.extend(get_stat_in_tuples(read_mean_list, 'read_mean_names')) 
    kv_tuples.extend(get_stat_in_tuples(write_median_list, 'write_median_names'))
    kv_tuples.extend(get_stat_in_tuples(write_mean_list, 'write_mean_names'))
    
    outputfile = os.path.join(folder, 'latency_stats_names.txt')
    write_tuple_array(kv_tuples, outputfile, p = True)
    os.system('cat ' + outputfile)

exclude_ns = {}
x = [23, 24, 30, 57, 61, 71]
for i in x:
    exclude_ns[i] = 1


def write_filter_excluding_ns(tokens):
    """exclud a few NS in calculating write latencies"""
    ns = int(tokens[2])
    if tokens[5] == 'w' and ns not in exclude_ns:
        return True
    return False


def write_filter(tokens):
    if tokens[5] == 'w':
        return True
    return False


def read_filter(tokens):
    if tokens[5] == 'r':
        return True
    return False


def fail_filter(tokens):
    if tokens[5] == 'rf':
        return True
    return False


def retrans_filter(tokens):
    if tokens[5] == 'r' and int(tokens[3]) > 1:
        return True
    return False


def select_failed_reads_names(filename):
    names = {}
    for line in open(filename):
        tokens = line.split()
        if tokens[5] == 'rf':
            names[tokens[0]] = 1
    return names


def write_all_tuples_excluding_failed(all_tuples_file, exclude_names):
    new_filename = all_tuples_file + '_filter'
    fw = open(new_filename, 'w')
    for line in open(all_tuples_file):
        tokens = line.split()
        if tokens[0] not in exclude_names:
            fw.write(line)
    fw.close()
    return new_filename

if __name__ == "__main__":
    output_stats_by_name(sys.argv[1])
    #main()
