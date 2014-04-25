#!/usr/bin/env python
import os
import sys


def main():
    graph_format_file = sys.argv[1]
    tabulate_stats(graph_format_file)


def tabulate_stats(graph_format_file):
    # read params
    stat_names, output_file, columns,  folder_names = \
            read_graph_format(graph_format_file)
    
    # read values
    
    values = read_table_values(folder_names, stat_names, columns)
    
    # write values to output_file
    from write_array_to_file import write_tuple_array
    write_tuple_array(values, output_file, p = True)
    os.system('cat ' + output_file)

def read_graph_format(filename):
    f = open(filename)
    
    lines = f.readlines()
    stat_names = lines[0].strip().split() # line 0: stat_name
    output_file = lines[1].strip() # line 1: output_file
    columns = lines[2].strip().split()
    
    #rows = lines[3].strip().split()
    #print 'rows', rows
    folder_names = []
    count = 4 # line 3 is empty, line 3 onwards folder names.
    while count < len(lines):
        if lines[count].strip() != '':
            folder_names.append(lines[count].strip())
        count += 1
    return stat_names, output_file, columns, folder_names

def read_table_values(folder_names, stat_names, columns):
    values = []
    tuple1 = ['x']
    tuple1.extend(columns)
    values.append(tuple1)
    print stat_names
    print folder_names
    for stat_name in stat_names:
        tuple1 = [stat_name]

        for f in folder_names:
            if os.path.isdir(f):
                tuple1.append(get_stat_from_folder(stat_name, f))
            else:
                tuple1.append(get_stat_from_file(stat_name, f))
        values.append(tuple1)
    return values


def get_stat_from_folder(stat_name, folder):
    stat_files = ['summary.txt', 'latency_stats.txt','latency_stats_names.txt', 'ns-fairness.txt','avg_replica_count4.txt']
    files = []
    for f1 in stat_files:
        files.append(os.path.join(folder, f1))
        
    lines = []
    for f in files:
        if os.path.isfile(f):
            lines.extend(open(f).readlines())
    for line in lines:
        tokens = line.strip().split('\t')
        if tokens[0] == stat_name:
            return tokens[1]
    return -1


def get_stat_from_file(stat_name, filename):
    if not os.path.exists(filename):
        return -1
    lines = []
    lines.extend(open(filename).readlines())
    #for f in filenames:
    #        lines.extend(open(f).readlines())
    
    for line in lines:
        tokens = line.strip().split('\t')
        if tokens[0] == stat_name:
            return tokens[1]
    return -1

if __name__ == "__main__":
    main()
