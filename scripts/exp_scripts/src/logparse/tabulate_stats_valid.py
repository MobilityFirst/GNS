#!/usr/bin/env python
import os
import sys

def main():
    graph_format_file = sys.argv[1]
    tabulate_stats(graph_format_file)
    
def tabulate_stats(graph_format_file):
    # read params
    stat_name, output_file, file_names = \
            read_graph_format(graph_format_file)
    print stat_name
    print output_file
    print file_names
    # read values
    
    values = read_values(stat_name, file_names)
    
    # write values to output_file
    from write_array_to_file import write_array
    write_array(values, output_file, p = True)
    os.system('cat ' + output_file)

def read_graph_format(filename):
    f = open(filename)
    
    lines = f.readlines()
    stat_name =  lines[0].strip()
    #stat_names = lines[0].strip().split() # line 0: stat_name
    output_file = lines[1].strip() # line 1: output_file
    #columns = lines[2].strip().split()
    
    #rows = lines[3].strip().split()
    #print 'rows', rows
    folder_names = []
    count = 3 # line 2 is empty, line 3 onwards folder names.
    while count < len(lines):
        if lines[count].strip() != '':
            folder_names.append(lines[count].strip())
        count += 1
    return stat_name, output_file, folder_names


def read_values(stat_name, filenames):
    values = []
    for filename in filenames:
        values.append(get_stat_from_file(stat_name, filename))
    return values
    
    
def get_stat_from_file(stat_name, filename):
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
