#!/usr/bin/env python
import os
import sys

def main():
    # step 1: for each file: output key stats
    schemes = ['locality', 'uniform', 'beehive', 'static3']
    output_folder =  '/home/abhigyan/gnrs/results/jan17/comparison'
    # system
    system_filename_file = '/home/abhigyan/gnrs/results/jan17/comparison/sys-files'
    system_files = open(system_filename_file).readlines()
    col = 1
    
    for i in range(len(schemes)):
        output_file = os.path.join(output_folder, 'sys-'+schemes[i])
        output_file_stats(output_file, system_files[i].strip(), col, '')

    # system-ping
    system_filename_file = '/home/abhigyan/gnrs/results/jan17/comparison/sys-ping-files'
    system_files = open(system_filename_file).readlines()
    col = 1
    
    for i in range(len(schemes)):
        output_file = os.path.join(output_folder, 'sys-ping-'+schemes[i])
        output_file_stats(output_file, system_files[i].strip(), col, '')
    
    # simulator-ping
    simulator_filename_file = '/home/abhigyan/gnrs/results/jan17/comparison/simulator-files'
    simulator_files = open(simulator_filename_file).readlines()
    col = 0
    
    for i in range(len(schemes)):
        output_file = os.path.join(output_folder, 'simulator-'+schemes[i])
        output_file_stats(output_file, simulator_files[i].strip(), col, '')

    #from stats import print_stats
    from tabulate_stats_valid import tabulate_stats
    #graph_format_file = '/home/abhigyan/gnrs/results/jan17/comparison/validation.gf'
    #tabulate_stats(graph_format_file)
    
    
    graph_format_file = '/home/abhigyan/gnrs/results/jan17/comparison/validation.gf'
    tabulate_stats(graph_format_file)
    # step 2: call tabulate output to plot stats
    
    # step 3: plot files using gnuplot
    
    

def output_file_stats(output_filename, input_filename, col, prefix):
    from select_columns import  extract_column_from_file
    values = extract_column_from_file(input_filename, col)
    from stats import get_stat_in_tuples
    output_tuples = get_stat_in_tuples(values, prefix)
    from write_array_to_file import write_tuple_array
    write_tuple_array(output_tuples, output_filename, p = True)


if __name__ == "__main__":
    main()
