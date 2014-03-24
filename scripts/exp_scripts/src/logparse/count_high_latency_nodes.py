#!/usr/bin/env python
import os
import sys
from group_by import group_by

def main():
    tuples_file = sys.argv[1]
    count_high_latency_nodes(tuples_file)
    count_successful_requests_by_nodes(tuples_file)


def count_high_latency_nodes(tuples_file):
    group_by_index = 1 # lns
    value_index = 4 # latency
    filter = high_read_latency_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)
    
    from write_array_to_file import write_tuple_array
    high_latency_lns_file = os.path.join(os.path.split(tuples_file)[0], 'high_latency_lns.txt')
    write_tuple_array(output_tuples, high_latency_lns_file, True)
    
    group_by_index = 2 # ns
    value_index = 4 # latency
    filter = high_read_latency_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)

    from write_array_to_file import write_tuple_array
    high_latency_ns_file = os.path.join(os.path.split(tuples_file)[0], 'high_latency_ns.txt')
    write_tuple_array(output_tuples, high_latency_ns_file, True)
    
    
    group_by_index = 1 # lns
    value_index = 4 # latency
    filter = high_write_latency_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)
    
    from write_array_to_file import write_tuple_array
    high_latency_lns_file = os.path.join(os.path.split(tuples_file)[0], 'high_write_latency_lns.txt')
    write_tuple_array(output_tuples, high_latency_lns_file, True)
    
    group_by_index = 2 # ns
    value_index = 4 # latency
    filter = high_write_latency_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)

    from write_array_to_file import write_tuple_array
    high_latency_ns_file = os.path.join(os.path.split(tuples_file)[0], 'high_write_latency_ns.txt')
    write_tuple_array(output_tuples, high_latency_ns_file, True)
    



def count_successful_requests_by_nodes(tuples_file):
    from output_by import read_filter, write_filter
    group_by_index = 1 # lns
    value_index = 4 # latency
    filter = read_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)
    
    from write_array_to_file import write_tuple_array
    latency_lns_file = os.path.join(os.path.split(tuples_file)[0], 'latency_lns.txt')
    write_tuple_array(output_tuples, latency_lns_file, True)
    
    group_by_index = 2 # ns
    value_index = 4 # latency
    filter = read_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)

    from write_array_to_file import write_tuple_array
    latency_ns_file = os.path.join(os.path.split(tuples_file)[0], 'latency_ns.txt')
    write_tuple_array(output_tuples, latency_ns_file, True)
    
    
    group_by_index = 1 # lns
    value_index = 4 # latency
    filter = write_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)
    
    from write_array_to_file import write_tuple_array
    latency_lns_file = os.path.join(os.path.split(tuples_file)[0], 'write_latency_lns.txt')
    write_tuple_array(output_tuples, latency_lns_file, True)
    
    group_by_index = 2 # ns
    value_index = 4 # latency
    filter = write_filter
    output_tuples = group_by(tuples_file, group_by_index, value_index, filter)

    from write_array_to_file import write_tuple_array
    latency_ns_file = os.path.join(os.path.split(tuples_file)[0], 'write_latency_ns.txt')
    write_tuple_array(output_tuples, latency_ns_file, True)
    

def high_read_latency_filter(tokens):
    if tokens is None or len(tokens) < 8:
        return False
    if tokens[5] == 'r' and float(tokens[4]) > 5000:
        return True
    return False

def high_write_latency_filter(tokens):
    if tokens is None or len(tokens) < 8:
        return False
    if tokens[5] == 'w' and float(tokens[4]) > 5000:
        return True
    return False

if __name__ == "__main__":
    main()
