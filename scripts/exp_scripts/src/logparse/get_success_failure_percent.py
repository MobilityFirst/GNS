#!/usr/bin/env python
import os
import sys
from os.path import join

def main():
    folder = sys.argv[1]
    if not os.path.isdir(folder):
        print 'Folder does not exist:', folder
        return
    
    output_success_failure_counts(folder)
    write_success_failure_files(folder)


def output_success_failure_counts(folder):
    from output_by import group_and_write_output, read_filter, fail_filter
    filename = join(folder, 'all_tuples.txt')

    output_file = join(folder, 'lns_success_count.txt')
    group_and_write_output(filename, 1, 3, output_file, read_filter)
    output_file = join(folder, 'lns_failure_count.txt')
    group_and_write_output(filename, 1, 3, output_file, fail_filter)
    
    output_file = join(folder, 'ns_success_count.txt')
    group_and_write_output(filename, 2, 3, output_file, read_filter)
    output_file = join(folder, 'ns_failure_count.txt')
    group_and_write_output(filename, 2, 3, output_file, fail_filter)
    
    
    
def write_success_failure_files(folder):

    # LNS
    success_file = join(folder, 'lns_success_count.txt')
    failure_file = join(folder, 'lns_failure_count.txt')
    output_file = join(folder, 'lns_succ_fail.txt')
    get_success_failure_tuple(success_file, failure_file, output_file)

    success_file = join(folder, 'ns_success_count.txt')
    failure_file = join(folder, 'ns_failure_count.txt')
    output_file = join(folder, 'ns_succ_fail.txt')
    get_success_failure_tuple(success_file, failure_file, output_file)


def get_success_failure_tuple(success_file, failure_file, output_file):
    success_dict = get_dict(success_file)
    failure_dict = get_dict(failure_file)

    output_tuples = []

    for k in success_dict.keys():
        success = success_dict[k]
        failure = 0
        if k in failure_dict:
            failure = failure_dict[k]
        if (success + failure) > 0:
            output_tuples.append([k, success, failure, success+failure, failure *1.0 /(success + failure)])
        else:
            output_tuples.append([k, 0,0,0,0])
    for k in failure_dict.keys():
        if k in success_dict:
            continue
        success = 0
        failure = failure_dict[k]
        if failure > 0:
            output_tuples.append([k, success, failure, success+failure, failure *1.0 /(success + failure)])
        else:
            output_tuples.append([k, 0,0,0,0])
    
    from write_array_to_file import write_tuple_array
    write_tuple_array(output_tuples, output_file, p = True)
    return output_tuples
                              

def get_dict(filename):
    f = open(filename)
    my_dict = {}
    for line in f:
        tokens = line.split()
        print line,
        my_dict[int(tokens[0])] = int(tokens[1])
    return my_dict

if __name__ == "__main__":
    main()
