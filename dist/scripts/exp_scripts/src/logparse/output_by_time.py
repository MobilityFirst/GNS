#!/usr/bin/env python
import os
import sys
from os.path import join
from group_by import group_by
from write_array_to_file import write_tuple_array
from plot_cdf import get_cdf_and_plot
from output_by import read_filter


def main():
    folder = sys.argv[1]
    output_by_time(folder)


def output_by_time(folder, outputfilename='latency_by_time.txt'):
    grouping_index = 6  # start time
    value_index = 4  # value time

    filename = join(folder, 'all_tuples.txt')

    output_tuples = group_by(filename, grouping_index, value_index, filter=read_filter, grouping_function=time_bin)

    output_file = join(folder, outputfilename)
    write_tuple_array(output_tuples, output_file, p=True)


def time_bin(val):
    interval = 30000  # interval of 30s is 1 group
    return int(val) / interval


if __name__ == "__main__":
    main()
