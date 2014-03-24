#!/usr/bin/env python
import os,sys
from select_columns import extract_column_from_file
from stats import get_cdf
from write_array_to_file import write_tuple_array
from substitute_macro import substitute_macro


def write_cdf(filename, col_no, output_filename):
    values = extract_column_from_file(filename, col_no)
    #print values
    cdf_values = get_cdf(values)
    #print cdf_values
    write_tuple_array(cdf_values, output_filename, p = True)
    #os.system('cat ' + output_filename)
    
                            


if __name__ == '__main__':
    write_cdf(sys.argv[1], int(sys.argv[2]), sys.argv[3])

