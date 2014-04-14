#!/usr/bin/env python

import os
import sys
import inspect


script_folder = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))) # script directory
gnuplot_file = os.path.join(script_folder, 'plot_groupchange_latency.gp')

def plot_groupchange_latency(folder):
    search_terms = ['GroupChangeDuration','OldActiveStopDuration']
    latency_index = 4
    stats_folder = get_stats_folder(folder)
    os.system('mkdir -p ' + stats_folder)
    for term in search_terms:
        file_output = os.path.join(stats_folder,term)
        os.system('grep ' + term + ' ' + folder + '/*/*/* > ' + file_output)
        from read_array_from_file import read_col_from_file2
        values = read_col_from_file2(file_output, 4)
        temp = [float(v) for v in values]
        values = temp
        from stats import get_cdf
        cdf_values = get_cdf(values)
        cdf_file = file_output + '_cdf'
        from write_array_to_file import write_tuple_array
        write_tuple_array(cdf_values, cdf_file, p = True)
        os.system('cp ' + cdf_file + ' .')

    try:
        os.system('gnuplot ' + gnuplot_file)
        os.system('mv groupchange_cdf.pdf ' + stats_folder)
    except:
        print 'ERROR: gnuplot error'
    os.system('rm GroupChangeDuration_cdf OldActiveStopDuration_cdf')


def get_stats_folder(output_folder):
    if output_folder[-1] == '/':
        output_folder = output_folder[:-1]
    return output_folder + '_stats'


        

    


if __name__ == '__main__':
    plot_groupchange_latency(sys.argv[1])
