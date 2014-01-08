#!/usr/bin/env python
import os
import sys
import output_config2
from tabulate_stats import tabulate_stats

output_dir = output_config2.output_dir
scheme_dirs = output_config2.scheme_dirs
schemes = output_config2.schemes
plot_names = output_config2.plot_names
plot_files_dir = output_config2.plot_files_dir

def main():
    if not os.path.exists(output_dir):
        os.system('mkdir -p ' + output_dir)
    output_read_latency_cdf()
#output_write_latency_cdf()
            #    output_number_replicas_cdf()
    output_stats_graphs()

def output_read_latency_cdf():
    
    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        srcfile = scheme_dir + '/readlatencies_cdf.txt'
        dstfile = output_dir + '/' + scheme + 'readlatencies_cdf.txt'
        columnhead = [plot_names[scheme], 'X']
        add_columnhead_and_copy(columnhead, srcfile, dstfile)
    
    srcfile = scheme_dirs['Auspice'] + '/closest_ns_latency.txt'

    dstfile = output_dir + '/Auspiceclosest_ns_latency.txt'
    columnhead = ['Closest-NS-RTT', 'X']
    add_columnhead_and_copy(columnhead, srcfile, dstfile)

    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        srcfile = scheme_dir + '/cdf_5_reads_by_name.txt'
        dstfile = output_dir + '/' + scheme + 'cdf_5_reads_by_name.txt'
        columnhead = [plot_names[scheme], 'X']
        add_columnhead_and_copy(columnhead, srcfile, dstfile)


    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        srcfile = scheme_dir + '/cdf_4_reads_by_name.txt'
        dstfile = output_dir + '/' + scheme + 'cdf_4_reads_by_name.txt'
        columnhead = [plot_names[scheme], 'X']
        add_columnhead_and_copy(columnhead, srcfile, dstfile)


    cmd = 'cp ' + plot_files_dir + '/plot_read.gp '   + output_dir
    print cmd
    os.system(cmd)

    run_gnuplot('plot_read.gp')


def output_stats_graphs():
    scheme_names = [plot_names[scheme] for scheme in schemes]
    
#   output a .gf file for mean
    filename = output_dir + '/mean-latencies.gf'
    fw = open(filename, 'w')
    fw.write('readmean read_median_namesmedian read_median_namesmean\n')
    fw.write(output_dir + '/mean-latencies.txt\n')
    fw.write(' '.join(scheme_names) + '\n')
    fw.write('\n')
    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        fw.write(scheme_dir + '\n')
    fw.close()
    tabulate_stats(filename)
    
    filename = output_dir + '/fairness.gf'
    fw = open(filename, 'w')
    fw.write('Overall-Fairness Update-Recvd-Fairness Query-Fairness\n')
    fw.write(output_dir + '/fairness.txt\n')
    fw.write(' '.join(scheme_names) + '\n')
    fw.write('\n')
    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        fw.write(scheme_dir + '\n')
    fw.close()
    tabulate_stats(filename)

    filename = output_dir + '/success.gf'
    fw = open(filename, 'w')
    fw.write('Read Write Failed-Read\n')
    fw.write(output_dir + '/success.txt\n')
    fw.write(' '.join(scheme_names) + '\n')
    fw.write('\n')
    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        fw.write(scheme_dir + '\n')
    fw.close()
    tabulate_stats(filename)


    filename = output_dir + '/update-bw.gf'
    print filename
    fw = open(filename, 'w')
    #fw.write('update-recvdmedian update-recvdmaximum\n')
    fw.write('update-recvdmedian\n')
    fw.write(output_dir + '/update-bw.txt\n')
    fw.write(' '.join(scheme_names) + '\n')
    fw.write('\n')
    for scheme in schemes:
        scheme_dir = scheme_dirs[scheme]
        fw.write(scheme_dir + '\n')
    fw.close()
    tabulate_stats(filename)
    
    cmd = 'cp ' + plot_files_dir + '/stats.gp ' + output_dir
    print cmd
    os.system(cmd)
    
    run_gnuplot('stats.gp')


def run_gnuplot(gpt_file):
    os.system('cd ' + output_dir + '; gnuplot ' + gpt_file)


def add_columnhead_and_copy(columnhead, srcfile, dstfile):
    print columnhead, srcfile, dstfile
    fw = open(dstfile, 'w')
    fw.write('\t'.join(columnhead) + '\n')
    fw.close()
    os.system('cat ' + srcfile + ' >>' + dstfile)
    


if __name__ == "__main__":
    main()
