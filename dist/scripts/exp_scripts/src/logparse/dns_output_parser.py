#!/usr/bin/env python
import os
import sys
from group_by import group_by
from write_array_to_file import write_tuple_array
from plot_cdf import get_cdf_and_plot


all_latencies = []


exclude_hosts = []
#exclude_hosts = ['planetlab2.hust.edu.cn', 'planetlab1.cs.otago.ac.nz','planetlab2.c3sl.ufpr.br','plab1.cs.msu.ru']
#exclude_hosts = ['planetlab2.hust.edu.cn',
#                 'planetlab1.csee.usf.edu',
#                 'planetlab3.bupt.edu.cn',
#                 'plab2.cs.ust.hk',
#                 'planetlab2.tamu.edu',
#                 'planetlab1.cs.otago.ac.nz',
#                 'planetlab2.c3sl.ufpr.br',
#                 'planetlab1.cs.otago.ac.nz',
#                 'plab1.cs.msu.ru']

#planetlab2.c3sl.ufpr.br
#planetlab1.cs.otago.ac.nz
#plab1.cs.msu.ru

# Subtracts latency to nearest server
lns_latencies = {}
lns_ping_latencies_file = None  # '/home/abhigyan/gnrs/managedDNS/ultra-dns-lns-pings'


def main():
    log_files_dir = sys.argv[1]
    if not os.path.exists(log_files_dir):
        print 'Folder does not exist:', log_files_dir
        return
    
    if len(sys.argv) >= 3:
        output_files_dir = sys.argv[2]
    else:
        log_files_dir,output_files_dir = get_indir_outdir(log_files_dir)
    os.system('mkdir -p ' + output_files_dir)
    parse_dns_output(log_files_dir, output_files_dir)


def get_indir_outdir(dir):
    """returns default output directory"""
    tokens = dir.split('/')
    dir1 = ''
    dir2 = ''
    if len(tokens[-1]) == 0:
        dir1 = '/'.join(tokens[:-2])
        dir2 = tokens[-2]
    else:
        dir1 = '/'.join(tokens[:-1])
        dir2 = tokens[-1]
    
    prefix1 = dir1+'/'+dir2+'/'
    prefix2 = dir1+'/'+dir2+'_stats/'
    
    return prefix1, prefix2



def read_lns_latencies():
    global lns_latencies
    lns_latencies = {}
    f = open(lns_ping_latencies_file)
    for line in f:
        tokens = line.split()
        lns_latencies[tokens[5]] = float(tokens[4])
        #print tokens[5], tokens[4]


def parse_dns_output(log_files_dir, output_dir, filter=None):
    
    output_extended_tuple_file(log_files_dir, output_dir)
    
    # plot cdf across requests
    tuples_file = os.path.join(output_dir, 'all_tuples.txt')
    
    filenames = [tuples_file]*2
    schemes = ['Ultra-DNS', 'LNS-RTT']
    #latency_index = 5
    #ping_latency_index = 6
    
    # latency index = 4, ping to lns = 5 for this experiment.
    col_nos = [6, 7]
    pdf_file = os.path.join(output_dir, 'cdf_latency.pdf')
    template_file = '/home/abhigyan/gnrs/gpt_files/template1.gpt'
    
    get_cdf_and_plot(filenames, schemes, col_nos, pdf_file, output_dir, template_file)
    
    # plot cdf across names
    value_index = 6
    name_index = 1  # 0 = lns-query-id, 1 = name-id, 2 = name, 3 = ultra-dns-latency,
    outfile1 = os.path.join(output_dir, 'reads_by_name.txt')
    output_tuples1 = group_by(tuples_file, name_index, value_index, filter = None)
    write_tuple_array(output_tuples1, outfile1, p = True)

    value_index = 7
    name_index = 1  # 1 = name,
    outfile2 = os.path.join(output_dir, 'pings_by_name.txt')
    output_tuples2 = group_by(tuples_file, name_index, value_index, filter = None)
    write_tuple_array(output_tuples2, outfile2, p = True)
    
    filenames = [outfile1,outfile2]
    schemes = ['Ultra-DNS', 'LNS-RTT']
    col_nos = [5, 5] # Mean value index = 5
    pdf_file = os.path.join(output_dir, 'read_mean_by_name.pdf')
    template_file = '/home/abhigyan/gnrs/gpt_files/template1.gpt'
    get_cdf_and_plot(filenames, schemes, col_nos, pdf_file, output_dir, template_file)
    
    filenames = [outfile1,outfile2]
    schemes = ['Ultra-DNS', 'LNS-RTT']
    col_nos = [4, 4] # Median value index = 4
    pdf_file = os.path.join(output_dir, 'read_median_by_name.pdf')
    template_file = '/home/abhigyan/gnrs/gpt_files/template1.gpt'
    get_cdf_and_plot(filenames, schemes, col_nos, pdf_file, output_dir, template_file)

    latency_stats = []
    from stats import get_stat_in_tuples
    latency_stats.extend(get_stat_in_tuples(all_latencies, 'read'))
    latency_stats.extend(get_stat_in_tuples(all_latencies, 'read'))
    
    read_median_list = [ t[4] for t in output_tuples1]
    read_mean_list = [ t[5] for t in output_tuples1]
    latency_stats.extend(get_stat_in_tuples(read_median_list, 'read_median_names'))
    latency_stats.extend(get_stat_in_tuples(read_mean_list, 'read_mean_names'))
    
    outputfile = os.path.join(output_dir, 'latency_stats.txt')
    write_tuple_array(latency_stats, outputfile, p = True)
    os.system('cat ' + outputfile)
    
    ## output them hostwise
    value_index = 6
    name_index = 5 # 0 = lns-query-id, 1 = name-id, 2 = name, 3 = ultra-dns-latency,  4 = hostname
    outfile1 = os.path.join(output_dir, 'reads_by_host.txt')
    output_tuples1 = group_by(tuples_file, name_index, value_index, filter = None, numeric = False)
    write_tuple_array(output_tuples1, outfile1, p = True)


def output_extended_tuple_file(log_files_dir, output_dir):
    global all_latencies
    all_latencies = []
    if not os.path.exists(log_files_dir):
        print 'ERROR: Input folder does not exist:', log_files_dir
        sys.exit(2)

    if not os.path.exists(output_dir):
        os.system('mkdir -p ' + output_dir)

    files = os.listdir(log_files_dir)
    all_tuples = []
    for f in files:
        all_tuples.extend(get_extended_tuple(os.path.join(log_files_dir, f)))

    write_tuple_array(all_tuples, os.path.join(output_dir, 'all_tuples.txt'), p=True)
    

def get_extended_tuple(input_file):
    if not os.path.exists(input_file):
        print 'Input file does not exist:', input_file
        return []
    hostname = os.path.split(input_file)[1]
    if hostname in exclude_hosts:
        return []
    
    f = open(input_file)
    global all_latencies
    tuples = []
    for line in f:
            tokens = line.strip().split('\t')
        
#        try:
            latency = float(tokens[3])
            ping_latency_to_lns = float(tokens[4])

            if hostname in lns_latencies:
                ping_latency_to_lns = lns_latencies[hostname]
                if ping_latency_to_lns == -1.0:
                    ping_latency_to_lns = 1.0
            
            #latency = latency - ping_latency_to_lns
            # append latency value to all latencies
            all_latencies.append(latency)
            # append to tokens
            tokens.append(hostname)
            tokens.append(latency)
            tokens.append(ping_latency_to_lns)
#        except:
#            print 'exception:', line, '\tfile: ', input_file
            
#            sys.exit(3)
#            pass
            tuples.append(tokens)
        
#    for t in tuples:
#        t.extend([os.path.split(input_file)[1],
#                  min_latency,
#                  all_latencies[len(all_latencies)/2]])
    return tuples

def get_ping_latency(hostname):
    """Get ping latency to dns server from this host."""
    filename = os.path.join('/home/abhigyan/gnrs/ping_log/', hostname)
    
    if os.path.exists(filename):
        f = open(filename)
        try:
            latency = float(f.readline())
            return latency
        except:
            return -1.0
    else:
        return -1.0

if __name__ == "__main__":
    main()
