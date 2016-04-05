#!/usr/bin/env python

import os
import sys
from stats import get_stats, get_stat_in_tuples, get_cdf
from write_array_to_file import *
from operator import itemgetter
from os.path import join
from output_by_time import output_by_time


# GLOBAL VARIABLES
p = 10
initial_fraction = 0.0              # exclude initial fraction of requests
final_fraction = 1.0                # parse queries up to the final fraction of responses.

success = 0                         # total number of succesful requests
failed = 0                          # total failed requests

read_success = 0
read_failed = 0

write_success = 0
write_failed = 0

lns_cache_hit = 0                   # cache hits at LNS
retrans_count = 0                   # number of retransmitted queries
total_processing_delay = 0          # total processing delay = (latency - ping delay)

FAIL_LATENCY  = -1

latencies = {}
read_latencies = {}
write_latencies = {}
all_tuples = []                     # stats for all read and write requests
ping_latencies = []                 # ping latency to first NS contacted for each read
closest_ns_latencies = []           # for each read, the closest NS to the LNS receiving this read query

closest_ns_latency_dict = {}        # mapping: k = LNS-hostname, v = latency to closest NS
lns_ids = {}                        # mapping: k = LNS-hostname, v = node ID in experiment

log_file_name = 'gnrs_stat.xml'     # suffix of log file storing all statistics
exclude_hosts = []
local = False


exclude_ns = []
exclude_lns = []

exclude_hosts = ['planetlab2.hust.edu.cn',
                 'planetlab1.csee.usf.edu',
                 'planetlab3.bupt.edu.cn',
                 'plab2.cs.ust.hk',
                 'planetlab2.tamu.edu',
                 'planetlab1.cs.otago.ac.nz',
                 'planetlab2.c3sl.ufpr.br',
                 'planetlab1.cs.otago.ac.nz',
                 'plab1.cs.msu.ru']

def main():
    global local 
    log_files_dir = sys.argv[1]
    if not os.path.exists(log_files_dir):
        print 'Folder does not exist:', log_files_dir
        return
    
    if not log_files_dir.startswith('/home/abhigyan/gnrs'):
        log_files_dir = os.path.join('/home/abhigyan/gnrs/', log_files_dir)
    
    if len(sys.argv) >= 3:
        output_files_dir = sys.argv[2]
    else:
        log_files_dir,output_files_dir = get_indir_outdir(log_files_dir)
    os.system('mkdir -p ' + output_files_dir)
    
    local = False
    if len(sys.argv) >= 4 and sys.argv[3] == 'local':
        local = True
    #print log_files_dir
    #print output_files_dir
    #print local
    
    filter1 = None
    if filter1 is not None:
        print '*************USING A FILTER*********'
    parse_log(log_files_dir, output_files_dir, local, filter1)


def myfilter(tokens):
    
    latency, start_time, ping_latency, name, ns1, lns, num_transmissions = \
             parse_line_query_success(tokens)
    if num_transmissions > 1:
        return False
    #if int(name) < 1000:
    #    return False
    return True

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


def parse_log(log_files_dir, output_dir, local, filter = None):
    """main function to read logs and generate output"""
    
    initialize_variables()
    
    extract_data_from_log(log_files_dir, local, filter)

    output_latency_stats(output_dir)
    initialize_variables()
    
    from output_by import output_stats_by_name
    output_stats_by_name(os.path.join(output_dir, 'all_tuples.txt'))
    
    #from count_ns_msgs import count_ns_msgs
    #count_ns_msgs(log_files_dir, output_dir)

    #from get_active_count import output_active_counts
    #replication_round = 4 # by 4th round, number of replicas is stable.
    #output_active_counts(log_files_dir, output_dir, replication_round)

def initialize_variables():
    global latencies, read_latencies, write_latencies, all_tuples,\
    ping_latencies, closest_ns_latencies, closest_ns_latency_dict, lns_ids
    
    latencies = {}
    read_latencies = {}
    write_latencies = {}
    all_tuples = []                     # stats for all read and write requests
    ping_latencies = []                 # ping latency to first NS contacted for each read
    closest_ns_latencies = []           # for each read, the closest NS to the LNS receiving this read query
    closest_ns_latency_dict = {}        # mapping: k = LNS-hostname, v = latency to closest NS
    lns_ids = {}                        # mapping: k = LNS-hostname, v = node ID in experiment
    success = 0                         # total number of succesful requests
    failed = 0                          # total failed requests
    read_success = 0
    read_failed = 0
    write_success = 0
    write_failed = 0
    lns_cache_hit = 0                   # cache hits at LNS
    retrans_count = 0                   # number of retransmitted queries
    #total_processing_delay = 0          # total processing delay = (latency - ping delay)
    


def extract_data_from_log(log_files_dir, local, filter):
    """parses log files in this dir."""
    
    global latencies, read_latencies, write_latencies
    
    os.system('gzip -d '+ log_files_dir + '/log_lns_*/gnrs_stat* 1> /dev/null 2>/dev/null')
    #os.system('gzip -d '+ log_files_dir + '/log_ns_*/gnrs.xml.*')
    
    if local == True:
        host_files = get_all_files_local(log_files_dir)
    else:
        host_files = get_all_files(log_files_dir)
    
    if not local:
        fill_lns_ids(log_files_dir)
    
    fill_closest_ns_latency_table(log_files_dir)
    #print 'Number of lns ids', len(lns_ids)
    #print lns_ids
    # get all, read and write latencies
    latencies, read_latencies, write_latencies = get_all_latencies(host_files, filter)


def fill_lns_ids(log_files_dir):
    """read a pl_config to fill lns_ids mapping"""
    global lns_ids
    files = os.listdir(log_files_dir)
    for f in files:
        if f.startswith('log_lns') and os.path.exists( log_files_dir + '/' + f + '/pl_config'):
            f = open(log_files_dir + '/' + f + '/pl_config')
            lns_ids = {}
            for line in f:
                tokens = line.split()
                if tokens[1] == 'no':
                    lns_ids[tokens[2]] = int(tokens[0])
            return lns_ids



def get_all_files(mydir):
    """Returns a dict such that key = hostname, value = list of log files."""
    host_files = {}
    all_files = os.listdir(mydir)
    for f in all_files:
        if f.startswith('log_lns_') and os.path.isdir(mydir + '/' + f):
            hostname = f[len('log_lns_'):]
            cur_host_files = []
            i = 20
            while i >= 0:
                filename = mydir + '/' + f + '/gnrs_stat.xml.' + str(i)
                if os.path.exists(filename):
                    cur_host_files.append(filename)
                    #print filename
                i = i - 1
#            xml_files = os.listdir(mydir + '/' + f)
#            cur_host_files = []
#            for xml_file in xml_files:
#                if xml_file.startswith(log_file_name) and not xml_file.endswith('.lck'):
#                    cur_host_files.append(mydir + '/' + f + '/' + xml_file)
            host_files[hostname] = cur_host_files
    return host_files

def get_all_files_local(mydir):
    """Returns a dict such that key = hostname, value = list of log files."""
    host_files = {}
    all_files = os.listdir(mydir)
    for f in all_files:
#        log_folder = mydir + '/' + f + '/log'  # just /log/ is the difference in this method
        if f.startswith('log_lns_') and os.path.isdir(mydir + '/' + f):
            #os.system('mv ' + mydir + '/' + f + '/log/* ' + mydir + '/' + f)
            #print mydir + '/' + f
            hostname = f[len('log_lns_'):]
            cur_host_files = []
            i = 20
            while i >= 0:
                filename = mydir + '/' + f + '/log/gnrs_stat.xml.' + str(i)
                if os.path.exists(filename):
                    cur_host_files.append(filename)
                    #print filename
                i = i - 1

#            xml_files = os.listdir(log_folder)
#            cur_host_files = []
#            for xml_file in xml_files:
#                if xml_file.startswith(log_file_name) and not xml_file.endswith('.lck'):
#                    cur_host_files.append(os.path.join(log_folder, xml_file))
#            hostname = f[len('log_lns_'):]
            host_files[hostname] = cur_host_files
    return host_files


def fill_closest_ns_latency_table(log_files_dir):
    """Returns a dict, with key = LNS-hostname, value = latency-to-closest-NS."""
    global closest_ns_latency_dict
    files = os.listdir(log_files_dir)
    closest_ns_latency_dict = {}
    for f in files:
        if f.startswith('log_lns_') and os.path.isdir(os.path.join(log_files_dir, f)):
            hostname = f[len('log_lns_'):]
            config_file = os.path.join(os.path.join(log_files_dir, f), 'pl_config')
            if os.path.exists(config_file):
                least_latency = get_closest_ns_latency_from_config_file(config_file)
                closest_ns_latency_dict[hostname] = least_latency
            else:
                closest_ns_latency_dict[hostname] = -1


def get_closest_ns_latency_from_config_file(config_file):
    """Returns closest ns after parsing pl_config file."""
    least_latency = -1.0
    f = open(config_file)
    for line in f:
        tokens = line.split()
        if tokens[1] == 'yes':
            try:
                latency = float(tokens[4])
                if latency == -1.0: continue
                if least_latency == -1.0 or least_latency > latency:
                    least_latency = latency
            except:
                continue
    return least_latency

def get_all_latencies(host_files, filter):
    """Get latency from all hosts"""
    global success, failed, host_success, host_failed, lns_cache_hit, total_reads, total_writes, all_tuples
    
    latencies = {}
    read_latencies = {}
    write_latencies = {}
    
    for hostname,files in host_files.items():
        if hostname in exclude_hosts:
            print 'EXCLUDING HOST:', hostname
            continue
        
        read_l1, write_l1, host_tuples1, ping_host1, closest_host1 =\
            get_host_latencies(files, hostname, filter)
        
        read_latencies[hostname] = read_l1
        write_latencies[hostname] = write_l1
        latencies[hostname] = []
        latencies[hostname].extend(read_l1)
        latencies[hostname].extend(write_l1)
        ping_latencies.extend(ping_host1)
        closest_ns_latencies.extend(closest_host1)
        all_tuples.extend(host_tuples1)
    
    return latencies, read_latencies, write_latencies

def get_host_latencies(filenames, hostname, filter):
    lookups = {}
    updates = {}
    for filename in filenames:
        #print filename
        f = open(filename)
        for line in f.readlines():
            if line.find('<message>') == -1:
                continue
            tokens = line.strip().split()
            if len(tokens) < 2:
                continue
            if tokens[1] == 'Lookup-Query' or tokens[1] == 'Lookup-Response':
                #print tokens
                update_tuples(lookups, tokens)
            elif tokens[1] == 'Update-Query' or  tokens[1] == 'Update-Response':
                #print tokens
                update_tuples(updates, tokens)
    
    read_l1 = []
    write_l1 = []
    host_tuples1 = []
    ping_host1 = []
    closest_host1 = []
    
    read_sent = len(lookups)
    read_success1 = 0
    for i in sorted(lookups.keys()):
        latency = lookups[i][7]
        if latency >= 0:
            read_success1 += 1
            read_l1.append(latency)
            ping_host1.append(lookups[i][4])
            closest_host1.append(0)
        t1 = get_tuple_for_all_tuples(lookups[i], 1)
        host_tuples1.append(t1)
    read_failed1 = read_sent - read_success1

    write_sent = len(updates)
    write_success1 = 0
    for i in sorted(updates.keys()):
        latency = updates[i][7]
        if latency >= 0:
            write_success1 += 1
            write_l1.append(latency)
        t1 = get_tuple_for_all_tuples(updates[i], 2)
        host_tuples1.append(t1)
    
    write_failed1 = write_sent - write_success1

    print 'Read\t',read_sent, '\tRead-OK\t',read_success1,'\tRead-Fail\t', read_failed1,
    print '\tWrite\t',write_sent, '\tWrite-OK\t',write_success1,'\tWrite-Fail\t', write_failed1, '\tHost\t', hostname

    global success, failed, read_success, read_failed, write_success, write_failed
    success += read_success1  + write_success1
    failed += read_failed1  + write_failed1
    read_success += read_success1
    read_failed += read_failed1
    write_success += write_success1
    write_failed += write_failed1
    
#sys.exit(2)
    return read_l1, write_l1, host_tuples1, ping_host1, closest_host1

def get_tuple_for_all_tuples(tuple1, lookup_or_update):
    #name, lns, ns1, num_transmissions, latency, 'r', start_time, start_time + latency
    tuple2 = []
    tuple2.append(tuple1[1]) # name
    tuple2.append(tuple1[2]) # lns
    tuple2.append(tuple1[3]) # ns
    tuple2.append(1) # num trans
    tuple2.append(tuple1[7]) # latency
    if lookup_or_update == 1 and tuple1[7] >= 0:
        tuple2.append('r')
    elif lookup_or_update == 2 and tuple1[7] >= 0:
        tuple2.append('w')
    elif lookup_or_update == 1 and tuple1[7] == -1:
        tuple2.append('rf')
    elif lookup_or_update == 2 and tuple1[7] == -1:
        tuple2.append('wf')
    tuple2.append(tuple1[5]) # send time
    tuple2.append(tuple1[6]) # response time
    return tuple2

def update_tuples(input_dict, tokens):
    # lns
    # 
    try:
        if tokens[1].endswith('Query'):
            #query_id = int(tokens[2])
            #name = tokens[3]
            #lns = tokens[4]
            #ns = tokens[5]
            #ping = tokens[6]XXXXXX
            #send_time = int(tokens[6])
            if p > 0 and int(tokens[2]) % p != 0:
                return
            
            input_dict[int(tokens[2])] = [int(tokens[2]),tokens[3],tokens[4],tokens[5], 0, int(tokens[6]), -1, -1]
        else :
            #recv_time = int(tokens[7])
            query_id = int(tokens[2])
            if p > 0 and int(tokens[2]) % p != 0:
                return
            if query_id in input_dict:
                input_dict[query_id][6] = int(tokens[6])
                input_dict[query_id][7] = float(input_dict[query_id][6] - input_dict[query_id][5])
    except:
        print 'Exception:', tokens

#    3       8051    1       0       1359376138443


def get_host_latencies1(filenames, hostname, filter):
    # initialize
    read_l1 = []
    write_l1 = []
    host_tuples1 = []
    ping_host1 = []
    closest_host1 = []
    
    for filename in filenames:
        # get tuples from this log file
        read_l, write_l, host_tuples, ping_host, closest_host = \
                get_latencies(filename, hostname, filter)
        # append to main list
        read_l1.extend(read_l)
        write_l1.extend(write_l)
        host_tuples1.extend(host_tuples)
        ping_host1.extend(ping_host)
        closest_host1.extend(closest_host)

    # exclude initial / final queries
    read_l1 = exclude_queries(read_l1)
    write_l1 = exclude_queries(write_l1)
    host_tuples1 = exclude_queries(host_tuples1)
    ping_host1 = exclude_queries(ping_host1)
    closest_host1 = exclude_queries(closest_host1)
    return read_l1, write_l1, host_tuples1, ping_host1, closest_host1

def get_latencies(filename, hostname, filter = None):
    """Get latencies from this file"""
    
    global success, failed, host_success, host_failed, lns_cache_hit, retrans_count,  total_processing_delay
    
    
    ping_latencies = []
    closest_ns_latencies = []

    read_latencies = []
    write_latencies = []
    host_tuples = []
    host_retrans = 0
    f = open(filename)
    if local:
        this_local_name_server = hostname
    else:
        this_local_name_server = lns_ids[hostname]
    closest_ns_latency = 0
    if not local:
        closest_ns_latency = closest_ns_latency_dict[hostname]

    first_start = -1
    for line in f.readlines():
        line = line.strip()
        tokens = line.split('\t')
        if tokens[0] == '<message>Success-LookupRequest':
            try:
                if filter is not None and filter(tokens) == False:
                    continue
                latency, start_time, ping_latency, name, ns1, lns, num_transmissions = \
                    parse_line_query_success(tokens)
                                
                if first_start == -1:
                        first_start = start_time
                start_time -= first_start
                read_latencies.append(latency)
                host_tuples.append([name, lns, ns1, num_transmissions, latency, 'r', start_time, start_time + latency])
                ping_latencies.append(ping_latency)
                closest_ns_latencies.append(closest_ns_latency)
                #if ping_latency!=closest_ns_latency:
                #    print line
                #    total_processing_delay += latency - ping_latency
            except:
                continue
            if num_transmissions > 1:
                retrans_count += 1
                host_retrans += 1
            if num_transmissions == 0:
                lns_cache_hit +=1
            host_success += 1
        elif tokens[0] == '<message>Success-UpdateRequest':
            latency, name, name_server, local_name_server = \
                parse_line_update_success(tokens)
            host_tuples.append([name, local_name_server, name_server, 1, latency, 'w', 0,0])
            write_latencies.append(latency)
            #host_success += 1
        elif tokens[0].startswith('<message>Failed'):
            if tokens[0].startswith('<message>Failed-NoResponseReceived'):
                try:
                    ns = int(tokens[7][1:-len('</message>') -1].split(',')[0])
                    host_tuples.append([tokens[3], this_local_name_server, ns, tokens[4], -1.0, 'rf', 0, 0])
                except:
                    print 'EXCEPTION: in parsing failed msg:', line
            host_failed += 1
    
    success += host_success
    failed += host_failed
    if host_retrans > 0:
        retrans_percent = host_retrans  / host_success
    
    print '\tRetransmissions', host_retrans,
    return read_latencies, write_latencies, host_tuples, ping_latencies, closest_ns_latencies


def parse_line_query_success(tokens):
    """Parses line which logs stats for a successful read/query request."""
    latency = float(tokens[4])
    start_time = float(tokens[11])
    #ping_latency = float(tokens[5])
    
    name = tokens[3]
    ns_queried = tokens[13]
    ping_latency = float(tokens[14][: - len('</message>')].split('|')[-1])
    ns1 = int(ns_queried.split('|')[-1])
    lns = int(tokens[10])
    num_transmissions = int(tokens[8])
    return latency, start_time, ping_latency, name, ns1, lns, num_transmissions


def parse_line_update_success(tokens):
    """Parses line which logs stats for a successful write/update request."""
    latency = float(tokens[2])
    name = tokens[1]
    name_server = int(tokens[4])
    local_name_server = int(tokens[5])
    return latency, name, name_server, local_name_server


def exclude_queries(x):
    """Return a list after excluding 'initial_fraction' and 'final_fraction' of elements from given list 'x'."""
    if x is None:
        return None
    y = int(initial_fraction * len(x))
    z = int(final_fraction * len(x)) + 1
    return x[y: z]







def output_latency_stats(output_dir):
    """Output all statistics"""
    # older code: output read, write, overall stats
    output_stats(latencies, os.path.join(output_dir, 'all'))
    output_stats(read_latencies, os.path.join(output_dir, 'read'))
    output_stats(write_latencies, os.path.join(output_dir, 'write'))
    
    # output all tuples in a file, used in group-by-name, group-by-time script.
    write_tuple_array(all_tuples, os.path.join(output_dir,'all_tuples.txt'), p = True)
    
    # output ping latencies
    write_tuple_array(get_cdf(ping_latencies), os.path.join(output_dir,'ping_latency.txt'), p = True)
    
    # output closest NS latencies
    write_tuple_array(get_cdf(closest_ns_latencies), os.path.join(output_dir,'closest_ns_latency.txt'), p = True)
    
    # output mean and median query latencies ove time during the experiment.
    #output_by_time(output_dir, 'latency_by_time.txt')
    
    # output start and end times
    #get_start_end_times(all_tuples, os.path.join(output_dir,'start_end_times.txt'))
    
    # output key stats : mean-latency, median-write-latency-etc.
    latency_tuples = get_latency_stats_tuples(latencies, read_latencies, write_latencies, ping_latencies)
    write_tuple_array(latency_tuples, os.path.join(output_dir,'latency_stats.txt'), p = True)
    os.system('cat ' + os.path.join(output_dir,'latency_stats.txt'))
    
    # experiment summary stats:
    write_tuple_array(get_summary_stats(), output_dir + '/summary.txt', p = True)
    os.system('cat ' + output_dir + '/summary.txt')
    
    # results for this folder.
    plot(output_dir)


def output_stats(latencies, prefix):
    # commented this out. not needed.
#    outfile = prefix + 'latencies.txt'
#    output_latencies(latencies,outfile)
    
    #   output CDF
    mycdf = get_cdf1(latencies)
    outfile = prefix + 'latencies_cdf.txt'
    write_tuple_array(mycdf, outfile, p = True)
    
    #   output all hosts stats
    hostwise_stats = get_hostwise_stats(latencies)
    output_file = prefix + 'latencies_hostwise.txt'
    write_tuple_array(hostwise_stats, output_file, p = True)



def get_cdf1(latencies):
    """Get CDF of all latencies"""
    all_values = []
    for k,values in latencies.items():
        all_values.extend(values)
    all_values.sort()
    number_values = len(all_values)
    p = 1.0
    if number_values > 10000:
        p = 10000.0/number_values
    import random
    cdf_array = []
    for i,v in enumerate(all_values):
        p1 = random.random()
        if p1 > p:
            continue
        cdf_array.append([i*1.0/len(all_values), v])
    return cdf_array



def output_latencies(latencies,outfilename):
    fw  = open(outfilename,'w')
    for k,values in latencies.items():
        for v in values:
            fw.write(k + '\t' + str(v) + '\n')
    fw.close()
    print 'Output File:',outfilename




def get_hostwise_stats(latencies):
    stats_2d_array = []
    for lns,lns_lat in latencies.items():
        lns_stats = get_stats(lns_lat)
        lns_stats.insert(0,lns)
        stats_2d_array.append(lns_stats)
    return stats_2d_array
    

def write_tuple_array1(tuple_array,output_file, p = False):
    fw = open(output_file,'w')
    for t in tuple_array:
        for val in t:
            fw.write(str(val)+'\t')
        fw.write('\n')
    fw.close()
    if p : 
        print "Output File:",output_file


def get_latency_stats_tuples(latencies, read_latencies, write_latencies, ping_latencies):
    """returns key stats (mean, median etc) for read, write and (read + write)."""
    kv_tuples = []
    
    kv_tuples.extend(get_stat_in_tuples(get_all_values(latencies), 'all'))
    kv_tuples.extend(get_stat_in_tuples(get_all_values(read_latencies), 'read'))
    kv_tuples.extend(get_stat_in_tuples(get_all_values(write_latencies), 'write'))
    kv_tuples.extend(get_stat_in_tuples(ping_latencies, 'ping'))
    kv_tuples.extend(get_stat_in_tuples(closest_ns_latencies, 'closest_ns'))
    
    avg_processing_delay = 0
    if len(all_tuples) > 0:
        avg_processing_delay = total_processing_delay/len(all_tuples)
    kv_tuples.append(['Avg-processing-delay', avg_processing_delay])
    mean_read_by_ping = 0
    readmean = get_value(kv_tuples, 'readmean')
    pingmean = get_value(kv_tuples, 'pingmean')
    if readmean is not None and pingmean is not None and pingmean != 0:
        mean_read_by_ping = readmean / pingmean
        
    median_read_by_ping = 0
    readmedian = get_value(kv_tuples, 'readmedian')
    pingmedian = get_value(kv_tuples, 'pingmedian')
    if readmedian is not None and pingmedian is not None and pingmedian != 0:
        median_read_by_ping = readmedian / pingmedian
    
    kv_tuples.append(['mean_read_by_ping', mean_read_by_ping])
    kv_tuples.append(['median_read_by_ping', median_read_by_ping])
    return kv_tuples

def get_value(tuples, key):
    for t in tuples:
        if t[0] == key:
            return t[1]
    return None
    
def get_all_values(latencies):
    values = []
    for k,v in latencies.items():
        values.extend(v)
    return values



def get_start_end_times(all_tuples,output_file):
    """Get start end times."""
    my_tuples = []
    for t in all_tuples:
        if t[5] == 'r':
            my_tuples.append([int(t[6]), int(t[7]), int(t[7]) - int(t[6])])
    my_tuples.sort(key = itemgetter(0))
    write_tuple_array(my_tuples, output_file, p = True)


def get_summary_stats():
    
    summary_stats = []
    summary_stats.append(['All', (success+failed)])
    summary_stats.append(['All-Success', (success)])
    summary_stats.append(['All-Fail', (failed)])
    
    summary_stats.append(['Read', read_success + read_failed])
    summary_stats.append(['Read-Succcess', read_success])
    summary_stats.append(['Read-Fail', read_failed])
    
    summary_stats.append(['Write', write_success + write_failed])
    summary_stats.append(['Write-Succcess', write_success])
    summary_stats.append(['Write-Fail', write_failed])
    
    summary_stats.append(['CacheHit', 0])
    summary_stats.append(['Retransmissions', 0])
    
    return summary_stats

def get_summary_stats1():
    
    summary_stats = []
    summary_stats.append(['All', (success+failed)])
    summary_stats.append(['Read', total_reads])
    summary_stats.append(['Write', total_writes])
    #summary_stats.append(['Success', success])
    summary_stats.append(['Failed-Read', get_failed_count()])
    summary_stats.append(['CacheHit', lns_cache_hit])
    summary_stats.append(['Retransmissions', get_retrans_count()])
    
    return summary_stats

def get_retrans_count():
    retrans = 0
    for t in all_tuples:
        if (int(t[3]) > 1 and t[5] == 'r'):
            retrans += 1
    return retrans

def get_failed_count():
    retrans = 0
    for t in all_tuples:
        if  t[5] == 'rf':
            retrans += 1
    return retrans
            
def plot(output_dir):
    os.system('cp /home/abhigyan/gnrs/logparse/planetlab_latency.gp ' + output_dir)
    os.system('cd ' + output_dir + '; gnuplot planetlab_latency.gp')

if __name__ == "__main__":
    main()



def main_old():
    """main function."""
    
    log_files_dir = None
    x = 'no'
    local = False
    
    if len(sys.argv) > 1:
        log_files_dir = sys.argv[1]
        x = 'no'
        if len(sys.argv) > 2 and sys.argv[2] == 'local':
            local = True
    
    if log_files_dir is None or log_files_dir == '':
        print
        x = raw_input('Copy files from log folder? yes/no: ')
        
        print
        log_files_dir = raw_input('Output folder: ')
        
        print
        local_str = raw_input('Local Output? yes/no: ')
        if local_str == 'yes':
            local = True
    
    print
    
    if x == 'yes':
        
        if os.path.exists(log_files_dir):
            override = raw_input('Folder exists: \'' + log_files_dir + '\' Replace contents ? yes/no: ')
            if override != 'yes':
                return
            os.system('rm -rf ' + log_files_dir)
        
        os.system('mkdir -p ' + log_files_dir)
        #os.system('ssh skuld.cs.umass.edu "cd gnrs;tar -czf log.tgz log"')
        #os.system('scp -r skuld.cs.umass.edu:gnrs/log.tgz .')
        #os.system('tar -xzf log.tgz')
        #os.system('rmdir log')
        print 'Unzipping logs ...'
        if local == True:
            os.system('cp -r log_local/* ' + log_files_dir)
        else:
            os.system('cp -r log/* ' + log_files_dir)
            os.system('gzip -d '+ log_files_dir + '/log_lns_*/gnrs_stat*')
            os.system('gzip -d '+ log_files_dir + '/log_ns_*/gnrs.xml.*')
        print 'Done!'
    
    if not log_files_dir.startswith('/home/abhigyan'):
        log_files_dir = '/home/abhigyan/gnrs/' + log_files_dir
    
    if not os.path.exists(log_files_dir):
        print 'Folder does not exist:', log_files_dir
        return



#def read_values(filename):
#    f = open(filename)
#    val = []
#    for line in f:
#        try:
#            val.append(float(line.split()[1]))
#        except:
#            continue
#    return val


#def main():
#    log_files_dir   = sys.argv[1]
#    files = os.listdir(log_files_dir)
#    latencies = {}
#    for f in files:
#        if f.startswith('log_lns'):
#            hostname = f[8:]
#            latency_values = get_latencies(log_files_dir + '/' + f)
#            latencies[hostname] = latency_values
##   output latency, hostname tuples
#    outfile = log_files_dir + '/latencies.txt'
#    output_latencies(latencies,outfile)
##   output CDF
#    mycdf = get_cdf(latencies)
#    outfile = log_files_dir + '/latencies_cdf.txt'
#    write_tuple_array(mycdf, outfile, p = True)
##   output all hosts stats
#    hostwise_stats = get_hostwise_stats(latencies)
#    output_file = log_files_dir + '/latencies_hostwise.txt'
#    write_tuple_array(hostwise_stats, output_file, p = True)
