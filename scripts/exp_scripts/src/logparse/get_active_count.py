#!/usr/bin/env python

import os,sys
from write_array_to_file import  write_tuple_array 
from plot_cdf import get_cdf_and_plot
## this file counts the number of actives when

num_names = 11000
num_ns = 80
regular_max = 999

def main():

    log_file_dir = sys.argv[1]
    output_dir = log_file_dir + '_stats'
    if log_file_dir.endswith('/'):
        output_dir = log_file_dir[:-1] +'_stats'
    replication_round = int(sys.argv[2])
    output_active_counts(log_file_dir, output_dir,replication_round)


def get_active_count_all(log_file_dir, output_dir, replication_round):
    """Get number of  active replicas by parsing logs."""
    if log_file_dir.find('static3') >= 0:
        active_count_all = {}
        for i in range(num_names):
            active_count_all[i] = [3,3]
        return active_count_all
    if log_file_dir.find('replicate_all') >= 0:
        active_count_all = {}
        for i in range(num_names):
            active_count_all[i] = [num_ns, num_ns]
        return active_count_all
    mobile_zero_write_rate = 0
    log_file_prefix = 'gnrs_stat.xml.'
    files = os.listdir(log_file_dir)
    active_count_all = {}
    for f in files:
        if f.startswith('log_ns'):
            folder = os.path.join(log_file_dir, f)
            
            active_count = get_active_count_for_NS(folder, log_file_prefix, replication_round)
            for k,v in active_count.items():
                # Some mobile names have zero write rates because LNS that sent writes failed.
                # So, exclude mobile names with zero write rates.
                if (log_file_dir.find('locality') >= 0 or log_file_dir.find('uniform') >= 0) \
                    and k > regular_max and v[0] > 40 :
                    #print k, v[0]
                    mobile_zero_write_rate += 1
                    continue
                # since 3 primaries are always replicas, add 3.
                v[0] = min(v[0] + 3, num_ns)
                active_count_all[k] = v
    
    print 'Mobile zero write rate count: ', mobile_zero_write_rate
    return active_count_all

def output_active_counts(log_file_dir, output_dir, replication_round):
    active_count_all = get_active_count_all(log_file_dir, output_dir, replication_round)
    output_file = os.path.join(output_dir, 'active_counts' + \
                               str(replication_round) + '.txt')
    output_tuples = []
    for k in sorted(active_count_all.keys()):
        v = active_count_all[k]
        
        #print 'Name:', str(k), '\tActives: ',v[0], '\tNumberReplica', v[1]
        output_tuples.append([k, int(v[0]), v[1]])
    write_tuple_array(output_tuples, output_file, p = True)
    
    # plot CDF
    #filename = os.path.join(output_dir, 'active_counts_cdf' + \
    #                            str(replication_round) + '.txt')
    #get_cdf_and_plot([filename],['ACTIVE-CDF'], col_nos, pdf_filename, output_dir, template_file)
    
    namecount = len(active_count_all.keys())
    replicacount = get_replica_count(active_count_all)
    avg_replica = 0
    if namecount > 0:
        avg_replica = replicacount*1.0/namecount
    output_tuples = []    
    output_tuples.append(['Number-names', namecount])
    output_tuples.append(['Number-actives', replicacount])
    output_tuples.append(['Average-actives', avg_replica])
    output_file = os.path.join(output_dir, 'avg_replica_count' + \
                           str(replication_round) + '.txt')
    write_tuple_array(output_tuples, output_file, p = True)
    print 'Number of Names:', namecount
    print 'Number of Active Replicas:', replicacount
    print 'Average Number of Active Replicas:', avg_replica
    
    # CDF of active replicas
    output_file = os.path.join(output_dir, 'active_counts' + str(replication_round) + '.txt')
    os.system('wc -l ' + output_file)
    pdf_filename = os.path.join(output_dir, 'replica_cdf' + str(replication_round) + '.pdf')
    template_file  = '/home/abhigyan/gnrs/gpt_files/template2.gpt'
    get_cdf_and_plot([output_file], ['Average-replica'], [1], pdf_filename, output_dir, template_file)
    
def get_replica_count(active_count_all):
    count = 0
    for k,v in active_count_all.items():
        #if v[0] > 1:
        #    print 'MORE THAN 1 replica', k, v
        count += v[0]
    return count


def get_active_count_for_NS(folder,log_file_prefix, replication_round):
    """returns a dict k = name, v = [number of actives, number of replicas] at this nameserver"""
    
    select_log_file = os.path.join(folder, 'replication.xml')
    
    #if not os.path.exists(select_log_file):
    output_filtered_log_file(folder, select_log_file, log_file_prefix)
    if not os.path.exists(select_log_file):
        return {}
    active_count = {}
    
    f = open(select_log_file)
    for line in f:
        line = line.strip()
        tokens = line.split()
        if tokens[1] == '<message>ReplicationAnalysisThread:' and tokens[2] == ('Round:' + str(replication_round)):
            name = -1
            replica = -1
            for token in tokens:
                try:
                    if token.startswith('Name:'):
                        name = int(token[len('Name:'):])
                    if token.startswith('NumberReplica:'):
                        replica = int(token[len('NumberReplica:'):])
                except:
                    continue
            if name != -1 and replica != -1:
                i = line.index('NewReplica:') 
                token = line[i:]
                token = token[len('NewReplica:') + 1: - len('</message>') - 1].strip()
                if token == '':
                    number_actives = 0
                else:
                    number_actives = len(token.split(','))
                active_count[name] = [number_actives, replica]
                #print line
                    
    return active_count

def output_filtered_log_file(folder, select_log_file, log_file_prefix):
    if os.path.exists(folder + '/log'):
        print folder
        folder = folder + '/log'
    os.system('touch ' + select_log_file)
    for i in range(100):
        log_file = os.path.join(folder, log_file_prefix + str(i))
        if os.path.exists(log_file + '.gz'):
            os.system('gzip -d ' + log_file + '.gz')
        elif not os.path.exists(log_file):
            break
        #print 'LOG FILE:', log_file
        cmd = 'grep $"<message>ReplicationAnalysisThread: Round:" ' + folder + '/gnrs_stat.xml.* > ' + select_log_file
        #print cmd
        os.system(cmd)
        #os.system('wc -l ' + select_log_file)

if __name__ == "__main__":
    main()
