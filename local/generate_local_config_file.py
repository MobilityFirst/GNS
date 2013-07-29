#!/usr/bin/env python
import os
import sys
from read_pl_latencies import read_pl_latencies

def main():
    ns = 80
    lns = 80
    load = 1
    generate_local_config_file(ns, lns, load)

def generate_local_config_file(ns, lns, load):
    global pl_latencies
    hosts_file_ns = 'hosts_ns.txt'
    hosts_file_lns = 'hosts_lns.txt'
    config_dir = 'configLocal'
    config_file = 'config'
    os.system('mkdir -p ' + config_dir+ '; rm -rf ' + config_dir + '/*')
    pl_latencies = read_pl_latencies()
    for i in range(ns + lns):
        config_file1 = os.path.join(config_dir, config_file + '_' + str(i))
        write_config_file(i, config_file1, ns, lns, hosts_file_ns, hosts_file_lns)
    #generate_workloads(ns, lns, load)
    
pl_latencies = {}

def get_pl_latency(node1, node2):
    return pl_latencies[node1][node2]

def write_config_file(node_id, config_file, ns, lns, hosts_file_ns, hosts_file_lns):    
    from read_array_from_file import read_col_from_file
    from random import random    
    #hosts = ['compute-0-13']
    
    hosts = read_col_from_file(hosts_file_ns)
    host_count = 0
    port_number = 31011
    
    fw = open(config_file, 'w')
    for i in range(ns):
        port_number += 100
        #s = '\t'.join([str(i), 'yes', hosts[host_count], str(port_number), str(random()), '100.0', '100.0'])
        latency = get_pl_latency(node_id, i)
        s = '\t'.join([str(i), 'yes', hosts[host_count], str(port_number), str(latency), '100.0', '100.0'])
        fw.write(s + '\n')
        #print s
        host_count = (host_count + 1) % len(hosts)
    
    hosts = read_col_from_file(hosts_file_lns)            
    host_count = 0
    port_number = 20000
    for i in range(lns):
        port_number += 100
        latency = get_pl_latency(node_id, i + ns)
        s = '\t'.join([str(i + ns), 'no', hosts[host_count], str(port_number), str(latency), '100.0', '100.0'])
        fw.write(s + '\n')
        #print s
        host_count = (host_count + 1) % len(hosts)
    fw.close()

def generate_workloads(ns, lns, load):
    # now workloads
    from read_array_from_file import read_col_from_file
    lns_names = read_col_from_file('pl_lns')
    lns_count = 0
    
    os.system('rm -rf lookupLocal updateLocal ; mkdir lookupLocal updateLocal ')
    # generate trace for load = load
    
    #from trace_generator import trace_generator
    #trace_generator(load)
    lookup_folder = 'lookupTrace' + str(load)
    update_folder = 'updateTrace' + str(load)
    os.system('./trace_generator.py ' + str(load))
    
    generate_beehive_trace(load)
    no_updates = False
    for i in range(lns):
        id = str(i + ns)
        host = lns_names[lns_count]
        if os.path.exists(lookup_folder + '/lookup_' + host):
            os.system('cp ' + lookup_folder + '/lookup_' + host + ' lookupLocal/' + id)
        else:
            os.system('rm lookupLocal/' + id + '; touch  lookupLocal/' + id)
        
        if no_updates == False and os.path.exists(update_folder + '/update_' + host):
            os.system('cp ' + update_folder + '/update_' + host + ' updateLocal/' + id)
        else :
            os.system('rm -rf updateLocal/' + id + '; touch  updateLocal/' + id)
        #if os.path.exists('workloadTrace/workload_' + host):
        #    os.system('cp workloadTrace/workload_' + host + ' workloadLocal/' + id)
        #else :
        #    os.system('rm workloadLocal/' + id + '; touch  workloadLocal/' + id)
        lns_count = (lns_count + 1) % len(lns_names)

    # delete folders
    os.system('rm -rf ' + lookup_folder + ' ' + update_folder)

def generate_beehive_trace(load):
    lookup_folder = 'lookupTrace' + str(load)
    update_folder = 'updateTrace' + str(load)
    # make a read list file
    os.system('cat ' + lookup_folder + '/* > read_list.txt' )
    # now write a read rate file
    from count_frequency import output_read_rate
    output_read_rate()
    from beehive_trace_transform import output_transformed_trace
    output_transformed_trace(lookup_folder, 'read_rate')
    output_transformed_trace(update_folder, 'read_rate')
    os.system('rm -rf ' + lookup_folder + ' ' + update_folder)
    os.system('mv ' + lookup_folder + '_beehive ' + lookup_folder)
    os.system('mv ' + update_folder + '_beehive ' + update_folder)
    

if __name__ == "__main__":
    main()
