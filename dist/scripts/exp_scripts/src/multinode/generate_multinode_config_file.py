#!/usr/bin/env python
import os
import sys
import random

from read_pl_latencies import read_pl_latencies
import exp_config
from haversine import haversine

pl_lns_workload = exp_config.lns_file

hosts_file_ns = exp_config.ns_file  # '/home/abhigyan/gnrs/ec2_scripts/pl_ns'
hosts_file_lns = exp_config.lns_file  # '/home/abhigyan/gnrs/ec2_scripts/pl_lns'
hosts_file_ns_geo = exp_config.ns_geo_file
hosts_file_lns_geo = exp_config.lns_geo_file

config_dir = exp_config.config_folder
config_file = 'config'

pl_latency_folder = exp_config.pl_latency_folder

lookup_trace = exp_config.lookupTrace
update_trace = exp_config.updateTrace

other_data = exp_config.other_data


def main():
    #ns = exp_config.num_ns
    #lns = exp_config.num_lns
    load = exp_config.load
    generate_multinode_config_file(load)


def generate_multinode_config_file(load):
    print 'start'
    from read_array_from_file import read_col_from_file
    ns_hostnames = read_col_from_file(hosts_file_ns)
    ns = len(ns_hostnames)
    lns_hostnames = read_col_from_file(hosts_file_lns)
    lns = len(lns_hostnames)
    if exp_config.gen_workload == 'locality':
        print 'Generating locality-based workload'
        #sys.exit(2)
        generate_workload(ns, lns, ns_hostnames, lns_hostnames, load)
        print 'Generated'
        #print 'Generate full workload. Nodes = ', lns
    elif exp_config.gen_workload == 'test':
        for i in range(lns):
            generate_test_workload(ns + i, lns_hostnames[ns + i - ns]) # id of lns is 'ns'
        print 'Generated single LNS test workload'
    print 'after workload'

    # generate EC2 config file:
    if exp_config.gen_config == False:
        return
#    global pl_latencies
    compute_latency_between_nodes(hosts_file_ns, hosts_file_lns, hosts_file_ns_geo, hosts_file_lns_geo)
    # config files
    os.system('mkdir -p ' + config_dir+ '; rm -rf ' + config_dir + '/*')
    
    #assert len(ns_hostnames) == ns
    #assert len(lns_hostnames) == lns
    #pl_latencies = read_pl_latencies(pl_latency_folder)
    for i in range(ns + lns):
        if i < ns:
            config_file1 = os.path.join(config_dir, config_file + '_' + ns_hostnames[i])
        else:
            config_file1 = os.path.join(config_dir, config_file + '_' + lns_hostnames[i - ns])
        write_config_file(i, config_file1, ns, lns, hosts_file_ns, hosts_file_lns)
    print 'Written config files. ' + config_dir + ' count = ' + str(ns + lns)
    
    # workload
#    if exp_config.experiment_run_time > 0:
#    else :
#        os.system('rm -rf lookupLocal updateLocal')

pl_latencies = {}


def get_pl_latency(node1, node2):
    return pl_latencies[node1][node2]


def compute_latency_between_nodes(hosts_file_ns, hosts_file_lns, hosts_file_ns_geo, hosts_file_lns_geo):
    pl_ns, pl_ns_geo = read_pl_lns_geo(hosts_file_ns, hosts_file_ns_geo)
    pl_lns, pl_lns_geo = read_pl_lns_geo(hosts_file_lns, hosts_file_lns_geo)
    global pl_latencies
    print pl_ns
    print pl_ns_geo
    print pl_lns
    print pl_lns_geo

    for i in range(len(pl_ns) + len(pl_lns)):
        pl_latencies[i] =  {}
        for j in range(len(pl_ns) + len(pl_lns)):
            pl_latencies[i][j] =  1
    if not os.path.exists(hosts_file_ns_geo):
        return
    for i in range(len(pl_ns)):
        for j in range(len(pl_ns)):

            pl_latencies[j][i] = haversine(pl_ns_geo[j][0], pl_ns_geo[j][1], pl_lns_geo[i][0], pl_lns_geo[i][1])

    for i in range(len(pl_lns)):
        i1 = i + len(pl_ns)
        for j in range(len(pl_ns)):
            pl_latencies[i1][j] = haversine(pl_lns_geo[i][0], pl_ns_geo[i][1], pl_lns_geo[j][0], pl_lns_geo[j][1])


def read_pl_lns_geo(pl_lns, pl_lns_geo):
    """returns list of lns, and a dict with pairwise distances between lns."""
    lns = []
    f = open(pl_lns)
    for line in f:
        lns.append(line.split()[0].strip())

    if not os.path.exists(pl_lns_geo):
        return lns,[]
    lns_geo = []
    f = open(pl_lns_geo)
    for line in f:
        tokens = line.split()
        lns_geo.append([float(tokens[1]), float(tokens[2])])
    return lns, lns_geo


def write_config_file(node_id, config_file, ns, lns, hosts_file_ns, hosts_file_lns):
    from read_array_from_file import read_col_from_file
    
    #hosts = ['compute-0-13']
    
    hosts = read_col_from_file(hosts_file_ns)
    host_count = 0
    port_number = 44001
    port_per_node = 50
    fw = open(config_file, 'w')
    for i in range(ns):
        port_number += port_per_node
        #s = '\t'.join([str(i), 'yes', hosts[host_count], str(port_number), str(random()), '100.0', '100.0'])
        latency = pl_latencies[node_id][i]  # latency from node_id to (i)  #(1 + random.random()) * 10
        #latency = 10.0 #get_pl_latency(node_id, i)
        s = '\t'.join([str(i), 'yes', hosts[host_count], str(port_number), str(latency), '100.0', '100.0'])
        fw.write(s + '\n')
        #print s
        host_count = (host_count + 1) % len(hosts)
    
    hosts = read_col_from_file(hosts_file_lns)            
    host_count = 0
    #port_number = 20000
    for i in range(lns):
        port_number += port_per_node
        latency = pl_latencies[node_id][i + ns] # latency from node_id to (i+ns)  #(1 + random.random()) * 10
        #latency = 10.0 #get_pl_latency(node_id, i + ns)
        s = '\t'.join([str(i + ns), 'no', hosts[host_count], str(port_number), str(latency), '100.0', '100.0'])
        fw.write(s + '\n')
        #print s
        host_count = (host_count + 1) % len(hosts)
    fw.close()


def generate_test_workload(lns_id, hostname):
    from generate_sequence import write_single_name_trace,write_random_name_trace
    name = '0'
    number = exp_config.lookup_count
    filename = os.path.join(lookup_trace, 'lookup_' + hostname)
    if exp_config.regular_workload + exp_config.mobile_workload == 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename,exp_config.regular_workload + exp_config.mobile_workload, number)
    
    number = exp_config.update_count
    filename = os.path.join(update_trace, 'update_' + hostname)
    if exp_config.regular_workload + exp_config.mobile_workload== 1:
        write_single_name_trace(filename, number, name)
    else:
        write_random_name_trace(filename,exp_config.regular_workload + exp_config.mobile_workload, number)


def generate_workload(ns, lns, ns_hostnames, lns_hostnames, load):
    # now workload
    from read_array_from_file import read_col_from_file
    lns_names = read_col_from_file(pl_lns_workload)
    lns_count = 0
    
    os.system('rm -rf ' + lookup_trace + '; mkdir -p ' + lookup_trace)
    os.system('rm -rf ' + update_trace + '; mkdir -p ' + update_trace)
    # generate trace for load = load
    
    lookup_temp = '/home/abhigyan/gnrs/lookupTrace' + str(load)
    update_temp = '/home/abhigyan/gnrs/updateTrace' + str(load)

    from trace_generator import trace_generator
    trace_generator(load, lookup_temp, update_temp, other_data)
    print 'after trace here .....'
    #os.system('/home/abhigyan/gnrs/trace_generator.py ' + str(load))
    # trace generator outputs in following folders
    
    #generate_beehive_trace(load)
    no_updates = False
    for i in range(lns):
        #id = str(i + ns)
        node_id = lns_hostnames[i]
        host = lns_names[lns_count]
        lookup_input = lookup_temp + '/lookup_' + host
        lookup_output = os.path.join(lookup_trace, 'lookup_' + node_id)
        os.system('cp ' + lookup_input + ' ' + lookup_output)
#        if os.path.exists(lookup_temp + '/lookup_' + host):
#            #print 'cp ' + lookup_temp + '/lookup_' + host + ' lookupLocal/' + id
#            output_file = os.path.join(lookup_trace, 'lookup_' + id)
#            os.system('cp ' + lookup_temp + '/lookup_' + host + ' ' + id)
#        else:
#            #print 'rm lookupLocal/' + id + '; touch  lookupLocal/' + id
#            os.system('rm lookupLocal/' + id + '; touch  lookupLocal/' + id)
        update_input = update_temp + '/update_' + host
        update_output = os.path.join(update_trace, 'update_' + node_id)
        os.system('cp ' + update_input + ' ' + update_output)
        
#        if no_updates == False and os.path.exists(update_temp + '/update_' + host):
#            os.system('cp ' + update_temp + '/update_' + host + ' updateLocal/' + id)
#        else :
#            os.system('rm -rf updateLocal/' + id + '; touch  updateLocal/' + id)
        #if os.path.exists('workloadTrace/workload_' + host):
        #    os.system('cp workloadTrace/workload_' + host + ' workloadLocal/' + id)
        #else :
        #    os.system('rm workloadLocal/' + id + '; touch  workloadLocal/' + id)
        lns_count = (lns_count + 1) % len(lns_names)

    # delete folders
    os.system('rm -rf ' + lookup_temp + ' ' + update_temp)
    print 'Lookup trace:', lookup_trace
    print 'Update trace:', update_trace


def generate_beehive_trace(load):
    
    lookup_temp = 'lookupTrace' + str(load)
    update_temp = 'updateTrace' + str(load)
    # make a read list file
    os.system('cat ' + lookup_temp + '/* > read_list.txt' )
    # now write a read rate file
    from count_frequency import output_read_rate
    output_read_rate()
    from beehive_trace_transform import output_transformed_trace
    output_transformed_trace(lookup_temp, 'read_rate')
    output_transformed_trace(update_temp, 'read_rate')
    os.system('rm -rf ' + lookup_temp + ' ' + update_temp)
    os.system('mv ' + lookup_temp + '_beehive ' + lookup_temp)
    os.system('mv ' + update_temp + '_beehive ' + update_temp)


if __name__ == "__main__":
    main()
