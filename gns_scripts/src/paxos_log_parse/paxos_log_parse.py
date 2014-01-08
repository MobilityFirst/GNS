#!/usr/bin/env python

__author__ = 'abhigyan'

import os
import sys
from copy_paxos_logs_cluster import copy_paxos_logs_cluster
from analyze_missing_req_file import analyze_missing_req_file


remote_folder = '/state/partition1/paxos_log'   # at each cluster node, paxos logs are stored at this node

paxos_log_folder_name = 'paxos_log'     # paxos logs copied from remote_folder to this folder inside the given folder

hosts_file = '/home/abhigyan/gnrs/hosts_ns.txt'     # list of cluster nodes where paxos logs are stored

gns_jar = '/home/abhigyan/gnrs/GNS.jar'  # location of GNS jar file used in analyzing paxos logs

log_analyzer_class = 'edu.umass.cs.gns.paxos.PaxosLogAnalyzer'  # class in GNS jar which analyzes paxos logs

failed_nodes_file_name = 'failed_nodes.txt'     # GNS jar reads list of failed nodes from this file name

missing_req_file_name = 'missing.txt'   # GNS jar writes info about requests with missing accepts/commits later

java_vm_options = '-Xmx6000m'   # java VM options in running GNS jar

compute_node = 'compute-0-13'   # compute node where java jar file will be run


def main():
    output_folder = sys.argv[1]     # use folder where output is stored from exp
    num_nodes = int(sys.argv[2])    # number of name servers
    failed_nodes = sys.argv[3:]     # specify all failed nodes

    paxos_log_parse(output_folder, num_nodes, failed_nodes)


def paxos_log_parse(output_folder, num_nodes, failed_nodes):

    paxos_log_folder = os.path.join(output_folder, paxos_log_folder_name)

    # copy logs from remote folder to paxos_log_folder
    copy_paxos_logs_cluster(hosts_file, remote_folder, paxos_log_folder, num_nodes)

    # run java
    stats_folder = get_stats_folder(output_folder)
    if not os.path.exists(stats_folder):
        os.system('mkdir -p ' + stats_folder)

    missing_request_file = os.path.join(stats_folder, missing_req_file_name)

    failed_nodes_file = os.path.join(stats_folder, failed_nodes_file_name)
    write_failed_nodes_file(failed_nodes_file, failed_nodes)
    print '\n'
    cmd = 'ssh ' + compute_node + ' "java ' + java_vm_options + ' -cp ' + gns_jar + ' ' + log_analyzer_class + ' ' +\
          paxos_log_folder + ' ' + missing_request_file + ' ' + str(num_nodes) + ' ' + failed_nodes_file + '"'
    print cmd
    os.system(cmd)
    #sys.exit(2)
    # plot graphs and run other tests
    analyze_missing_req_file(stats_folder, missing_request_file)


def get_stats_folder(output_folder):
    if output_folder.endswith('/'):
        return output_folder[:-1] + '_stats'
    return output_folder + '_stats'


def write_failed_nodes_file(file_name, failed_nodes):
    fw = open(file_name, 'w')
    fw.write(' '.join(failed_nodes))
    fw.close()


if __name__ == '__main__':
    main()