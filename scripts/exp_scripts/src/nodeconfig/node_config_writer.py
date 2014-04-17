
""" Writes node config files for each node in GNS, in the following format which is accepted by GNS:

ID yes/no Hostname startingPort ping_latency latitude longitude

This module only fills valid values for the first four fields which are common across all nodes, and generates a single
node config file. The later three fields: ping_latency, latitude, and longitude are filled by a separate module in this
package: node_config_latency_calculator.
"""
import os

__author__ = 'abhigyan'


def emulation_config_writer(num_ns, num_lns, output_file, ns_file=None, lns_file=None, starting_port=35123, id_seed=0):
    """
    Write node config files for a setup where we emulate a distributed GNS by running multiple nodes on a single/multiple
    machines. These config files are used in testing GNS.
    The ns_file (lns_file) is the list of nodes that will be used as name servers (local name servers). If either of
    ns_file or lns_file is None, then it is assumed that config file is for local machine.
    starting_port is a reference port number around which port numbers of different nodes are assigned.
    if id_seed == 0, then node_ids are assumed in the range (0, num_ns + num_lns).
    If id_seed > 0, then it acts as the seed to generate node IDs. Name server IDs are generated using random number
    seed as id_seed, and local name server IDs are generated using seed as (id_seed + 1), while ensuring that name
    server and local name server IDs never conflict.
    """
    pass


def deployment_config_writer(ns_file, lns_file, output_file, starting_port=24132, id_seed=0):
    """
    Writes config files like those that will be used in a deployed GNS system where one node is running on each machine.
    ns_file contains a list of name server host names, lns_files contains a list of local name server host names.
    starting_port is the port number of each node in the system.
    if id_seed == 0, then node_ids are assumed in the range (0, num_ns + num_lns).
    If id_seed > 0, then it acts as the seed to generate node IDs. Name server IDs are generated using random number
    seed as id_seed, and local name server IDs are generated using seed as (id_seed + 1), while ensuring that name
    server and local name server IDs never conflict.
    """
    parent_folder = os.path.split(output_file)[0]
    os.system('mkdir -p ' + parent_folder)
    fw = open(output_file, 'w')
    node_id = 0
    for line in open(ns_file):
        fw.write(str(node_id) + "\t" + "yes" + "\t" + line.strip() + "\t" + str(starting_port) + "\t0\t0\t0\n")
        node_id += 1

    for line in open(lns_file):
        fw.write(str(node_id) + "\t" + "no" + "\t" + line.strip() + "\t" + str(starting_port) + "\t0\t0\t0\n")
        node_id += 1
    fw.close()




