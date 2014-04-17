
""" This module fills the latency field in a node config file, specifying the latency between owner node of the
 config file, and any other nodes in GNS. This latency value is used to emulate latency between nodes when running tests
 with GNS. The input to this module is a file containing a common node config file for all nodes, and output is the
 a set of node config files, one per node, in which the latency field is updated as per that node's location.
"""
import os

__author__ = 'abhigyan'


def default_latency_calculator(node_config_file, output_folder, default_latency=100, filename_id=True):
    """Specifies a default latency between all nodes in system. If filename_id is True, files are named as
    'config_<id>', else they are named as 'config_<hostname>'.
    """
    f = open(node_config_file)
    os.system('mkdir -p ' + output_folder)
    for line in f:

        tokens = line.split()
        name = tokens[2]
        id = tokens[0]
        if filename_id:
            fname = id
        else:
            fname = name
        fw = open(os.path.join(output_folder, 'config_' + fname), 'w')
        f1 = open(node_config_file)
        for line1 in f1:
            tokens = line1.split()
            tokens[4] = str(default_latency)
            fw.write('\t'.join(tokens) + '\n')
        fw.close()



def random_geo_latency_calculator(node_config_file, output_folder):
    """
    Assumes a random geo-graphic distribution of nodes across globe, and calculates latencies proportional
    to geographic distance between nodes.
    """
    pass


def geo_latency_calculator(node_config_folder, ns_geo_file):
    """
    This class takes as input the lat-long of nodes in system and and calculates latencies proportional
    to actual geographic distance between nodes.
    """
    pass

