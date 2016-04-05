
""" This module fills the latency field in a node config file, specifying the latency between owner node of the
 config file, and any other nodes in GNS. This latency value is used to emulate latency between nodes when running tests
 with GNS. The input to this module is a file containing a common node config file for all nodes, and output is the
 a set of node config files, one per node, in which the latency field is updated as per that node's location.
"""
import os

from math import radians, cos, sin, asin, sqrt

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
        fw = open(os.path.join(output_folder, fname), 'w')
        f1 = open(node_config_file)
        for line1 in f1:
            tokens = line1.split()
            tokens[4] = str(default_latency)
            fw.write('\t'.join(tokens) + '\n')
        fw.close()


def random_geo_latency_calculator(node_config_file, output_folder):
    """Assumes a random geo-graphic distribution of nodes across globe, and calculates latencies proportional
    to geographic distance between nodes"""
    pass


def geo_latency_calculator(output_folder, node_config_file, ns_geo_file, lns_geo_file, filename_id=True):
    """This class takes as input the lat-long of nodes in system and and calculates latencies proportional
    to actual geographic distance between nodes."""

    node_id_geo = get_node_ids_geo_map(node_config_file, ns_geo_file, lns_geo_file)
    print  node_id_geo
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
        fw = open(os.path.join(output_folder, fname), 'w')
        f1 = open(node_config_file)
        for line1 in f1:
            tokens = line1.split()
            dist_km = haversine(node_id_geo[id][0], node_id_geo[id][1], node_id_geo[tokens[0]][0],
                                node_id_geo[tokens[0]][1])
            latency_ms = dist_km / 300000.0 * 1000.0 * 3  # (dist_km)/(speed of light)*(1000 msec/sec)*(inflation)
            tokens[4] = str(int(latency_ms))
            fw.write('\t'.join(tokens) + '\n')
        fw.close()


def get_node_ids_geo_map(node_config_file, ns_geo_file, lns_geo_file):

    ns_geo = open(ns_geo_file).readlines()
    lns_geo = open(lns_geo_file).readlines()
    ns_count = 0
    lns_count = 0
    node_ids_geo = {}
    for line in open(node_config_file):
        tokens = line.split()
        if tokens[1].lower() == 'yes' or tokens[1].lower() == 'true':
            node_ids_geo[tokens[0]] = get_lat_long(ns_geo[ns_count])
            ns_count += 1
        else:
            node_ids_geo[tokens[0]] = get_lat_long(lns_geo[lns_count])
            lns_count += 1

    return node_ids_geo


def get_lat_long(line):
    return [float(line.split()[1]), float(line.split()[2])]


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on the earth (specified in decimal degrees)"""
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km