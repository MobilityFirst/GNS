#!/usr/bin/env python2.7

import os
import sys
import random
from operator import itemgetter
from math import radians, cos, sin, asin, sqrt

__author__ = 'abhigyan'

# Workload generation parameters

number_mobiles = 10000   # number of names in workload.
first_mobile_name = 0    # workload will have names in range (first_mobile_name, first_mobile_name + number_mobiles)

number_lookups = 100000  # number of lookups
number_updates = 100000  # number of updates

locality_percent = 0.75  # fraction of lookups that have geo-locality of requests
locality_parameter = 3   # determines number of locations that will have 'locality_percent' requests, other requests
                         # are randomly distributed among local name servers
# NOTE: all update requests for a name will be from a randomly selected local name server

# the parameters below are not needed for workload generation. can be ignored.
exp_duration = 300
num_lns = -1  # set this to -1 if you want to generate trace for all LNS in lns geo file.
              # otherwise trace will be generated for first 'num_lns' in lns geo file


def main():

    trace_folder = '../resources/workload'    # folder where output will be generated
    lns_geo_file = '../resources/pl_lns_geo_ec2'   # files with hostname, and lat-long of local name servers (lns)
    gen_mobile_trace(trace_folder, lns_geo_file)


def gen_mobile_trace(trace_folder, lns_geo_file):
    """
    Generates lookups (aka reads) and updates for all local name servers in lns_geo_file. Lookup traces and update
    traces are respectively stored in sub-directories 'lookupTrace' and  'updateTrace' of trace_folder.
    This also generates meta-data regarding traces in 'otherData' folder.
    """
    global locality_parameter

    lookup_trace_folder = os.path.join(trace_folder, 'lookupTrace')
    update_trace_folder = os.path.join(trace_folder, 'updateTrace')
    other_data_folder = os.path.join(trace_folder, 'otherData')

    #global number_lookups, number_updates

    os.system('mkdir -p ' + lookup_trace_folder + ' ' + update_trace_folder)
    os.system('rm  ' + lookup_trace_folder + '/update_* ' + update_trace_folder + '/lookup_* 2> /dev/null')

    lns_list, lns_dist, lns_nbrs_ordered_bydist = read_lns_geo_file(lns_geo_file, num_lns)

    min_update = int(number_updates*1.0/number_mobiles*0.5)
    max_update = int(number_updates*1.0/number_mobiles*1.5)
    if number_updates > 0 and max_update == 0:
        print 'Script error: max updates per mobile name = 0'
        sys.exit(2)

    min_query = int(number_lookups*1.0/number_mobiles*0.5)
    max_query = int(number_lookups*1.0/number_mobiles*1.5)

    if locality_parameter == 0:
        print 'Locality parameter = 0. Locality parameter must be positive'
        sys.exit(2)
    if locality_parameter > len(lns_list):
        locality_parameter = len(lns_list)

    total_lookups = 0  # tracks the number of lookups actually generated
    total_updates = 0  # tracks the number of updates actually generated

    fw_lookup = []  # file writer for lookup traces of each local name server
    fw_update = []  # file writer for update traces of each local name server
    for i, lns in enumerate(lns_list):
        lookup_trace_file = os.path.join(lookup_trace_folder, 'lookup_' + lns)
        update_trace_file = os.path.join(update_trace_folder, 'update_' + lns)
        fw_lookup.append(open(lookup_trace_file, 'w'))
        fw_update.append(open(update_trace_file, 'w'))

    if not os.path.exists(other_data_folder):
        os.system('mkdir -p ' + other_data_folder)
    fw_readwrite = open(os.path.join(other_data_folder, 'read_write_rate'), 'w')

    fw_namelnslookup = open(os.path.join(other_data_folder, 'name_lns_lookup'), 'w')

    print 'Generating workload ...'
    for i in range(number_mobiles):

        mobile_name = str(first_mobile_name + i)
        mobile_name_updates = random.randint(min_update, max_update)
        mobile_name_lookups = random.randint(min_query, max_query)

        fw_readwrite.write(str(mobile_name_updates*1.0/exp_duration))
        fw_readwrite.write(' ')
        fw_readwrite.write(str(mobile_name_lookups*1.0/exp_duration))
        fw_readwrite.write('\n')

        total_lookups += mobile_name_lookups
        total_updates += mobile_name_updates

        x = random.randint(0, len(lns_list) - 1)
        if mobile_name_updates > 0:
            write_to_file2(fw_update[x], mobile_name, mobile_name_updates)

        if mobile_name_lookups <= 0:
            continue

        q = max(1, int(locality_percent*mobile_name_lookups/locality_parameter))   # lookups per node
        locality_lookups = int(locality_percent*mobile_name_lookups)
        qcount = 0
        write_to_file(fw_lookup[x], fw_namelnslookup, x, mobile_name, q)

        locality_lookups -= q
        qcount += q

        for j in range(locality_parameter - 1):   # code correct only for python2.*, not python 3.*
            if locality_lookups == 0:
                break
            y = lns_nbrs_ordered_bydist[lns_list[x]][j]
            write_to_file(fw_lookup[y], fw_namelnslookup, y, mobile_name, q)
            locality_lookups -= q
            qcount += q

        ## newer code for workload in paper
        while qcount < mobile_name_lookups:  # > 0
            x = random.randint(0, len(lns_list) - 1)   # randint behaves differently in python 3
            write_to_file(fw_lookup[x], fw_namelnslookup, x, mobile_name, 1)
            qcount += 1

        fw_namelnslookup.write('\n')

    # close all requests
    for fw in fw_lookup:
        fw.close()
    for fw in fw_update:
        fw.close()
    # close read write rates file
    fw_readwrite.close()
    fw_namelnslookup.close()

    for i, lns in enumerate(lns_list):
        lookup_trace_file = os.path.join(lookup_trace_folder, 'lookup_' + lns)
        update_trace_file = os.path.join(update_trace_folder, 'update_' + lns)
        randomize_trace_file(lookup_trace_file)
        randomize_trace_file(update_trace_file)

    print 'Total lookups', total_lookups
    print 'Total updates', total_updates
    print 'Lookup trace folder', lookup_trace_folder
    print 'Update trace folder', update_trace_folder


def write_to_file2(fw, val, count):
    """
    Writes 'val' to file 'fw', 'count' times (once per line).
    """
    while count > 0:
        fw.write(val)
        fw.write('\n')
        count -= 1


def write_to_file(fw, fw_namelnslookup, x, val, count):
    if count <= 0:
        return
    fw_namelnslookup.write(str(x))
    fw_namelnslookup.write(' ')
    fw_namelnslookup.write(str(count))
    fw_namelnslookup.write(' ')

    while count > 0:  # writing this later because it decreases var 'count'
        fw.write(val)
        fw.write('\n')
        count -= 1


def read_lns_geo_file(filename, num_lns):
    """returns list of lns, and a dict with pairwise distances between lns."""
    lns = []
    f = open(filename)
    for line in f:
        lns.append(line.split()[0].strip())

    lns_geo = {}
    f = open(filename)
    for line in f:
        tokens = line.split()
        lns_geo[tokens[0]] = [float(tokens[1]), float(tokens[2])]
    lns_dist = {}
    if num_lns < 0:
        num_lns = len(lns)
    lns = lns[:num_lns]
    for lns1 in lns:
        lns_dist[lns1] = {}
        for lns2 in lns:
            if lns1 == lns2:
                lns_dist[lns1][lns2] = 0
            else:
                lns_dist[lns1][lns2] = haversine(lns_geo[lns1][0], lns_geo[lns1][1], lns_geo[lns2][0], lns_geo[lns2][1])

    lns_nbrs_ordered_bydist = {}
    for lns1 in lns:
        tuples = []
        for k, v in lns_dist[lns1].items():
            if k == lns1:
                continue
            tuples.append([k, v])

        tuples.sort(key=itemgetter(1))
        lns_nbrs_ordered_bydist[lns1] = [lns.index(t[0]) for t in tuples]
    print 'Number of lns', len(lns)

    return lns, lns_dist, lns_nbrs_ordered_bydist


def randomize_trace_file(filename):
    """
    Randomly shuffle lines in file.
    """
    values = read_col_from_file(filename)
    random.shuffle(values)

    write_array(values, filename, p=False)


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians

    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def write_array(array, output_file, p=False):
    """
    Writes array elements to file, one element per line.
    """
    if os.path.dirname(output_file) != '' and not os.path.exists(os.path.dirname(output_file)):
        os.system('mkdir -p ' + os.path.dirname(output_file))
    fw = open(output_file, 'w')
    for val in array:
        fw.write(str(val)+'\n')
    fw.close()
    if p:
        print "Output File:", output_file


def read_col_from_file(filename):
    """
    Reads a list of values (one per line) from a file.
    """
    f = open(filename)
    array1d = []
    for line in f:
        if len(line.strip()) == 0:
            continue
        tokens = line.strip().split()
        array1d.append(tokens[0])
    return array1d


if __name__ == "__main__":
    main()
