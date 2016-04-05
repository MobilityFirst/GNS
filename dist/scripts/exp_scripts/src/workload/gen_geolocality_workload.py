#!/usr/bin/env python2.7

import os
import sys
import random
from operator import itemgetter
from math import radians, cos, sin, asin, sqrt

import write_workload

__author__ = 'abhigyan'


def main():
    trace_folder = '../resources/workload'    # folder where output will be generated
    lns_geo_file = '../resources/pl_lns_geo_ec2'   # files with hostname, and lat-long of local name servers (lns)
    gen_geolocality_trace(trace_folder, lns_geo_file)


def gen_geolocality_trace(trace_folder, lns_geo_file, number_names=10000, first_name=0, num_lookups=100000,
                          num_updates=100000, locality_percent=0.75, locality_parameter=3, num_lns=-1, exp_duration=300,
                          append_to_file=False, lns_ids=None, name_prefix=None, seed=12345):
    """
    Generates lookups (aka reads) and updates for all local name servers in lns_geo_file. Lookup traces and update
    traces are respectively stored in sub-directories 'lookupTrace' and  'updateTrace' of trace_folder.
    This also generates meta-data regarding traces in 'otherData' folder.

    # Workload generation parameters
    number_names = 10000   # number of names in workload.
    first_name = 0    # workload will have names in range (first_name, first_name + number_names)

    num_lookups = 100000  # number of lookups
    number_updates = 100000  # number of updates

    locality_percent = 0.75  # fraction of lookups that have geo-locality of requests
    locality_parameter = 3   # determines number of locations that will have 'locality_percent' requests, other requests
                             # are randomly distributed among local name servers

    # NOTE: all update requests for a name will be from a randomly selected local name server

    num_lns = -1             # set this to -1 if you want to generate trace for all LNS in lns geo file.
                             # otherwise trace will be generated for first 'num_lns' in lns geo file

    append = True            # append new requests to end of trace files

    exp_duration = 3000      # this parameter can be ignored

    lns_ids = None           # list of IDs of local name servers in the order of their names in LNS geo file.
                             # if lns_ids is not None, trace file for that LNS is name the same as is ID.

    name_prefix = None       # if name-prefix is not None, append given prefix to all names.
    """

    lookup_trace_folder = os.path.join(trace_folder, 'lookupTrace')
    update_trace_folder = os.path.join(trace_folder, 'updateTrace')
    other_data_folder = os.path.join(trace_folder, 'otherData')

    # tmp folder to write lookups and updates, which we will include in trace_folder now
    tmp_trace_folder = '/tmp/trace/'
    lookup_trace_folder = os.path.join(tmp_trace_folder, 'lookupTrace')
    update_trace_folder = os.path.join(tmp_trace_folder, 'updateTrace')
    other_data_folder = os.path.join(tmp_trace_folder, 'otherData')

    os.system('mkdir -p ' + lookup_trace_folder + ' ' + update_trace_folder)
    os.system('rm  ' + lookup_trace_folder + '/update_* ' + update_trace_folder + '/lookup_* 2> /dev/null')

    lns_list, lns_dist, lns_nbrs_ordered_bydist = read_lns_geo_file(lns_geo_file, num_lns)

    min_update = int(num_updates*1.0/number_names*0.5)
    max_update = int(num_updates*1.0/number_names*1.5)
    if num_updates > 0 and max_update == 0:
        print 'Script error: max updates per mobile name = 0'
        sys.exit(2)

    min_query = int(num_lookups*1.0/number_names*0.5)
    max_query = int(num_lookups*1.0/number_names*1.5)

    if locality_parameter == 0:
        print 'Locality parameter = 0. Locality parameter must be positive'
        sys.exit(2)
    if locality_parameter > len(lns_list):
        locality_parameter = len(lns_list)

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

    fw_name_lns_lookup = open(os.path.join(other_data_folder, 'name_lns_lookup'), 'w')
    if seed is not None:
        random.seed(seed)
    print 'Generating workload ...'
    for i in range(number_names):

        mobile_name = str(first_name + i)
        mobile_name_updates = random.randint(min_update, max_update)
        mobile_name_lookups = random.randint(min_query, max_query)

        fw_readwrite.write(str(mobile_name_updates*1.0/exp_duration))
        fw_readwrite.write(' ')
        fw_readwrite.write(str(mobile_name_lookups*1.0/exp_duration))
        fw_readwrite.write('\n')

        # total_lookups += mobile_name_lookups
        # total_updates += mobile_name_updates

        x = random.randint(0, len(lns_list) - 1)
        if mobile_name_updates > 0:
            write_to_file2(fw_update[x], mobile_name, mobile_name_updates)

        if mobile_name_lookups <= 0:
            continue

        q_local = max(1, int(locality_percent*mobile_name_lookups/locality_parameter))   # lookups per node
        locality_lookups = int(locality_percent*mobile_name_lookups)
        q_count = 0
        write_to_file(fw_lookup[x], fw_name_lns_lookup, x, mobile_name, q_local)

        locality_lookups -= q_local
        q_count += q_local
        # q_local = min(mobile_name_lookups - q_count, q_local)
        for j in range(locality_parameter - 1):   # code correct only for python2.*, not python 3.*
            if locality_lookups == 0:
                break
            y = lns_nbrs_ordered_bydist[lns_list[x]][j]
            write_to_file(fw_lookup[y], fw_name_lns_lookup, y, mobile_name, q_local)
            locality_lookups -= q_local
            q_count += q_local

        # newer code for workload in paper
        while q_count < mobile_name_lookups:  # > 0
            x = random.randint(0, len(lns_list) - 1)   # randint behaves differently in python 3
            write_to_file(fw_lookup[x], fw_name_lns_lookup, x, mobile_name, 1)
            q_count += 1
        fw_name_lns_lookup.write('\n')

    # close all files
    for fw in fw_lookup:
        fw.close()
    for fw in fw_update:
        fw.close()
    # close read write rates file
    fw_readwrite.close()
    fw_name_lns_lookup.close()

    total_lookups = 0  # tracks the number of lookups actually generated
    total_updates = 0  # tracks the number of updates actually generated

    # merge lookups and updates into a single file.
    for i, lns in enumerate(lns_list):
        lookup_trace_file = os.path.join(lookup_trace_folder, 'lookup_' + lns)
        update_trace_file = os.path.join(update_trace_folder, 'update_' + lns)
        tmp_file = '/tmp/trace/merge_tmp'
        fw = open(tmp_file, 'w')
        req_count = 0
        for line in open(lookup_trace_file).readlines():
            if name_prefix is not None:
                fw.write(name_prefix)
            fw.write(line.strip() + '\t' + write_workload.RequestType.LOOKUP + '\n')
            total_lookups += 1
            req_count += 1
        for line in open(update_trace_file).readlines():
            if name_prefix is not None:
                fw.write(name_prefix)
            fw.write(line.strip() + '\t' + write_workload.RequestType.UPDATE + '\n')
            total_updates += 1
            req_count += 1
        fw.close()
        randomize_trace_file(tmp_file)
        f_name = lns
        if lns_ids is not None:
            f_name = lns_ids[i]
        output_file = os.path.join(trace_folder, f_name)

        req_rate = int(req_count / exp_duration + 1)
        print 'Request rate for ', f_name, ' = ', req_rate

        fw = open(output_file, 'a')
        fw.write(str(req_rate) + '\t' + str(write_workload.RequestType.RATE) + '\n')
        fw.close()
        if not append_to_file:
            os.system('rm ' + output_file)
        os.system('cat ' + tmp_file + ' >> ' + output_file)

    print 'Total lookups', total_lookups
    print 'Total updates', total_updates
    print 'Output trace folder', trace_folder
    # print 'Update trace folder', update_trace_folder
    return total_lookups, total_updates


def write_to_file2(fw, val, count):
    """Writes 'val' to file 'fw', 'count' times (once per line)"""
    while count > 0:
        fw.write(val)
        fw.write('\n')
        count -= 1


def write_to_file(fw, fw_name_lns_lookup, x, val, count):
    if count <= 0:
        return
    fw_name_lns_lookup.write(str(x))
    fw_name_lns_lookup.write(' ')
    fw_name_lns_lookup.write(str(count))
    fw_name_lns_lookup.write(' ')

    while count > 0:  # writing this later because it decreases var 'count'
        fw.write(val)
        fw.write('\n')
        count -= 1


def read_lns_geo_file(filename, num_lns):
    """Returns list of lns, and a dict with pairwise distances between lns"""
    lns = []
    f = open(filename)
    for i, line in enumerate(f):
        # lns.append(line.split()[0].strip())
        lns.append(str(i))

    lns_geo = {}
    f = open(filename)
    for i, line in enumerate(f):
        tokens = line.split()
        lns_geo[str(i)] = [float(tokens[1]), float(tokens[2])]
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

    lns_nbrs_ordered_by_dist = {}
    for lns1 in lns:
        tuples = []
        for k, v in lns_dist[lns1].items():
            if k == lns1:
                continue
            tuples.append([k, v])

        tuples.sort(key=itemgetter(1))
        lns_nbrs_ordered_by_dist[lns1] = [lns.index(t[0]) for t in tuples]
    print 'Number of lns', len(lns)

    return lns, lns_dist, lns_nbrs_ordered_by_dist


def randomize_trace_file(filename):
    """Randomly shuffle lines in file"""
    lines = open(filename).readlines()
    random.shuffle(lines)
    fw = open(filename, 'w')
    for line in lines:
        fw.write(line)
    fw.close()


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


if __name__ == "__main__":
    main()


'''
cqlsh> create keyspace icepice with replication = { 'class' : 'SimpleStrategy' , 'replication_factor' : 2 } ;
cqlsh> use icepice ;
cqlsh:icepice> create table info ( nos int primary key );
cqlsh:icepice> insert into info (nos) values (1) ;
'''
