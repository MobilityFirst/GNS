#!/usr/bin/env python
__author__ = 'abhigyan'


import os
import sys


REPLICA_RECEIVE = "URP-RR";

COORDINATOR_PROPOSE = "URP-CP";

COORDINATOR_MAJORITY = "URP-CM";

REPLICA_ACCEPT = "URP-RA";

REPLICA_COMMIT = "URP-RC";

CLIENT_SIDE = "URP-CS";

def main():
    generate_data_for_log_folder(sys.argv[1], sys.argv[2])


def generate_data_for_log_folder(log_folder, ping_file):

    urp_output_file = log_folder + '/URP'
    referenceTS_output_file = log_folder + '/ReferenceTS'
    os.system('grep URP ' + log_folder + '/*/log/* > ' + urp_output_file)
    os.system('grep ReferenceTS ' + log_folder + '/*/log/* > ' + referenceTS_output_file)
    os.system('wc -l ' + referenceTS_output_file)
    os.system('wc -l ' + urp_output_file)
    node_offset = calculate_timestamps(referenceTS_output_file, ping_file)
    client, majority, last = compute_latencies(urp_output_file, node_offset)

    from stats import get_cdf
    client_cdf = get_cdf(client)
    majority_cdf = get_cdf(majority)
    last_cdf = get_cdf(last)
    indir, outdir = get_indir_outdir(log_folder)

    from write_array_to_file import write_tuple_array
    write_tuple_array(client_cdf, os.path.join(outdir, 'client_write_cdf.txt'))
    write_tuple_array(majority_cdf, os.path.join(outdir, 'majority_write_cdf.txt'))
    write_tuple_array(last_cdf, os.path.join(outdir, 'last_write_cdf.txt'))


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


def calculate_timestamps(referenceTS_output_file, ping_file):

    differences = {}
    #for k in pings:
    #    differences[k] = 0

    for line in open (referenceTS_output_file):
        tokens = line.split()
        node = tokens[4]
        diff = int(tokens[7])
        if node not in differences or  differences[node] > diff:
            differences[node] = diff
            print node, diff
    return differences
    # assuming ping = 0

    #pings = read_ping_values(ping_file)
    #print 'Ping values are', pings
    #
    #ts = {}
    #for k in pings:
    #    if k not in differences:
    #        print 'no measurement found for node', k
    #        ts[k] = 0
    #        print k, 0
    #    else:
    #        ts[k] = differences[k]  - pings[k]/2 # divide by 2 because we want to subtract RTT
    #        print k, ts[k]
    #
    #return ts


def read_ping_values(ping_file):
    pings = {}
    for i, line in enumerate(open(ping_file)):
        pings[str(i)] = float(line)
    return pings


def compute_latencies(urp_output_file, node_offset):
    messages_by_write = {}
    f = open(urp_output_file)
    for line in f:
        try:
            tokens = line.split()
            offset = 1
            name = tokens[offset + 1]
            reqID = tokens[offset + 2]
            if int(reqID) < 0:
                continue
            node = tokens[offset + 3]
            msg_type = tokens[offset + 4]
            if msg_type == COORDINATOR_MAJORITY:
                ts = int(tokens[offset + 6])
            else:
                ts = int(tokens[offset + 5])
            if msg_type == COORDINATOR_MAJORITY or msg_type == REPLICA_ACCEPT:
                name = name.split('-')[0]
            #print line,
            #print name, reqID
            primary_key = name + ':' + reqID # using colon (':') as it does not occur in name or reqID
            if primary_key in messages_by_write:
                messages_by_write[primary_key].append(tokens)
            else:
                messages_by_write[primary_key] = [tokens]
        except:
            print 'Exception:', line
            sys.exit(2)
    client_latency = []
    majority_latency = []
    last_latency = []
    fw = open('count.txt', 'w')
    none_count = 0
    for k in messages_by_write:
        client, majority, last = get_write1_write2_latencies(k, messages_by_write[k], node_offset)
        fw.write(k + ' ' + str(len(messages_by_write[k])) + '\n')
        if client is None:
            none_count += 1
        else:
            client_latency.append(client)
            majority_latency.append(majority)
            last_latency.append(last)
            #last_replica_write_latency.append(lr_write)
            #coordinator_majority_latency.append(coor_write)
    fw.close()
    if none_count > 0:
        print 'None count', none_count

    return client_latency, majority_latency, last_latency


def get_write1_write2_latencies(req_key, all_messages, node_offset):

    client_perceived_latency = -1
    start_time = -1
    replica_latencies = []
    num_replicas = -1
    for message in all_messages:
        if message[5] == CLIENT_SIDE:
            client_perceived_latency = int(message[7]) - int(message[6])
            start_time = int(message[6])
        elif message[5] == REPLICA_ACCEPT:
            replica_latencies.append(int(message[6]))
        elif message[5] == REPLICA_COMMIT:
            replica_latencies.append(int(message[6]))
        if message[5] == REPLICA_RECEIVE:
            num_replicas = int(message[7])
    if client_perceived_latency == -1:
        return None, None, None
    print  'Num replicas', num_replicas, client_perceived_latency, len(replica_latencies)
    #if len(replica_latencies) < 3:
    #    return client_perceived_latency, 100000, 100000

    replica_latencies.sort()
    majority_index = int(num_replicas/2)
    majority = 100000
    if len(replica_latencies) > majority_index:
        majority_index = int(num_replicas/2)
        majority = replica_latencies[majority_index] - start_time
    last_write = 100000
    if len(replica_latencies) == num_replicas:
        last_write = replica_latencies[-1] - start_time

    return client_perceived_latency, majority, last_write


#def read_pings(ping_file):
#    pings = {}
#    for line in open(ping_file):
#        tokens = line.split()
#        pings[tokens[3].strip()] = float(tokens[0])
#    return pings
#
#def make_ping_file(ping_file, host_file):
#    pings = read_pings(ping_file)
#    ping_values = []
#    for line in open(host_file):
#        host = line.strip()
#        ping_values.append(pings[host])
#    fw = open('pings.txt', 'w')
#    for val in ping_values:
#        fw.write(str(val) + '\n')
#    fw.close()


if __name__ == "__main__":
    #make_ping_file(sys.argv[1], sys.argv[2])
    main()


