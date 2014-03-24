__author__ = 'abhigyan'

import os
import sys

lns_avg = []
all_latencies = []
lns_median = []

def main():
    folder = sys.argv[1]
    print 'unzipping output ...'
    os.system('gzip -d ' + folder + "/*/*gz")
    files = os.listdir(folder)
    for f in files:
        full_path = os.path.join(folder, f + '/sample_output')
        parse_output_file(full_path)

    print 'sorting started ...'
    all_latencies.sort()
    print 'sorted'
    fw = open(folder+ '/latencies.txt', 'w')
    for l in all_latencies:
        fw.write(str(l))
        fw.write('\n')
    fw.close()
    fw = open(folder+ '/summary.txt', 'w')
    fw.write('median ' + str(all_latencies[len(all_latencies)/2]))
    fw.write('\n')
    fw.write('avg ' + str(sum(all_latencies)*1.0/len(all_latencies)))
    fw.write('\n')
    fw.close()


def parse_output_file(full_path):
    global lns_median, lns_avg, all_latencies
    latencies = []
    for line in open(full_path):
        tokens = line.split()
        #latency, start_time, ping_latency, name, ns1, lns, num_transmissions = \
        latency = parse_line_query_success(tokens)
        latencies.append(latency)
    latencies.sort()
    if len(latencies) == 0:
        return
    print 'Median', latencies[int(len(latencies)/2)], '\tAvg', sum(latencies)/(len(latencies) + 1)
    lns_median.append(latencies[int(len(latencies)/2)])
    lns_avg.append(sum(latencies)/(len(latencies) + 1))
    all_latencies.extend(latencies)
    print 'number of latencies', len(all_latencies)


def parse_line_query_success(tokens):
    """Parses line which logs stats for a successful read/query request."""

    latency = float(tokens[12])
    #start_time = float(tokens[11])
    ##ping_latency = float(tokens[5])
    #
    #name = tokens[3]
    #ns_queried = tokens[13]
    #try:
    #    ping_latency = float(tokens[5])
    ##ping_latency = float(tokens[14][: - len('</message>')].split('|')[-1])
    #except:
    #    ping_latency = 0
    #try:
    #    ns1 = int(ns_queried.split('|')[-1])
    #except:
    #    ns1 = 0
    #lns = int(tokens[10])
    #num_transmissions = int(tokens[8])
    #num_restarts = int(tokens[12])
    return latency #, start_time, ping_latency, name, ns1, lns, num_transmissions, num_restarts


main()