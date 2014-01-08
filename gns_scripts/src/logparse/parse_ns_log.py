#!/usr/bin/env python
import os
import sys
from stats import get_stats

#grep NSUpdateTimeline locality_updatelog/log_ns_*/gnrs_stat* > locality_updatelog_stats/update_tuples.txt
#grep connection-event locality_updatelog/log_ns_*/gnrs_stat* > locality_updatelog_stats/connection_events.txt

def main():
    filename = sys.argv[1]
    count_success_failure_by_ns(filename)

    

def count_success_failure_by_ns(filename):
    number_ns = 80
    ns_total = {}
    ns_success = {}
    for i in range(number_ns):
        ns_total[i] = {}
        ns_success[i] = {}

    f = open(filename)
    for line in f:
        tokens = line.split()
        try:
            id = int(tokens[3])
            ns = int(tokens[4])
            client_confirmation = int(tokens[9])
            if client_confirmation != -1:
                update_recved = int(tokens[6])
                latency = client_confirmation - update_recved
                ns_success[ns][id]  = latency
            else:
                ns_total[ns][id]  = 1
        except:
            pass

    tuples = []
    for i in range(number_ns):
        ns_tuple = [i, len(ns_total[i]), len(ns_success[i]), len(ns_total[i]) - len(ns_success[i])]
        latencies = get_stats(ns_success[i].values())
        ns_tuple.extend(latencies)
        tuples.append(ns_tuple)
        
    output_filename = os.path.join((os.path.split(filename)[0]),'ns_update_stats.txt')
    from write_array_to_file import write_tuple_array
    write_tuple_array(tuples, output_filename, p = True)
    


# count average number of updates by name server.

# <message>     NIOSTAT connection-event        53      26      </message>
# <message>     NSUpdateTimeline        22      77      82      43133   43134   43359   43359   3       2       </message>

# ns lns latency  l1 l2 l3
#
# 

# ns1 ns2 connectioncount

if __name__ == "__main__":
    main()
