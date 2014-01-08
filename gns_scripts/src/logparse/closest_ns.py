#!/usr/bin/env python

import os
import sys

def main():
    log_files_dir = sys.argv[1]
    fill_closest_ns_latency_table(log_files_dir)
    

def fill_closest_ns_latency_table(log_files_dir):
    """Returns a dict, with key = LNS-hostname, value = latency-to-closest-NS."""
    
    files = os.listdir(log_files_dir)
    closest_ns_latency_dict = {}
    closest_ns_dict = {}
    for f in files:
        if f.startswith('log_lns_') and os.path.isdir(os.path.join(log_files_dir, f)):
            hostname = f[len('log_lns_'):]
            config_file = os.path.join(os.path.join(log_files_dir, f), 'pl_config')
            if os.path.exists(config_file):
                least_latency, closest_ns, second_closest_ns, third_closest_ns = get_closest_ns_latency_from_config_file(config_file)
                closest_ns_latency_dict[hostname] = least_latency
                if closest_ns in closest_ns_dict:
                    closest_ns_dict[closest_ns] = closest_ns_dict[closest_ns] + 1
                else:
                    closest_ns_dict[closest_ns] = 1

                if second_closest_ns in closest_ns_dict:
                    closest_ns_dict[second_closest_ns] = closest_ns_dict[second_closest_ns] + 1
                else:
                    closest_ns_dict[second_closest_ns] = 1

                if third_closest_ns in closest_ns_dict:
                    closest_ns_dict[third_closest_ns] = closest_ns_dict[third_closest_ns] + 1
                else:
                    closest_ns_dict[third_closest_ns] = 1
    
    for k, v in closest_ns_dict.items():
        print k,'\t',v/3.0
    
    return closest_ns_latency_dict

def get_closest_ns_latency_from_config_file(config_file):
    """Returns closest ns after parsing pl_config file."""
    least_latency = -1.0
    f = open(config_file)
    closest_ns = -1
    for line in f:
        tokens = line.split()
        if tokens[1] == 'yes':
            try:
                
                latency = float(tokens[4])
                if latency == -1.0: continue
                if least_latency == -1.0 or least_latency > latency:
                    least_latency = latency
                    closest_ns = tokens[0]
            except:
                continue
    
    second_closest_ns = -1
    second_closest_latency = -1.0

    f = open(config_file)
    for line in f:
        tokens = line.split()
        if tokens[1] == 'yes':
            try:
                
                latency = float(tokens[4])
                if latency == -1.0 or closest_ns == tokens[0]:
                    continue
                if second_closest_latency == -1.0 or second_closest_latency > latency:
                    second_closest_latency = latency
                    second_closest_ns = tokens[0]
            except:
                continue
    #print closest_ns, second_closest_ns
    third_closest = -1
    third_closest_latency = -1.0
    
    f = open(config_file)
    for line in f:
        tokens = line.split()
        if tokens[1] == 'yes':
            try:
                
                latency = float(tokens[4])
                if latency == -1.0 or closest_ns == tokens[0] or second_closest_ns == tokens[0]:
                    continue
                if third_closest_latency == -1.0 or third_closest_latency > latency:
                    third_closest_latency = latency
                    third_closest_ns = tokens[0]
            except:
                continue
    #print closest_ns, second_closest_ns, third_closest_ns

    return least_latency, closest_ns, second_closest_ns, third_closest_ns


if __name__ == "__main__":
    main()
