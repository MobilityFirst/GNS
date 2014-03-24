#!/usr/bin/env python
import os
import sys
from operator import itemgetter
from write_array_to_file import write_tuple_array

def main():
    pl_file = sys.argv[1]
    dns_file = sys.argv[2]
    output_file = sys.argv[3]
    compare_latencies_by_hosts(pl_file, dns_file, output_file)


def compare_latencies_by_hosts(pl_file, dns_file, output_file):
    # read median latencies in each case
    pl_values = read_kv_pairs_from_file(pl_file, 0, 3)
    dns_values = read_kv_pairs_from_file(dns_file, 0, 4)
    output_tuples = []
    for k in pl_values.keys():
        tup = [k, pl_values[k]]
        if k in dns_values:
            tup.append(dns_values[k])
        else:
            tup.append(0)
        output_tuples.append(tup)
        print tup
    # sort based on dns_values
    output_tuples.sort(key = itemgetter(2))
        
    write_tuple_array(output_tuples, output_file, p = True)
    


def read_kv_pairs_from_file(filename, key_index, value_index):
    f = open(filename)
    mydict = {}
    for line in f:
        tokens = line.split()
        mydict[tokens[key_index]] = float(tokens[value_index])
    return mydict

if __name__ == "__main__":
    main()
