#!/usr/bin/env python
import os
import sys
from operator import itemgetter

def main():
    write_rate_file = sys.argv[1]
    active_counts_file = sys.argv[2]
    output_file = sys.argv[3]
    update_bw_by_name(write_rate_file, active_counts_file, output_file)
def update_bw_by_name(write_rate_file, active_counts_file, output_file):
    f = open(write_rate_file)
    write_rates = {}
    for line in f:
        tokens = line.strip().split()
        write_rates[tokens[0]] = float(tokens[1])

    updatecosts = []
    f = open(active_counts_file)
    for line in f:
        tokens = line.split()
        if tokens[0] in write_rates:
            name = int(tokens[0])
            replicas = int(tokens[1])
            # exlcude mobiles with zero write rates
            updatecosts.append(replicas * write_rates[tokens[0]])

    from stats import get_cdf
    cdf = get_cdf(updatecosts)

    from write_array_to_file import write_tuple_array
    write_tuple_array(cdf, output_file, p = True)

if __name__ == "__main__":
    main()
