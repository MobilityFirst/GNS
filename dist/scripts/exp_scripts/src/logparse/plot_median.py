#!/usr/bin/env python
import os,sys
from read_array_from_file import read_array_from_file
#from write_array_from_time import read_array_from_time

def main():
    files = os.listdir(sys.argv[1])
    latencies = []
    for filename in files:
        array_values = read_array_from_file(os.path.join(sys.argv[1],filename))
        values = [float(t[3]) for t in array_values]
        from stats import get_stats_with_names
        stats = get_stats_with_names(values)
        print filename, stats['median'], '\t', stats['minimum']
        latencies.append(stats['median'])
    from stats import get_cdf
    cdf2 = get_cdf(latencies)
    from  write_array_to_file import write_tuple_array
    write_tuple_array(cdf2, 'ultradns')
    #for v  in cdf2:
    #    print v


if __name__ == "__main__":
    main()

                       
