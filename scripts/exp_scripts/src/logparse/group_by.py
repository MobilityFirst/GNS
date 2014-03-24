#!/usr/bin/env python
import os
import sys
from stats import get_stats
from operator import itemgetter


def group_by(filename, group_by_index, value_index, filter = None, grouping_function = None, numeric = True):
    """Performs group-by operation.
    
        group on group_by index, values are in value_index.
        index = 0, 1, 2 etc. """
    
    f = open(filename)
    group_kv = {}
    exception_count = 0
    for line in f:
        tokens = line.split()
        try:
            if filter is not None and filter(tokens) == False:
                continue
            val = float(tokens[value_index])
            if numeric:
                group_key = float(tokens[group_by_index])
                if grouping_function is not None:
                    group_key = grouping_function(float(tokens[group_by_index]))
            else:
                group_key = tokens[group_by_index]
                if grouping_function is not None:
                    group_key = grouping_function(tokens[group_by_index])
            
            if group_key in group_kv:
                group_kv[group_key].append(val)
            else:
                group_kv[group_key] = [val]
        except:
            exception_count += 1
            continue
    if exception_count > 0:
        print "EXCEPTION COUNT:", exception_count
    
    output_tuples = []

    for k, v in group_kv.items():
        output_tuple = [k]
        output_tuple.append(len(v))
        my_stat = get_stats(v)
        output_tuple.extend(my_stat)
        output_tuples.append(output_tuple)
    output_tuples.sort(key = itemgetter(0))
    return output_tuples



#def filter(tokens):
#    name = sys.argv[1]
#    lns =  sys.argv[2]
#    if (tokens[0] == name and tokens[1] == lns):
#        return True
#    return False
    
if __name__ == "__main__":
    
    #print group_by(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    d = group_by('results/dec5/static3-2/all_tuples.txt', 2, 4)
    #d = group_by(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    for k in d:
        print k[0], k[1]
