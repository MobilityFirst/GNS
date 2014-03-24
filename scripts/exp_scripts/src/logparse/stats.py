#!/usr/bin/env python

import os
import sys

def get_stats(values):
    values.sort()
    if len(values) > 0:
        min_val = values[0]
        max_val = values[len(values) - 1]
        median_val = values[len(values)//2]
        if len(values)%2 == 0:
            median_val = (values[len(values)//2 - 1] + values[len(values)//2]) / 2
        mean_val  = sum(values) / len(values)
        return [min_val, max_val, median_val, mean_val]
    else:
        return [0,0,0,0]


def get_cdf(values):
    if values is None:
        return None
    if len(values) == 0:
        return []
    number_values = len(values)
    p =1.0
    if number_values > 10000:
        p = 10000.0/number_values
    import random
    
    cdf_array = []
    for i,v in enumerate(sorted(values)):
        p1 = random.random()
        if p1 >p:
            continue
        cdf_array.append([(i + 1)*1.0/len(values), v])
    return cdf_array



mean = 'mean'
median = 'median'
maximum = 'maximum'
minimum = 'minimum'
perc90 = 'perc90'
perc99 = 'perc99'
perc75 = 'perc75'

def get_stats_with_names(values):
    values.sort()
    stat_dict = {}
    
    if len(values) > 0:
        stat_dict[minimum] = values[0]
        stat_dict[maximum] = values[len(values) - 1]
        median_val = values[len(values)//2]
        if len(values)%2 == 0:
            median_val = (values[len(values)//2 - 1] + values[len(values)//2]) / 2
        stat_dict[median] = median_val
        stat_dict[mean] = sum(values) / len(values)
        stat_dict[perc90] = values[len(values)*9//10]
        return stat_dict
    stat_dict['ZEROVALUES'] = 0
    return stat_dict

def get_stat_in_tuples(values, prefix = ''):
    stat_dict = get_stats_with_names(values)
    tuples = []
    for k,v in stat_dict.items():
        tuples.append([prefix + k, v])
    return tuples

                                                        
def print_stats(filename, col):
    from select_columns import  extract_column_from_file
    values = extract_column_from_file(filename, col)
    
    values.sort()
    if len(values) > 0:
        min_val = values[0]
        max_val = values[len(values) - 1]
        median_val = values[len(values)//2]

        if len(values)%2 == 0:
            median_val = (values[len(values)//2 - 1] + values[len(values)//2]) / 2
        mean_val  = sum(values) / len(values)
        perc90 = values[len(values)*9//10]
        return [min_val, max_val, median_val, mean_val, perc90]
    else:
        return [0,0,0,0,0]

if __name__ == "__main__":
    stats = print_stats(sys.argv[1], int(sys.argv[2]))
    for v in stats:
        print v

