#!/usr/bin/env python
import os
import sys
import random
from operator import itemgetter

from haversine import haversine
from write_array_to_file import write_array
from read_array_from_file import read_col_from_file
import exp_config

updateTraceFolder = 'updateTrace'
lookupTraceFolder = 'lookupTrace'

pl_lns = exp_config.pl_lns_workload
pl_lns_geo = exp_config.pl_lns_geo_workload

num_lns = exp_config.num_lns
#pl_lns_geo = '/home/abhigyan/gnrs/configs/validationTrace/pl_lns_geo'
locality_parameter = 10    ## requests will be generated with locality parameter 1 to locality_parameter
first_mobile_name = exp_config.regular_workload
number_mobiles = exp_config.mobile_workload 
number_queries = 200000000  ## approx number of lookups generated for mobile names
number_updates = 200000000  ## approx number of updates generated for mobile names

locality_percent =  0.75

exp_duration = 300

def main():
#    output_folder = sys.argv[1]
#    pl_lns = sys.argv[2]
#    pl_lns_geo = sys.argv[3]
#    locality_parameter = int(sys.argv[4])
#    number_mobiles = int(sys.argv[5])
    load = float(sys.argv[1])
    update_folder = 'updateTraceMobile' + str(load)
    lookup_folder = 'lookupTraceMobile' + str(load)
    
    generate_mobile_trace(load, update_folder, lookup_folder)
    

def generate_mobile_trace(load, lookupTraceFolder, updateTraceFolder):

    global locality_parameter
    global number_queries, number_updates
    number_queries = int(load * number_queries)
    number_updates = int(load * number_updates)
    
    os.system('mkdir -p ' + lookupTraceFolder + ' ' + updateTraceFolder)
    os.system('rm  ' + lookupTraceFolder + '/update_* '  + updateTraceFolder + '/lookup_* 2> /dev/null')
    
    lns_list, lns_dist, lns_nbrs_ordered_bydist = read_pl_lns_geo(pl_lns, pl_lns_geo, num_lns)
    # get a mobile
    # get number of queries, get number of updates.
    # choose update location.
    # choose queries location.
    # Add these to respective arrays.
    # Randomize queries/updates. Write all arrays to files.
    #min_update = int(number_updates*1.0/number_mobiles*0.5)
    #max_update = int(number_updates*1.0/number_mobiles*1.5)

    #min_query = int(number_queries*1.0/number_mobiles*0.5)
    #max_query = int(number_queries*1.0/number_mobiles*1.5)
    
    if locality_parameter == 0:
        print 'Locality parameter = 0. Locality parameter must be positive'
        sys.exit(2)
    if locality_parameter > len(lns_list):
        locality_parameter = len(lns_list)
    
    update_dict = {}
    query_dict = {}
    for lns1 in lns_list:
        update_dict[lns1] = []
        query_dict[lns1] = []
    
    total_queries = 0
    total_updates = 0
    
    #queries = []
    #updates = []
    
    #all_values = []
    import time
    t0 = time.time()
    for i in range(number_mobiles):
        if i%100000 == 0:
            print 'Name', i, 'Time', int(time.time() - t0), 'Queries', total_queries, 'Updates', total_updates
            for lns in lns_list:
                lookupTraceFile = os.path.join(lookupTraceFolder, 'lookup_' + lns)
                updateTraceFile = os.path.join(updateTraceFolder, 'update_' + lns)
                #print lookupTraceFile, updateTraceFile
                output_request_trace_from_request_counts_by_name(lookupTraceFile, query_dict[lns])
                output_request_trace_from_request_counts_by_name(updateTraceFile, update_dict[lns])
                query_dict[lns] = []
                update_dict[lns] = []
        
        mobile_name = first_mobile_name + i
        #random.seed(i + 1)
        #mobile_name_updates = random.randint(min_update, max_update)
        #mobile_name_queries = random.randint(min_query, max_query)
        
        mobile_name_updates = ((random.random() + 0.5) * number_updates*1.0/number_mobiles)
        mobile_name_queries = ((random.random() + 0.5) * number_queries*1.0/number_mobiles)
        frac = mobile_name_updates - int(mobile_name_updates)
        d = random.random()
        if d > frac:
            mobile_name_updates = int(mobile_name_updates)
        else:
            mobile_name_updates= int(mobile_name_updates) + 1
        
        frac = mobile_name_queries - int(mobile_name_queries)
        d = random.random()
        if d > frac:
            mobile_name_queries = int(mobile_name_queries)
        else:
            mobile_name_queries= int(mobile_name_queries) + 1
        
        mobile_name_queries_copy = mobile_name_queries
        # rate = (number of requests)/(exp_duration)
        #mobile_update_rate = (random.random() + 0.5) * number_updates*1.0/number_mobiles / exp_duration
        #mobile_query_rate = (random.random() + 0.5) * number_queries*1.0/number_mobiles / exp_duration
        
        #print i, mobile_name_updates
        #queries.append([mobile_name, mobile_update_rate])
        #updates.append([mobile_name, mobile_query_rate])
        
        total_queries += mobile_name_queries
        total_updates += mobile_name_updates
        
#        print 'Updates', mobile_name_updates
#        print 'Queries', mobile_name_queries
        #x = random.randint(0,len(lns_list)) - 1 # results in uneven distribution of names across local name servers
        
        x = i % len(lns_list) # results in even distribution of names  across local name servers
        
        update_location = lns_list[x]
        if mobile_name_updates > 0:
            update_dict[update_location].append([mobile_name, mobile_name_updates])
        
        if mobile_name_queries <= 0:
            continue
        #locality_name = random.randint(1,locality_parameter) # randint is inclusive
        locality_name = locality_parameter
#        print 'Locality:',locality_name
        #lns_query_count = {}
        queries_per_node = 0
        if locality_name == len(lns_list):
            queries_per_node = mobile_name_queries / locality_name + 1
        else:
            queries_per_node = int(locality_percent*mobile_name_queries/locality_name)
            
        query_dict[update_location].append([mobile_name, queries_per_node])

        #lns_query_count[update_location] = queries_per_node
        
        for i in range(locality_name - 1): # code correct only for python2.*, not python 3.*
            query_dict[lns_nbrs_ordered_bydist[update_location][i]].append([mobile_name, queries_per_node])

            #lns_query_count[lns_nbrs_ordered_bydist[update_location][i]] = queries_per_node
        
        mobile_name_queries -= queries_per_node*locality_name
        
        #if mobile_name_queries <= 0:
        #    continue
#        print 'remaining queries', mobile_name_queries
        ## earlier code
        #locality_name1 = min(5, len(lns_list) - locality_name)
        #queries_per_node = mobile_name_queries / locality_name1 + 1
        #i = locality_name - 1
        #while mobile_name_queries > 0:
        #    query_dict[lns_nbrs_ordered_bydist[update_location][i]].append([mobile_name, queries_per_node])
        #    i += 1
        #    mobile_name_queries -= queries_per_node
        #continue

        ## newer code for workload in paper
        while mobile_name_queries > 0:
            x = random.randint(0,len(lns_list)) - 1   #randint behaves differently in python 3
            lns =  lns_list[x] #x = random.randint(0,len(lns_list)) - 1 # randomly select a lns
            query_dict[lns].append([mobile_name,1])
            mobile_name_queries -= 1
            #if lns in lns_query_count:
            #    lns_query_count[lns] += 1
            #else:
            #    lns_query_count[lns] = 1
        
        # convert lns_query_count to probabilities
        #prob_sum = 0
        #for k in lns_query_count:
        #    lns_query_count[k] = lns_query_count[k]*1.0/mobile_name_queries_copy
        #    prob_sum += lns_query_count[k]*1.0/mobile_name_queries_copy
            
        #values = [mobile_name]
        
        ## append probailites to values
        #for k in lns_query_count:
        #    values.append(lns_list.index(k))
        #    values.append(lns_query_count[k])
        
        #all_values.append(values)
        
    
    #from write_array_to_file import write_tuple_array
    #write_tuple_array(queries, '/home/abhigyan/gnrs/optimalnew/mobile_read_rate_folder/readratefile_mobile_load' + str(int(load)))
    #write_tuple_array(updates, '/home/abhigyan/gnrs/optimalnew/mobile_write_rate_folder/writeratefile_mobile_load' + str(int(load)))
    
    for lns in lns_list:
        lookupTraceFile = os.path.join(lookupTraceFolder, 'lookup_' + lns)
        updateTraceFile = os.path.join(updateTraceFolder, 'update_' + lns)
        #print lookupTraceFile, updateTraceFile
        output_request_trace_from_request_counts_by_name(lookupTraceFile, query_dict[lns])
        output_request_trace_from_request_counts_by_name(updateTraceFile, update_dict[lns])
        
        # shuffle requests
        randomize_trace_file(lookupTraceFile)
        randomize_trace_file(updateTraceFile)

    #from write_array_to_file import write_tuple_array
    #output_file = '/home/abhigyan/gnrs/optimalnew/lns_mobilename_lse_folder/lns_mobilename_lse_load' + str(int(load))
    #write_tuple_array(all_values, output_file, p = False)
    
    print 'Mobile queries', total_queries
    print 'Mobile updates', total_updates
#        print update_dict.values()
#        print query_dict.values()
#        sys.exit()


def output_request_trace_from_request_counts_by_name(filename, request_counts):
    requests = []
    for t in request_counts:
        requests.extend([t[0]] * t[1])
    
    random.shuffle(requests)
    #len(requests)
    fw = open(filename, 'a')
    for val in requests:
            fw.write(str(val))
            fw.write('\n')
    fw.close()
    #write_array(requests, filename, p = False)


def read_pl_lns_geo(pl_lns, pl_lns_geo, num_lns):
    """returns list of lns, and a dict with pairwise distances between lns."""
    lns = []
    f = open(pl_lns)
    for line in f:
        lns.append(line.split()[0].strip())
    
    lns_geo = {}
    f = open(pl_lns_geo)
    for line in f:
        tokens = line.split()
        lns_geo[tokens[0]] = [float(tokens[1]), float(tokens[2])]
    lns_dist = {}
    lns = lns[:num_lns]
    for lns1 in lns:
        lns_dist[lns1] = {}
        for lns2 in lns:
            if lns1 == lns2:
                lns_dist[lns1][lns2] = 0
            else:
                lns_dist[lns1][lns2] = haversine(lns_geo[lns1][0], lns_geo[lns1][1], lns_geo[lns2][0], lns_geo[lns2][1])


    lns_nbrs_ordered_bydist = {}
    for lns1 in lns:
        tuples = []
        for k,v in lns_dist[lns1].items():
            if k == lns1:
                continue
            tuples.append([k,v])
            
        tuples.sort(key = itemgetter(1))
        lns_nbrs_ordered_bydist[lns1] = [t[0] for t in tuples]
    print 'Length lns', len(lns)
    print 'Length lns dist', len(lns_dist)
    print 'Length lns dist ordered', len(lns_nbrs_ordered_bydist)
    for lns1 in lns_nbrs_ordered_bydist:
        print lns1, len(lns_nbrs_ordered_bydist)
    return lns, lns_dist, lns_nbrs_ordered_bydist


def randomize_trace_file(filename):

    values = read_col_from_file(filename)
    random.shuffle(values)

    from write_array_to_file import write_array
    write_array(values, filename, p = False)

if __name__ == "__main__":
    main()
