#!/usr/bin/env python
import os,sys

def count_incomplete_response(folder):
    search_terms = ['RequestProposed','FullResponse']
    
    stats_folder = get_stats_folder(folder)
    os.system('mkdir -p ' + stats_folder)
    values = {}

    for term in search_terms:
        file_output = os.path.join(stats_folder,term)
        os.system('grep ' + term + ' ' + folder + '/*/*/* > ' + file_output)
        from read_array_from_file import read_array_from_file
        values[term] = read_array_from_file(file_output)
    propose_requests = {}
    v1 = values['RequestProposed']
    for t in v1:
        key = get_key(t)
        if key is None:
            continue
        if key in propose_requests:
            print 'Repeat key'
        propose_requests[key] = 1
        
    print 'RequestProposed', len(propose_requests)

    full_responses = {}
    v1 = values['FullResponse']
    for t in v1:
        key = get_key(t)
        if key is None:
            continue
        if key in full_responses:
            print 'Repeat key', key
        full_responses[key] = 1
        
    print 'FullResponse', len(full_responses)
    
    print 'NOT IN FUll RESPONSES', list(set(propose_requests.keys()) - set(full_responses.keys()))
    # exit
    sys.exit(2)

    for k in propose_requests:
        if k not in full_responses:
            print 'NOT IN FULL RESPONSES',k

    for k in full_responses:
        if k not in propose_requests:
            print 'NOT IN PROPOSE REQUESTS',k


def get_stats_folder(output_folder):
    if output_folder[-1] == '/':
        output_folder = output_folder[:-1]
    return output_folder + '_stats'


def get_key(t):
    if len(t) < 5:
        return None
    key = t[2] + '_' + t[3] + '_' + t[4]
    return key
    


if __name__ == "__main__":
    count_incomplete_response(sys.argv[1])
    
        
