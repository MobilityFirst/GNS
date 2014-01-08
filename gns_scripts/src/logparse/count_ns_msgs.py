#!/usr/bin/env python
import os
import sys
from stats import get_stat_in_tuples

def main():
    folder = sys.argv[1]
    output_folder = folder + '_stats'
    if folder.endswith('/'):
        output_folder = folder[:-1] + '_stats'
    count_ns_msgs(folder, output_folder)

def count_ns_msgs(folder, output_folder):
    if not os.path.exists(folder):
        print 'ERROR: Folder does not exist'
        sys.exit(2)
    
    os.system('gzip -d ' + folder + '/log_ns_*/*gz 2>/dev/null')
    host_log_folders = os.listdir(folder)
    msg_tuples = []
    for host_log in host_log_folders:
        if not host_log.startswith("log_ns_"):
            continue
        
        # log_ns_2 folder
        if os.path.isdir(folder + '/' + host_log + '/log/'):
            log_path = folder + '/' + host_log + '/log/'
        else:
            log_path = folder + '/' + host_log + '/'
        # print log_path
        node_id = -1
        query = -1
        update_sent = -1
        update_recvd = -1
        for i in range(50): # 10 max number of gnrs_stat files
            filename = log_path + '/gnrs_stat.xml.' + str(i)
            if not os.path.exists(filename):
                break
            node_id, query, update_sent, update_recvd = update_mgs_stats(filename)
        msg_tuples.append([node_id, query, update_sent, update_recvd])
    
    query_tuples = [t[1] for t in msg_tuples]
    update_sent_tuples = [t[2] for t in msg_tuples]
    update_recvd_tuples = [t[3] for t in msg_tuples]
    overall_tuples = [(t[1] + t[2] + t[3]) for t in msg_tuples]
            
    stat_tuples = []
    #print 'QUERY', sorted(query_tuples)
    #print 'Update Sent', sorted(update_sent_tuples)
    #print 'Update Recvd', sorted(update_recvd_tuples)
    #print 'Total queries', sum(query_tuples)
    #print 'Total updates sent', sum(update_sent_tuples)
    #print 'Total updates recvd', sum(update_recvd_tuples)
    stat_tuples.append(['Total-queries', sum(query_tuples)])
    stat_tuples.append(['Total-updates-recvd', sum(update_recvd_tuples)])
    stat_tuples.extend(get_stat_in_tuples(update_recvd_tuples, 'update-recvd'))
    
    query_fairness = get_fairness(query_tuples)
    update_sent_fairness = get_fairness(update_sent_tuples)
    update_recvd_fairness = get_fairness(update_recvd_tuples)
    overall_fairness = get_fairness(overall_tuples)
    
    #print 'Query-Fairness', query_fairness
    #print 'Update-Send-Fairness', update_sent_fairness
    #print 'Update-Recvd-Fairness', update_recvd_fairness
    #print 'Overall-Fairness', overall_fairness
    
    stat_tuples.append(['Query-Fairness', query_fairness])
    stat_tuples.append(['Update-Recvd-Fairness', update_recvd_fairness])
    stat_tuples.append(['Overall-Fairness', overall_fairness])
    
    output_file = os.path.join(output_folder, 'ns-fairness.txt')
    from write_array_to_file import write_tuple_array
    write_tuple_array(stat_tuples, output_file, p = True)
    os.system('cat ' + output_file)

    


def get_fairness(values):
    if len(values) == 0:
        return -1
    count = 0
    numerator = 0
    denominator = 0
    for v in values:
        if v < 0:
            continue
        count += 1
        numerator = numerator + v
        denominator = denominator + v*v
    if count == 0:
        return -1
    if denominator == 0:
        return 0
    return 1.0 * numerator * numerator / count / denominator


def update_mgs_stats(filename):
    f = open(filename)
    update_sent = 0
    update_recvd = 0
    query = 0
    node_id = 0
    for line in f:
        if line.strip().startswith('<message>'):
            tokens = line.strip().split('\t')
            if len(tokens) < 4:
                continue
            if tokens[1] == 'NumberOfLNSUpdatesRecvd':
                update_recvd = int(tokens[3])
                node_id = int(tokens[2])
            if tokens[1] == 'NumberOfLookupsRecvd':
                query = int(tokens[3])
                node_id = int(tokens[2])
            if tokens[1] == 'NumberOfUpdatesRecvd':
                update_sent = int(tokens[3])
                node_id = int(tokens[2])
    return node_id, query, update_sent, update_recvd

if __name__ == "__main__":
    main()
