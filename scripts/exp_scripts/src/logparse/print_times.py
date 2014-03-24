#!/usr/bin/env python
import os
import sys
from write_array_to_file import *

def main():
    folder = sys.argv[1]
    if not os.path.exists(folder):
        print 'Folder does not exist:', folder
        return
    os.system("grep $'Query-' "+ folder + "/log_ns_0/log/gnrs.xml.0 " + folder + "/log_lns_1/* " + folder + "/log_lns_1/log/* | sort -nk 3 > temp1.txt")
    query_diff = []
    min_q = 0
    max_q = 1000
    
    for query_number in range(min_q,max_q):
        diff = get_lns_ns_diff(query_number, folder)
        query_diff.append([query_number, diff])
        #print query_number, '\t', diff
    write_tuple_array(query_diff, os.path.join(folder, 'lns_ns_diff.txt'), p = True)
    
    

def get_lns_ns_diff(query_number,folder):
    
    #os.system("grep $'Query-" + str(query_number) + "\t' "+ folder + "/log_ns_0/log/gnrs.xml.0 "+ folder + "/log_lns_1/* | sort -nk 3 > temp.txt")

    os.system("grep $'\t" + str(query_number) + "\t' temp1.txt | sort -nk 3 > temp.txt")
    filename = 'temp.txt'
    
    diff_time = get_times(filename)
    return diff_time


def get_times(filename):
    times = []
    start = -1
    f = open(filename)
    #msg1 = 'End-of-timer</message>'
    msg = ['Recvd-packet', 'ListenerResponse-end']
    time_msg1 = -1
    #msg = ['Nameserver-request-recvd-time', 'Sent-DNS-response']
    #msg = ['Recvd-packet','After-send-packet']
    #msg = ['Nameserver-request-recvd-time','Name-server-processing-over']
    #msg = Sent-DNS-response
    msg1 = msg[0]
    msg2 = msg[1]
    time_msg2 = -1
    values = ['']
    for line in f:
        tokens = line.split()
        if start == -1:
            start = int(tokens[2])
        diff = int(tokens[2]) - start
        print str(diff) + '\t' + tokens[4], '\t', tokens[2]
        times.append(diff)
        if tokens[4].startswith(msg1):
            time_msg1 = int(tokens[2])
        if tokens[4].startswith(msg2):
            time_msg2 = int(tokens[2])
            #print tokens[4]
    #print time_msg1, msg1, time_msg2, msg2, time_msg2 - time_msg1
    print time_msg1, time_msg2, time_msg2 - time_msg1
    return time_msg2 - time_msg1
    
    
    



if __name__ == "__main__":
    main()
