#!/usr/bin/env python

import os
import sys

gnuplot_file = '/home/abhigyan/gnrs/paxos_log_parse/plot_missing.gp'


def main():
    missing_request_file = sys.argv[1]

    paths = os.path.split(missing_request_file)
    analyze_missing_req_file(paths[0], missing_request_file)


def analyze_missing_req_file(stats_folder, missing_req_file):
    """
    Parse missing_req_file to compute distribution of missing requests and other stats in stats_folder
    """

    os.system('sort -nk6 ' + missing_req_file + ' > ' + os.path.join(stats_folder, 'file1'))
    os.system('sort -nk7 ' + missing_req_file + ' > ' + os.path.join(stats_folder, 'file2'))
    os.system('sort -nk8 ' + missing_req_file + ' > ' + os.path.join(stats_folder, 'file3'))
    os.system('sort -nk9 ' + missing_req_file + ' > ' + os.path.join(stats_folder, 'file4'))
    os.system('cp ' + gnuplot_file + ' ' + stats_folder)
    os.system('cd ' + stats_folder + '; gnuplot ' + gnuplot_file + '; rm file1 file2 file3 file4 ')

    # check if any missing requests are either due to a failed node or due to a stop request
    columns = ['PaxosID', 'NumReplica', 'DefaultCoordinator', 'SlotCount', 'Slot', 'MissingAccept', 'MissingCommit',
               'MAminusFail', 'MCminusFail']
    print '\t'.join(columns)
    f = open(missing_req_file)
    for line in f:
        tokens = line.split()
        slot = int(tokens[4])
        missing_accept_failed = int(tokens[7])
        missing_commit_failed = int(tokens[8])
        stop = int(tokens[9])
        if (missing_accept_failed > 0) and stop == 0 and slot > 0:
            print line,
    print

if __name__ == '__main__':
    main()

