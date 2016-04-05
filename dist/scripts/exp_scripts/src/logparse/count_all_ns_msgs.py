import os
import sys

__author__ = 'abhigyan'


def count_msgs(log_folder, stats_folder):
    """ Filters log entries related to message count at name servers and outputs total msg count to file """
    assert os.path.exists(log_folder)
    assert os.path.exists(stats_folder)
    os.system('gzip -d  ' + log_folder + '/*/log/gns_stat*gz 2>/dev/null')

    msg_count_file = stats_folder + '/MsgCountLogs'
    os.system('grep TotalMsgCount ' + log_folder + '/*/log/gns_stat* > ' + msg_count_file)
    node_msgs, interval_msgs = get_msg_count_from_file(msg_count_file)

    fw = open(os.path.join(stats_folder, 'msgs_total.txt'), 'w')
    fw.write('TotalMsgs\t' + str(sum(node_msgs.values())) + '\n')
    fw.close()

    fw = open(os.path.join(stats_folder, 'msgs_nodes.txt'), 'w')
    for k in sorted(node_msgs.keys()):
        fw.write(str(k) + '\t' + str(node_msgs[k]) + '\n')
    fw.close()

    fw = open(os.path.join(stats_folder, 'msgs_intervals.txt'), 'w')
    for k in sorted(interval_msgs.keys()):
        fw.write(str(k) + '\t' + str(interval_msgs[k]) + '\n')
    fw.close()


def get_msg_count_from_file(msg_count_file):
    """Returns total number of messages received by all name servers"""
    node_msgs = {}
    interval_msgs = {}
    for line in open(msg_count_file).readlines():
        tokens = line.split()
        # the first token will be the name of the file
        if len(tokens) >= 10 and tokens[2] == 'Interval' and tokens[4] == 'TotalMsgCount':
            interval = int(tokens[3])
            total_msg = int(tokens[5])
            interval_msg = int(tokens[7])
            node = int(tokens[9])
            if interval not in interval_msgs:
                interval_msgs[interval] = interval_msg
            else:
                interval_msgs[interval] += interval_msg
            if node not in node_msgs or node_msgs[node] < total_msg:
                node_msgs[node] = total_msg
    return node_msgs, interval_msgs


if __name__ == '__main__':
    # msg_count_file1 = sys.argv[1]
    # print get_msg_count_from_file(msg_count_file1)
    log_folder = sys.argv[1]
    stats_folder = sys.argv[2]
    count_msgs(log_folder, stats_folder)