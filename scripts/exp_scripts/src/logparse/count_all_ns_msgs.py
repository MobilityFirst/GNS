import os
import sys

__author__ = 'abhigyan'


def count_msgs(log_folder, stats_folder):
    """ Filters log entries related to message count at name servers and outputs total msg count to file """
    assert os.path.exists(log_folder)
    assert os.path.exists(stats_folder)
    os.system('gzip -d  ' + log_folder + '/*/log/*gz')

    msg_count_file = stats_folder + '/MsgCountLogs'
    os.system('grep MsgCount ' + log_folder + '/*/log/* > ' + msg_count_file)
    fw = open(os.path.join(stats_folder, 'total_msgs.txt'), 'w')
    fw.write('TotalMsgs\t' + str(get_msg_count_from_file(msg_count_file)) + '\n')
    fw.close()


def get_msg_count_from_file(msg_count_file):
    """Returns total number of messages received by all name servers"""
    node_msgs = {}
    for line in open(msg_count_file).readlines():
        tokens = line.split()
        if len(tokens) >= 7 and tokens[2] == 'MsgCount' and tokens[4] == 'Node':
            node = tokens[5]
            msg = int(tokens[3])
            if node not in node_msgs or node_msgs[node] < msg:
                node_msgs[node] = msg
    return sum(node_msgs.values())

if __name__ == '__main__':
    msg_count_file1 = sys.argv[1]
    print get_msg_count_from_file(msg_count_file1)