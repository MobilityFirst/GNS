__author__ = 'abhigyan'

import os
import sys


tcp_code_local_folder = '/Users/abhigyan/Documents/workspace/TCPConnectionTest/out/production/TCPConnectionTest/'

tcp_code_remote_folder = '/Users/abhigyan/Documents/tcpTest/'

tcp_client_class = 'TCPClient.class'

tcp_server_class = 'TCPServer.class'

remote_user = 'abhigyan'

test_port = 22111

tcp_output_file = 'connectResult'

tcp_result_success = 'Success'

tcp_result_failed = 'Failed'

local_host_name = 'abhiair.local'

def read_result_from_file():
    result_filename = os.path.join(tcp_code_local_folder, tcp_output_file)
    if not os.path.exists(result_filename):
        return False
    f = open(result_filename)
    line = f.readline()
    f.close()
    os.system('rm ' + result_filename) # delete so that same result is not read twice
    if line.startswith(tcp_result_success):
        return True
    return False



def test_host_tcp_connection(host_name,  port):
    print 'Host', host_name
    print 'Port', port

    # copy code to remote folder
    print 'Making remote dir ...'
    os.system('ssh ' + remote_user + '@' + host_name + ' "mkdir -p ' + tcp_code_remote_folder + '"')
    print 'Copying code to remote ...'
    os.system('scp ' + os.path.join(tcp_code_local_folder, tcp_client_class) + ' ' +
              os.path.join(tcp_code_local_folder, tcp_server_class) + ' ' +
              remote_user + '@' + host_name + ':' + tcp_code_remote_folder)
    # Test connection: server = remote, client = local
    print 'Running server remotely ...'
    os.system('ssh ' + remote_user + '@' + host_name + ' "killall -9 java; cd ' + tcp_code_remote_folder + ';' +
              ' nohup java ' + tcp_server_class.split('.')[0] + ' ' + str(port) + ' >foo.out 2> foo.err </dev/null &"')

    print 'Running TCP client locally ...'
    os.system('cd ' + tcp_code_local_folder + ';java ' + tcp_client_class.split('.')[0] + ' ' + host_name + ' ' +
              str(port) + '')

    print 'Kill remote java and local java ...'
    os.system('killall -9 java; ssh ' + remote_user + '@' + host_name + ' "killall -9 java"')

    print 'Read result from file ...'
    result = read_result_from_file()

    if result is False:
        print host_name + '\tFailed'
        return False

    # Test connection: client = remote, server = local
    print 'Running TCP server locally ...'
    os.system('cd ' + tcp_code_local_folder + ';nohup java ' + tcp_server_class.split('.')[0] + ' ' + str(port) + ' &')

    print 'Running TCP client remotely ...'
    os.system('ssh ' + remote_user + '@' + host_name + ' "killall -9 java; cd ' + tcp_code_remote_folder + ';' +
              'java ' + tcp_client_class.split('.')[0] + ' ' + local_host_name + ' ' + str(port) + '"')

    print 'Copy result file from remote ...'
    os.system('rm ' + os.path.join(tcp_code_local_folder, tcp_output_file))
    os.system('scp ' + remote_user + '@' + host_name + ':' + tcp_code_remote_folder + '/' + tcp_output_file + ' ' +
              tcp_code_local_folder)

    print 'Kill remote and local java ...'
    os.system('ssh ' + remote_user + '@' + host_name + ' "killall -9 java"')

    print 'Read result ...'
    result = read_result_from_file()

    if result is False:
        print host_name + '\tFailed'
        return False
    else:
        print host_name + '\tSuccess'
        return True


def test_tcp_connection_all_hosts(host_names_file):

    f = open(host_names_file)
    host_results = {}
    for line in f:
        host_name = line.strip()
        print '\nTesting connection to host ' + host_name + ' ...\n'
        result = test_host_tcp_connection(host_name, test_port)
        host_results[host_name] = result

    fw = open('pl_connected', 'w')
    for host_name, result in host_results.items():
        if result is True:
            fw.write(host_name + '\n')
    fw.close()

    fw = open('pl_not_connected', 'w')
    for host_name, result in host_results.items():
        if result is False:
            fw.write(host_name + '\n')
    fw.close()


if __name__ == "__main__":
    #test_host_tcp_connection(sys.argv[1], int(sys.argv[2]))
    test_tcp_connection_all_hosts(sys.argv[1])

