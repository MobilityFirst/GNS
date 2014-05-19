#!/usr/bin/env python

import os
import sys


def run_all_lns(user, ssh_key, lns_ids, remote_gns_folder, config_file, node_config_file, update_trace_param,
                workload_config_file):

    tmp_cmd_file = '/tmp/local-name-server.sh'

    cmds = []
    for node_id, lns in lns_ids.items():
        remote_node_folder = os.path.join(remote_gns_folder, str(node_id))
        cmd = 'ssh -i ' + ssh_key + '  -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ' + user \
              + ' ' + lns + ' "mkdir -p ' + remote_node_folder + '; cd ' + remote_node_folder + '; python local_name_server.py '
        if update_trace_param is not None and update_trace_param is not '':
            cmd += '  --updateTrace ' + update_trace_param
        if workload_config_file is not None and workload_config_file is not '':
            cmd += '  --wfile ' + workload_config_file
        if config_file is not None and config_file is not '':
            cmd += '  --configFile ' + config_file
        if node_config_file is not None and node_config_file is not '':
            cmd += '  --nsfile ' + node_config_file
        cmd += ' --id ' + str(node_id) + '"'
        print cmd
        cmds.append(cmd)
    fw = open(tmp_cmd_file, 'w')
    for cmd in cmds:
        fw.write(cmd + '\n')
    fw.close()
    os.system('parallel -j 50 -a ' + tmp_cmd_file)


def run_all_ns(user, ssh_key, ns_ids, remote_gns_folder, config_file, node_config_file):
    tmp_cmd_file = '/tmp/name-server.sh'
    cmds = []
    for node_id, ns in ns_ids.items():
        remote_node_folder = os.path.join(remote_gns_folder, node_id)
        cmd = 'ssh -i ' + ssh_key + ' -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ' + user \
              + ' ' + ns + ' "mkdir -p ' + remote_node_folder + '; cd ' + remote_node_folder + '; python name_server.py --id ' + str(node_id)
        if node_config_file is not None and node_config_file is not '':
            cmd += '  --nsfile ' + node_config_file
        cmd += '  --configFile ' + config_file + ' "'
        print cmd
        cmds.append(cmd)
    fw = open(tmp_cmd_file, 'w')
    for cmd in cmds:
        fw.write(cmd + '\n')
    fw.close()
    os.system('parallel -j 50 -a ' + tmp_cmd_file)


def run_name_server():
    pass


def kill_name_server():
    pass


def read_host_names(file_name):
    host_names = []
    f = open(file_name)
    for line in f:
        if line.startswith('#') is False and line.strip() != '':
            host_names.append(line.strip())
    return host_names

if __name__ == "__main__":
    gns_folder = sys.argv[1]
    num_ns = 3
    run_all_lns(gns_folder, num_ns)
