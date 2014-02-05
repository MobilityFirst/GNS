#!/usr/bin/env python

import os
import sys
from write_array_to_file import write_array
import exp_config

def run_all_lns(gns_folder):
    from read_array_from_file import read_col_from_file
    ns_hostnames = read_col_from_file(exp_config.ns_file)
    num_ns = len(ns_hostnames)
    tmp_cmd_file = '/tmp/local-name-server.sh'
    from read_array_from_file import read_col_from_file
    cmds = []
    pl_lns = read_col_from_file(exp_config.lns_file)

    update_trace_param = ''#exp_config.update_trace_url
    lookup_trace_param = '' #exp_config.lookup_trace_url
    for i, lns in enumerate(pl_lns):
        node_id = str(i + num_ns)

        update_trace_param = 'update' # + node_id # may be nod id

        lookup_trace_param = 'lookup' # + node_id

        cmd = 'ssh -i ' + exp_config.ssh_key + '  -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ' + exp_config.user + ' ' + lns + ' "mkdir -p ' + \
            gns_folder + '; cd ' + gns_folder + '; python local-name-server.py  --lookupTrace ' \
            + lookup_trace_param + ' --updateTrace ' + update_trace_param + ' --id ' + node_id + '"'
        print cmd
        cmds.append(cmd)
    write_array(cmds, tmp_cmd_file, p=True)
    os.system('parallel -a ' + tmp_cmd_file)


def run_all_ns(gns_folder):
    tmp_cmd_file = '/tmp/name-server.sh'

    from read_array_from_file import read_col_from_file
    cmds = []
    pl_ns = read_col_from_file(exp_config.ns_file)
    for i, ns in enumerate(pl_ns):

        node_id = str(i)
        cmd = 'ssh -i ' + exp_config.ssh_key + ' -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ' + exp_config.user + ' ' + ns + ' "mkdir -p ' + \
              gns_folder + '; cd ' + gns_folder + '; python name-server.py --id ' + node_id + '"'
        print cmd
        cmds.append(cmd)

    write_array(cmds, tmp_cmd_file, p=True)

    os.system('parallel -a ' + tmp_cmd_file)


if __name__ == "__main__":
    gns_folder = sys.argv[1]
    num_ns = 3
    run_all_lns(gns_folder, num_ns)
    #run_all_ns(gns_folder)
