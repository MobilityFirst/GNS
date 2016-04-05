#!/usr/bin/env python

import os
import sys
from write_array_to_file import write_array


def run_all_lns(gns_folder, num_ns):
    tmp_cmd_file = '/tmp/local-name-server.sh'
    from read_array_from_file import read_col_from_file
    cmds = []
    pl_lns = read_col_from_file('pl_lns')
    for i, lns in enumerate(pl_lns):
        node_id = str(i + num_ns)
        cmd = 'ssh -i auspice.pem -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user ' + lns + ' "mkdir -p ' + \
            gns_folder + '; cd ' + gns_folder + '; python /home/ec2-user/local-name-server.py  --lookupTrace ' \
            '/home/ec2-user/lookup_' + lns + ' --updateTrace /home/ec2-user/update_' + lns + ' --id ' + node_id + '"'
        print cmd
        cmds.append(cmd)
    write_array(cmds, tmp_cmd_file, p=True)
    os.system('parallel -a ' + tmp_cmd_file)


def run_all_ns(gns_folder):
    tmp_cmd_file = '/tmp/name-server.sh'

    from read_array_from_file import read_col_from_file
    cmds = []
    pl_ns = read_col_from_file('pl_ns')
    for i, ns in enumerate(pl_ns):
        node_id = str(i)
        cmd = 'ssh -i auspice.pem -oConnectTimeout=60 -oStrictHostKeyChecking=no -l ec2-user ' + ns + ' "mkdir -p ' + \
              gns_folder + '; cd ' + gns_folder + '; python /home/ec2-user/name-server.py --id ' + node_id + '"'
        print cmd
        cmds.append(cmd)

    write_array(cmds, tmp_cmd_file, p=True)

    os.system('parallel -a ' + tmp_cmd_file)


if __name__ == "__main__":
    gns_folder = sys.argv[1]
    num_ns = 1
    run_all_lns(gns_folder, num_ns)
    run_all_ns(gns_folder)